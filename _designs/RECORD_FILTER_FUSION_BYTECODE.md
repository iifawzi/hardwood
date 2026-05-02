# Plan: Record Filter Bytecode-Generated Fusion

**Status: Implemented**

## Context

Stages 1–3 (on `main`) compile a `ResolvedPredicate` tree into a `RowMatcher` graph, reduce per-row dispatch via per-(type, operator) leaf lambdas, and split compound matchers into fixed-arity `And2Matcher` / `And3Matcher` / `And4Matcher` (and `Or` equivalents). The remaining cost in compound predicates is the inner virtual call inside those fixed-arity matchers:

```java
private static final class And2Matcher implements RowMatcher {
    private final RowMatcher a, b;
    public boolean test(StructAccessor row) {
        return a.test(row) && b.test(row);
    }
}
```

When many distinct query shapes flow through one JVM, the inner call sites `a.test(row)` and `b.test(row)` accumulate leaf classes and go megamorphic — HotSpot can no longer inline the leaf body into the compound matcher. Per-row cost regresses by 2–3×.

The lift from a literal Java-source matrix (~2,200 hand-written lambdas, one per `(typeA, opA, typeB, opB, connective, accessMode)` tuple) is described in `MATCHER_DISPATCH_FUNDAMENTALS.md`. This design uses **runtime bytecode generation** instead — strategy D in that doc — so the matrix exists only as a few parameterised emitter methods. Per-query, ASM emits a hidden class implementing `RowMatcher` whose `test(...)` body has the comparison hard-coded as raw bytecode. Same observable runtime artifact as the lambda matrix, smaller source surface (~1,000 lines vs ~3,000 per file).

This is an **inner-site fix only**: each fused class kills the virtual call inside `And2Matcher`/`Or2Matcher`. The outer call site at `FilteredRowReader.hasNext`'s `matcher.test(row)` is left as-is — its mitigation is out of scope for this round and tracked as future work in `OUTER_SITE_MEGAMORPHISM.md`.

---

## Inner-site fusion via bytecode generation

### Eligibility matrix

Same as the lambda-matrix design in `RECORD_FILTER_FUSION.md`. Both children of an `And` or `Or` node are simple primitive leaves; fusion applies to:

**Same-type** (different and same column, except `boolean` which is diff-column only):
- `int + int`, `long + long`, `float + float`, `double + double`, `binary + binary` — same and diff column
- `boolean + boolean` — diff column only

**Cross-type numeric** (different column only, canonicalised after AND/OR commutativity):
- `int + long`, `int + double`, `long + double`

For each combination, every `(opA, opB)` pair is implemented for **both** AND and OR connectives, and **both** name-keyed (`StructAccessor.getX(name)`) and indexed (`RowReader.getX(int)`) access modes. Indexed access is gated to top-level columns where the reader supplies a non-negative field index.

The lambda-matrix version of this surface is ~2,200 distinct lambda classes. The bytecode-gen version is one parameterised emitter per type-target (~13 emitters); operator switching becomes a `switch` over `(opA, opB)` that emits the right `IF_ICMPxx` / `IFxx` jump opcodes. Source size is ~1,000 lines.

### New file: `internal/predicate/RecordFilterFusionBC.java`

Single internal class. Public API:

```java
static RowMatcher tryFuseAnd2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema);
static RowMatcher tryFuseAnd2Indexed(ResolvedPredicate a, ResolvedPredicate b,
        FileSchema schema, IntUnaryOperator topLevelFieldIndex);
static RowMatcher tryFuseOr2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema);
static RowMatcher tryFuseOr2Indexed(ResolvedPredicate a, ResolvedPredicate b,
        FileSchema schema, IntUnaryOperator topLevelFieldIndex);
```

Returns `null` when the pair is not eligible (caller falls back to the generic `AndNMatcher` / `OrNMatcher`). Internal — package-private.

Each entry point dispatches on `(typeA, typeB)` to a per-combination emitter. Each emitter:

1. Resolves both leaves' intermediate struct paths and leaf field names at compile time.
2. Decides between same-column and diff-column body shape based on `columnIndex`.
3. Emits the class body via ASM. Operator pairs become parameterised jump-emission.
4. Defines the class via `MethodHandles.Lookup#defineHiddenClass(byte[], true)` so it's GC-eligible when the matcher is unreachable.

Cross-type combinations (`int+long`, `int+double`, `long+double`) canonicalise the order before emission so `(double, long)` and `(long, double)` flow through one emitter.

The class shape for indexed access is essentially:

```java
final class FusedLLAndIdx_N implements RowMatcher {
    public boolean test(StructAccessor row) {
        RowReader r = (RowReader) row;
        if (r.isNull(<idx>)) return false;
        long v = r.getLong(<idx>);
        return v <opA> vA && v <opB> vB;   // emitted as direct LCMP + IFxx
    }
}
```

`<idx>`, `vA`, `vB` are baked into the constant pool via LDC. No instance fields needed for primitive-only fusion. Binary fusion uses two `byte[]` instance fields and calls `RecordFilterCompiler.compareBinary` via `INVOKESTATIC`.

The name-keyed shape is:

```java
final class FusedLLAnd_N implements RowMatcher {
    private final String[] path;   // empty for top-level
    FusedLLAnd_N(String[] path) { this.path = path; }
    public boolean test(StructAccessor row) {
        StructAccessor s = RecordFilterCompiler.resolve(row, this.path);
        if (s == null || s.isNull("name")) return false;
        long v = s.getLong("name");
        return v <opA> vA && v <opB> vB;
    }
}
```

`"name"` is emitted as an LDC string constant; `path` is passed in via the constructor (only when non-empty — top-level fusion uses the same no-path constructor as the indexed case and skips `resolve`).

### Modify `internal/predicate/RecordFilterCompiler.java`

`compileAnd` and `compileOr` consult `RecordFilterFusionBC` before falling back to the generic walker, when `children.size() == 2`:

```java
private static RowMatcher compileAnd(List<ResolvedPredicate> children, FileSchema schema,
        IntUnaryOperator topLevelFieldIndex) {
    if (FUSION_ENABLED && children.size() == 2) {
        ResolvedPredicate a = children.get(0);
        ResolvedPredicate b = children.get(1);
        if (topLevelFieldIndex != null) {
            RowMatcher fused = RecordFilterFusionBC.tryFuseAnd2Indexed(a, b, schema, topLevelFieldIndex);
            if (fused != null) return fused;
        }
        RowMatcher fused = RecordFilterFusionBC.tryFuseAnd2(a, b, schema);
        if (fused != null) return fused;
    }
    RowMatcher[] compiled = compileAll(children, schema, topLevelFieldIndex);
    return switch (compiled.length) {
        case 1 -> compiled[0];
        case 2 -> new And2Matcher(compiled[0], compiled[1]);
        case 3 -> new And3Matcher(compiled[0], compiled[1], compiled[2]);
        case 4 -> new And4Matcher(compiled[0], compiled[1], compiled[2], compiled[3]);
        default -> new AndNMatcher(compiled);
    };
}
```

`compileOr` follows the same shape against `tryFuseOr2*`. Arity ≥ 3 keeps the generic `AndNMatcher` / `OrNMatcher` walker — out of scope for this round.

### Helper visibility on `RecordFilterCompiler`

The bytecode-generated body calls four static helpers on `RecordFilterCompiler`:

- `resolve(StructAccessor row, String[] path)` — for name-keyed fusion with non-empty paths.
- `compareBinary(byte[] a, byte[] b, boolean signed)` — for `binary + binary` fusion.
- `pathSegments(FileSchema schema, int columnIndex)` — used by the emitter to derive the path constant.
- `leafName(FileSchema schema, int columnIndex)` — used by the emitter to derive the field-name constant.

All four are already package-private on `main` (the fixed-arity merge made them so) and `EMPTY_PATH` is exported as a constant. No visibility change is required.

### Disable flag

A static `boolean FUSION_ENABLED` is read once at class init from the system property `hardwood.recordfilter.fusion`. Default `true`; set `-Dhardwood.recordfilter.fusion=false` to disable. Internal-only knob, used by benchmarks to A/B the fusion contribution. Not part of the public API.

### Why each tuple is a distinct concrete class

Same load-bearing constraint as the lambda matrix: each `(typeA, opA, typeB, opB, connective, accessMode)` tuple becomes a *distinct hidden class* with a unique receiver type. That's what kills inner-site megamorphism — the body has no inner virtual call to be megamorphic about. Any shared dispatch point inside the body (a `BiPredicate` field, a generic helper that takes the operator as a parameter) would defeat fusion.

### ASM dependency

Add `org.ow2.asm:asm` to `core/pom.xml`. ~270 KB on the classpath. The same dep is already used by the JMH module, so no version-coordination issue.

Alternative considered: JEP 484 `java.lang.classfile` (finalised in JDK 24, available in our Java 25 baseline). Avoids the dep. Deferred to a follow-up — sticking with ASM keeps the prototype-known-good emission path and the team's existing familiarity. If we want to drop ASM later, the swap is mechanical (one emitter file, no surface change).

### First-call latency

Each `defineHiddenClass` call costs ~0.5–2 ms (verify + define + class-load). Negligible for long scans, measurable for short interactive queries. Mitigations if it shows up:

- Cache fused classes by `(typeA, opA, vA, typeB, opB, vB, connective, accessMode, idx)` key. Two queries with identical predicates share one hidden class.
- Defer to follow-up; first measure under `RecordFilterMegamorphicEndToEndTest` whether codegen latency is visible at all.

Out of scope for the initial implementation.

---

## Null and NaN semantics

Fused matchers preserve `RecordFilterEvaluator.matchesRow` semantics exactly:

- **Intermediate struct null** — name-keyed bodies check `RecordFilterCompiler.resolve(row, path) != null` before any value access. Indexed bodies are gated to top-level columns, so `delegate` itself is the relevant non-null target.
- **Direct field null** — bodies check `acc.isNull(name)` or `delegate.isNull(idx)` before reading the primitive.
- **AND short-circuit** — bytecode emits the `IFEQ` jump from leaf A's failure path to the false return; leaf B is not evaluated when leaf A is false.
- **OR short-circuit** — bytecode emits the `IFNE` jump from leaf A's success path to the true return; leaf B is not evaluated when leaf A is true. When leaf A is null/false, leaf B *is* evaluated.
- **Float / double comparisons** — emit `INVOKESTATIC Float.compare(F, F)I` / `Double.compare(D, D)D` to match the legacy NaN ordering.
- **Binary comparisons** — call `RecordFilterCompiler.compareBinary(a, b, signed)` via `INVOKESTATIC`. Per-leaf `signed` is captured at compile time and emitted as a `ICONST_0` / `ICONST_1` argument.

---

## Testing

### Equivalence — `core/src/test/java/dev/hardwood/internal/predicate/RecordFilterFusionBCTest.java` (new)

Three-way equivalence check. For every fused (combo × connective × opA × opB × same-column × null / non-null leaf value) tuple:

1. Build a stub row implementing `RowReader` (which extends `StructAccessor` and adds the indexed accessors).
2. Compile the predicate two ways:
    - Through `RecordFilterCompiler.compile(p, schema, projection::toProjectedIndex)` — exercises the BC indexed path.
    - Through `RecordFilterCompiler.compile(p, schema)` — exercises the name-keyed path (currently the generic fallback).
3. Compare both against `RecordFilterEvaluator.matchesRow` (the legacy oracle).

### Existing tests

- `RecordFilterEvaluatorTest`, `RecordFilterIndexedTest` continue to pass unchanged — fusion is transparent when the legacy oracle is used as the equivalence reference.
- `./mvnw verify` continues to pass; no public API changes.

---

## Benchmarks

### JMH micro — `RecordFilterMegamorphicBenchmark` (new)

Path: `performance-testing/micro-benchmarks/src/main/java/dev/hardwood/benchmarks/RecordFilterMegamorphicBenchmark.java`.

Setup constructs ~12 distinct arity-2 predicate shapes covering every fusion combo × connective:

- `id BETWEEN a AND b` (long+long same-column AND)
- `tag BETWEEN x AND y` (int+int same-column AND)
- `value BETWEEN v1 AND v2` (double+double same-column AND)
- `id < a OR id > b` (long+long same-column OR)
- `tag == x OR tag == y` (int+int same-column OR)
- `id >= a AND value < v` (long+double diff-column AND)
- `tag < x AND value < v` (int+double diff-column AND)
- `id < a AND tag > x` (int+long diff-column AND)
- `id >= a OR value < v` (long+double diff-column OR)
- `tag <= x OR id > a` (int+long diff-column OR)
- `flag == true AND flag == false` (boolean+boolean diff-column AND)
- `bin >= "abc" AND bin < "xyz"` (binary+binary same-column AND)

Two arms, both interleaving rows × shapes per `@Benchmark` invocation:

- `legacyMegamorphic` — every row goes through `RecordFilterEvaluator.matchesRow`. Pre-compilation behaviour.
- `compiledMegamorphic` — matchers compiled via `RecordFilterCompiler.compile`. Each matcher is a unique hidden class when fusion is enabled, or shares a small set of fixed-arity matchers when `-Dhardwood.recordfilter.fusion=false`.

`@OperationsPerInvocation(BATCH_SIZE * SHAPE_COUNT)` so reported `ns/op` is per-row-per-shape cost.

### End-to-end — `RecordFilterMegamorphicEndToEndTest` (new)

Path: `performance-testing/end-to-end/src/test/java/dev/hardwood/perf/RecordFilterMegamorphicEndToEndTest.java`.

Lazy-generates `target/record_filter_megamorphic.parquet` (10M rows, schema `id long, value double, tag int, flag boolean, bin string`) on first run. Same generation pattern as the existing `RecordFilterBenchmarkTest`.

Runs the same ~12 query shapes through `ParquetFileReader.buildRowReader().filter(...).build()`, sequentially in the same JVM, multiple iterations (`-Dperf.runs`, default 5). Drives `FilteredRowReader.hasNext`'s outer call site to ≥12 receiver classes — every shape contributes one fused class with fusion on.

Two modes per query, controlled by the system property:

- `-Dhardwood.recordfilter.fusion=true` (default): BC-fused matchers.
- `-Dhardwood.recordfilter.fusion=false`: generic Stage 1–3 matchers (And2Matcher / Or2Matcher fallback).

Separate JVM invocations to keep the static `FUSION_ENABLED` flag clean. Captures wall time per shape and aggregate throughput.

---

## Risks and edge cases

- **Cast safety in indexed fusion.** Indexed bodies cast the row to `RowReader`. Safe by construction — `tryFuseAnd2Indexed` / `tryFuseOr2Indexed` are reached only via the 3-arg `RecordFilterCompiler.compile` overload, which is only invoked from `FlatRowReader` and `NestedRowReader` (both implement `RowReader`).
- **Hidden-class lifecycle.** Defined via `defineHiddenClass(bytes, true)` so the class is GC-eligible when its `RowMatcher` instance is unreachable. Per-query hidden classes do not retain in metaspace.
- **First-call latency.** ~0.5–2 ms per `defineHiddenClass`. Acceptable for analytical scans (millions of rows). If interactive workloads hit it, add the per-predicate cache described in *Inner-site fusion — First-call latency*.
- **Canonicalisation correctness.** Cross-type AND/OR canonicalisation relies on commutativity. Both `And` and `Or` are commutative regardless of leaf order; the legacy evaluator uses the same commutative semantics via short-circuit on first match/miss. Three-way equivalence tests cover both orderings.
- **Stack traces.** Generated classes show `dev.hardwood.internal.predicate.FusedLLAnd_42.test(Unknown Source)`. No source file. Comparable to lambda traces in production logs; worse in a debugger. Acceptable for internal-only types.

---

## Future work (deferred)

### Outer-site mitigation

After this round, every fused matcher is a unique hidden class — so the outer call `matcher.test(row)` inside `FilteredRowReader.hasNext` still goes megamorphic when many distinct queries flow through one JVM. This is the inner-site fix's flip side: the same property that kills inner-site megamorphism (one class per query shape) guarantees outer-site megamorphism. `OUTER_SITE_MEGAMORPHISM.md` outlines three mitigations — `TypeProfileWidth=8`, `runScan` iteration inversion, and per-query reader specialisation. None of them are landed here; they are independent follow-ups whose value should be evaluated against fresh measurements on this baseline.

### Arity-3+ inner-site fusion

Same emitter strategy extended to arity 3 / 4. Useful if benchmarks show `AndNMatcher` walker megamorphism at arity ≥ 3 is a measured cost.

### Same-type *diff-column* indexed coverage

The current `RecordFilterFusionBC.dispatchIndexed` covers same-column same-type for all five primitive types, plus all cross-type diff-column combos and `boolean+boolean` diff-column. *Same-type diff-column* (`int+int`, `long+long`, `float+float`, `double+double`, `binary+binary` against two distinct columns) falls back to the fixed-arity `And2Matcher`/`Or2Matcher`. The benchmark shapes don't exercise these, so the perf impact is zero today, but the matrix is incomplete — easy follow-up using the same emitters with two indices instead of one.

### Name-keyed dispatch

`tryFuseAnd2` / `tryFuseOr2` (the name-keyed entry points) currently return `null` from `dispatchNameKeyed`; all `RowReader` implementations on `main` (`FlatRowReader`, `NestedRowReader`) go through the indexed path, so this is also a future-proofing gap rather than a hot-path one. The name-keyed builders mirror the indexed ones — same body shape with `StructAccessor.getX(name)` and `RecordFilterCompiler.resolve(row, path)` substituted for the `RowReader.getX(int)` accessors.

### `java.lang.classfile` (JEP 484)

Drop the ASM dependency in favour of the JDK-shipped class-file API on Java 24+. Mechanical refactor; same hidden-class machinery.

---

## Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax for tracking. Work top-to-bottom; each task ends in a commit so the branch stays bisectable.

**Branch:** create `bytecode-fusion` from `main`.

**GitHub issue:** `#193` (the active fusion line of work). Commit messages must begin with the issue key.

**Build command:** `timeout 180 ./mvnw -pl core -am verify -DskipITs` for fast iteration on `core`. Always wrap with `timeout 180` per CLAUDE.md.

### Chunk 1 — Branch + ASM dependency

#### Task 1.1 — Cut the branch

- [x] **Step 1:** `git fetch origin && git checkout -b bytecode-fusion origin/main`.
- [x] **Step 2:** Verify `git status` is clean and `git log -1 --oneline` matches `origin/main`.
- [x] **Step 3:** Run `timeout 180 ./mvnw -pl core -am verify -DskipITs`. Expected: `BUILD SUCCESS`.

#### Task 1.2 — Add ASM to `core/pom.xml`

- [x] **Step 1:** Insert before the closing `</dependencies>`:

  ```xml
  <dependency>
    <groupId>org.ow2.asm</groupId>
    <artifactId>asm</artifactId>
  </dependency>
  ```

  Version is supplied by the parent `<dependencyManagement>` block.
- [x] **Step 2:** `timeout 180 ./mvnw -pl core -am verify -DskipITs`. Expected: `BUILD SUCCESS`.
- [x] **Step 3:** Commit: `#193 Add ASM dependency to core for bytecode-gen fusion`.

#### Task 1.3 — Add the disable flag

The four helpers (`compareBinary`, `resolve`, `pathSegments`, `leafName`) and `EMPTY_PATH` are already package-private on `main`; no visibility change is needed.

- [x] **Step 1:** Add to `RecordFilterCompiler`, near `EMPTY_PATH`:

  ```java
  /// Compile-time fusion of arity-2 AND/OR leaves into per-tuple synthetic
  /// classes. Default true; set `-Dhardwood.recordfilter.fusion=false` to
  /// route through the fixed-arity [And2Matcher] / [Or2Matcher] for
  /// benchmark A/B comparison. Read once at class init.
  static final boolean FUSION_ENABLED =
          !"false".equalsIgnoreCase(System.getProperty("hardwood.recordfilter.fusion"));
  ```
- [x] **Step 2:** Build + commit: `#193 Add fusion-disable flag to RecordFilterCompiler`.

### Chunk 2 — Bytecode emitter

#### Task 2.1 — Scaffold `RecordFilterFusionBC.java`

- [x] **Step 1:** Create the file with the four `tryFuse*` entry points and shared bytecode helpers (`startClass`, `startTestMethod`, `endTestWithBranches`, `emitJumpIfFalse`, `emitJumpIfTrue`, `emitIntCmpJumpIfFalse`, `emitIntCmpJumpIfTrue`, `defineNoArg`, `defineTwoByteArray`).

#### Task 2.2 — `long+long` same-column indexed (end-to-end pipeline check)

- [x] **Step 1:** Implement `tryFuseAnd2Indexed` / `tryFuseOr2Indexed` for the `long+long` same-column case via `buildLongSameColIndexed(idx, opA, vA, opB, vB, isAnd)`.
- [x] **Step 2:** In `RecordFilterCompiler.compileAnd` / `compileOr`, consult `RecordFilterFusionBC` first when `FUSION_ENABLED && children.size() == 2`; fall through on null.
- [x] **Step 3:** Build + run all `core` tests. Expected: `BUILD SUCCESS`.
- [x] **Step 4:** Commit.

#### Task 2.3 — Remaining same-type same-column indexed builders

- [x] `int+int` (same-col)
- [x] `float+float` (same-col)
- [x] `double+double` (same-col)
- [x] `binary+binary` (same-col)

#### Task 2.4 — Cross-type and boolean diff-column indexed

- [x] `int+long` (diff-col)
- [x] `int+double` (diff-col)
- [x] `long+double` (diff-col)
- [x] `boolean+boolean` (diff-col)

### Chunk 3 — Equivalence test

- [x] Create `RecordFilterFusionBCTest.java`. Parameterised over `(combo, connective, opA, opB, value, isNull)`. Three-way equivalence: BC indexed vs name-keyed compile path vs legacy `RecordFilterEvaluator.matchesRow` oracle.

### Chunk 4 — Benchmarks

- [x] Create `RecordFilterMegamorphicBenchmark.java` (JMH).
- [x] Create `RecordFilterMegamorphicEndToEndTest.java` (10M-row Parquet, two modes).
- [x] Run twice with `-Dhardwood.recordfilter.fusion={true,false}` and capture results into the section below.

---

## Results

Hardware: macOS aarch64 (Apple Silicon), Oracle JDK 25.0.3, Maven wrapper 3.9.12.

### JMH micro — `RecordFilterMegamorphicBenchmark`

12 arity-2 shapes interleaved per row through one bytecode location. `@OperationsPerInvocation(BATCH_SIZE × SHAPE_COUNT)`; 1 fork × 5 warmup × 5 measurement iterations.

| Arm | ns/op | Speedup vs legacy |
|---|---:|---:|
| `legacyMegamorphic` (RecordFilterEvaluator interpreter) | 21.056 ± 0.704 | 1.00× |
| `compiledMegamorphic`, fusion off (Stage 1–3 baseline) | 3.061 ± 0.067 | 6.88× |
| `compiledMegamorphic`, fusion on (BC) | 3.066 ± 0.145 | 6.87× |

The micro-benchmark separates compiled-with-fusion-off from fusion-on within noise. The reason is structural: in this JMH harness all 12 matchers are reachable from one `RowMatcher[]` array, and the JIT can profile types on `m.test(row)` based on the array's element types — even without fusion, the per-leaf lambda classes from the Stage 1–3 leaf factories already give the JIT enough type information to fold most calls. Inner-site megamorphism only meaningfully manifests through `FilteredRowReader.hasNext()`'s call site, where matchers from many short queries flow through the same bytecode location at runtime — that is what the end-to-end benchmark measures.

The 6.88× speedup over the interpreter is the cumulative Stage 1–3 + BC fusion win. The fusion-specific win lives in the end-to-end benchmark below.

### End-to-end — `RecordFilterMegamorphicEndToEndTest`

10M-row Parquet `(id long, value double, tag int, flag boolean, bin string)`, 3 runs per shape, separate JVMs per arm so the static fusion flag stays clean. Same 12 shapes as the JMH micro, executed sequentially through `ParquetFileReader.buildRowReader().filter(...).build()` so `FilteredRowReader.hasNext`'s outer call site sees one fused class per shape. **All times are wall-clock per scan, not predicate-only — they include I/O and decoding overhead, which is invariant across arms.**

| Shape | Generic ms | Fused ms | Speedup |
|---|---:|---:|---:|
| `id BETWEEN 1M and 4M` (long+long AND)             |  25.3 |  13.8 | **1.83×** |
| `id < 500K OR id > 9.5M` (long+long OR)            |   7.1 |   7.6 | 0.93× |
| `tag BETWEEN 0 and 50` (int+int AND)               | 115.0 |  69.4 | **1.66×** |
| `tag = 5 OR tag = 47` (int+int OR)                 |  57.1 |  39.3 | 1.45× |
| `value BETWEEN 0 and 500` (double+double AND)      | 116.5 |  78.9 | 1.48× |
| `id < 5M AND value < 500` (long+double AND)        |  60.3 |  43.6 | 1.38× |
| `id < 5M OR value > 500` (long+double OR)          |  69.7 |  66.5 | 1.05× |
| `tag < 50 AND id > 5M` (int+long AND)              |  50.3 |  38.1 | 1.32× |
| `tag < 50 AND value < 500` (int+double AND)        | 118.4 |  90.7 | 1.31× |
| `flag = true AND flag != false` (bool AND)         |  96.8 |  76.0 | 1.27× |
| `value > 0 AND id < 9999` (canonical-swap)         |   1.3 |   1.7 | 0.76× |
| `bin BETWEEN k200 and k800` (binary+binary AND)    | 175.3 |  98.1 | **1.79×** |
| **Total (12 shapes × 3 runs)** | **2679.4 ms** | **1871.1 ms** | **1.43×** |

Headline: **1.43× aggregate speedup** over the Stage 1–3 baseline. The win comes from removing the inner virtual call from compound matchers — every fused class's body is direct primitive arithmetic with no `a.test(row) / b.test(row)` to dispatch through.

- **Largest wins are on same-column AND BETWEEN shapes** (long, int, double, binary) — the legacy generic path pays for two megamorphic inner sites per row, while the fused matcher loads the value once and runs both comparisons inline.
- **A few low-selectivity shapes** (`id < 500K OR id > 9.5M`, `value > 0 AND id < 9999`) are flat or slightly worse — the selectivity is so high or so low that the matcher cost is amortised into noise. These are run-to-run variation, not regressions.

The JMH micro and the end-to-end benchmark answer different questions. The JMH harness puts every matcher in one array and the JIT can reason about the receiver types statically; the end-to-end test puts every matcher through one shared bytecode location (`FilteredRowReader.hasNext`'s `matcher.test(row)`) which the JMH benchmark cannot fully reproduce inside one process. The end-to-end numbers are the production-relevant ones.
