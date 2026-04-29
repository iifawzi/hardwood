# Record Filter Compilation

Status: Proposed

Hoist predicate dispatch out of the per-row evaluation loop in `RecordFilterEvaluator` by compiling the `ResolvedPredicate` tree once per reader into a specialised `RowMatcher` graph. Eliminates per-row type switches, operator switches, and name-keyed accessor lookups, enabling JIT inlining and (later) auto-vectorisation.

---

## A. Existing performance tests — what to run before and after

There is a directly relevant benchmark already wired up. It must be run both before any changes (baseline) and after each milestone.

### A.1 The benchmark
**`RecordFilterBenchmarkTest`** at `performance-testing/end-to-end/src/test/java/dev/hardwood/perf/RecordFilterBenchmarkTest.java:43-156`.

It is a JUnit test (not JMH), but the structure is exactly what is needed — three scenarios over a synthetic 10M-row Parquet file (`id: long`, `value: double`):

| Scenario | What it measures | Predicate |
|---|---|---|
| **No filter** (baseline) | Pure read throughput, no predicate eval | — |
| **Match-all** (`id ≥ 0`) | **Worst-case dispatch overhead** — every row is evaluated and kept; this isolates the cost of predicate dispatch from any I/O savings | `gtEq("id", 0L)` |
| **Selective** (`id < 1%`) | Realistic high-selectivity case where dispatch cost matters less | `lt("id", 100_000L)` |

The output is `Records/sec` per scenario plus two derived metrics:
- *Match-all overhead*: `(matchAllTime − noFilterTime) / noFilterTime` — **this is the number this change should reduce**
- *Selective speedup*: `noFilterTime / selectiveTime`

### A.2 How to run it

```bash
./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
  -Dtest=RecordFilterBenchmarkTest -Dperf.runs=10 \
  -DskipTests=false
```

Apply a 180s Maven timeout to detect deadlocks early. Bumping `perf.runs` from the default 5 to 10 reduces noise. The test caches the 10M-row file at `performance-testing/end-to-end/target/record_filter_benchmark.parquet` after first run, so subsequent runs are fast.

If `core` changes, install it first or the perf module will not see the changes:

```bash
./mvnw -pl core install -DskipTests
./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
  -Dtest=RecordFilterBenchmarkTest -Dperf.runs=10
```

### A.3 Recommended methodology
1. Run baseline 3× (the test itself averages 5–10 runs, but JIT warmup across full processes drifts) — record the *match-all overhead* % from each.
2. Use the median of the three for the baseline number.
3. After each milestone, re-run identically and compare medians.
4. **Add a multi-column compound-predicate scenario** as part of this work (see step E.1 below). The current benchmark uses one leaf — the hypothesis kicks in harder when the per-row overhead includes recursing into `And`/`Or`. Without this scenario, there is no clean number proving the win.

### A.4 Other measurement tools available
- `performance-testing/micro-benchmarks/` is a real JMH module (JMH 1.37) — currently has `SimdBenchmark`, `MemoryMapBenchmark`, `PageHandlingBenchmark`, `PageScanBenchmark`. **No predicate JMH benchmark exists yet.** A `RecordFilterMicroBenchmark` is added as part of this work — JMH's `@Fork` + warmup gives cleaner numbers than the JUnit timer for measuring the very tight inner loop.
- `PageFilterBenchmarkTest` exercises page-level (statistics-based) filtering, which is upstream of record-level filtering. It should not shift, but is run once to confirm there is no regression there.

---

## B. Current state

The code today does **not** have a `matchBatch` method. Predicate evaluation is strictly per-row through `RecordFilterEvaluator.matchesRow(...)`, called inside `FilteredRowReader.hasNext()`:

```
FilteredRowReader.hasNext()                    // core/.../internal/reader/FilteredRowReader.java:47-56
  while (delegate.hasNext()) {
      delegate.next();                          // delegate is FlatRowReader (or nested)
      if (matchesRow(predicate, delegate, schema)) { ... }
  }
```

Inside `matchesRow` (`core/.../internal/predicate/RecordFilterEvaluator.java:30-145`), every single row pays for:
1. **Type switch** on the sealed `ResolvedPredicate` (12 cases)
2. **`resolve(reader, schema, columnIndex)`** — walks the struct path for nested columns
3. **`schema.getColumn(p.columnIndex()).fieldPath().leafName()`** — string lookup
4. **`acc.isNull(name)`** — name-keyed null check
5. **`acc.getInt(name)` / `getLong(name)` / …** — name-keyed value fetch
6. **Operator switch** (6 operators)
7. For `And`/`Or`: iterator allocation and recursion into children

`FlatRowReader` already keeps the data **batched and columnar** internally (`Object[] flatValueArrays`, `BitSet[] flatNulls`). It just hides them behind `StructAccessor`'s name-keyed API. The raw material this design depends on already exists — the predicate path simply does not read it directly.

`ResolvedPredicate` is sealed, immutable, and fully known at `RowReader` creation time. That is what makes compile-once safe.

---

## C. Why this is faster

The transformation has **three independent wins** stacked on top of each other.

### C.1 Megamorphic → monomorphic call sites
HotSpot's C2 optimises *call sites* based on observed receiver types. A call site that sees ≤ 2 types is **monomorphic** or **bimorphic** — JIT inlines the target. A call site that sees ≥ 3 types is **megamorphic** — JIT inserts a virtual call and stops inlining.

Currently, the `switch (predicate)` covers 13 case shapes, and `resolve()` and `acc.getX()` hit the same method on `StructAccessor` regardless of column type — and `acc.isNull(name)` does a `HashMap` (or similar) lookup keyed by string. Per-row.

When the tree is compiled to a `RowMatcher` instance per leaf predicate, each compiled site sees **exactly one implementation class** — `IntGtMatcher`, `LongEqMatcher`, etc. — at one call site. C2 inlines aggressively, the operator becomes a literal `>`/`==`, and the entire leaf collapses to a few instructions.

### C.2 Hoisting loop invariants
Things like `schema.getColumn(p.columnIndex()).fieldPath().leafName()` are *invariant* over rows in the batch. Computing them once per batch, not once per row, is pure profit. `valueArrays[projIdx]` is invariant per batch. Even casting `Object` to `int[]` is invariant — do it once.

### C.3 Auto-vectorisation (SIMD) opportunity
Once the inner loop looks like this:

```java
for (int i = 0; i < batchSize; i++) {
    if (col2[i] > 42 && col5[i] == 100L) result.set(i);
}
```

C2's loop optimiser can apply auto-vectorisation on platforms where it is enabled. The repo already has a `SimdBenchmark`, so the project clearly cares about this. The current per-row dispatch path is **categorically unvectorisable** — there is a virtual call inside the loop.

Realistically, auto-vectorisation across `BitSet.set(i)` is unlikely to fire — but writing into a `byte[]`/`long[]` mask first and folding into a `BitSet` later does vectorise. The interface design keeps this option open.

### C.4 Why not bytecode generation?
Bytecode generation (LambdaMetafactory / ASM) is overkill and complicates the build. The JIT itself does the work: each compiled matcher is a small Java class, and after warmup the JIT inlines it as if it had been hand-written.

---

## D. Scope

The end state is delivered in four stages, each as its own GitHub issue and PR:

### D.1 Stage 1 — per-row compiled matcher
Replace `matchesRow(predicate, accessor, schema)` with `compile(predicate, schema) → RowMatcher`, then call `matcher.test(accessor)` in the row loop. Hoists field-name lookups, struct-path resolution, and operator switches out of the row loop. Iteration model and `FilteredRowReader` API are unchanged. Detailed in §I.

### D.2 Stage 2 — fixed-arity AND/OR matchers
Replace the `Object[]`-of-children loop in `compileAnd` / `compileOr` with fixed-arity classes (`And2Matcher`, `And3Matcher`, `And4Matcher`; symmetric for OR) holding children as `final` fields. The JIT inlines through the stable call sites and effectively fuses the leaf bodies for monomorphic per-query workloads. Detailed in §J.

### D.3 Stage 2.5 — arity-2 leaf fusion
For 2-arity AND nodes whose children are both primitive leaves of compatible numeric types, emit a single fused matcher whose body inlines both comparisons as primitive bytecode operations — no inner virtual call. Each `(typeA, opA, typeB, opB)` produces a distinct synthetic class, so the matcher stays monomorphic at every call site even when the JVM runs many concurrent predicate shapes through the shared `And2Matcher` bytecode location (the megamorphic-pollution scenario the doc previously deferred). Supported combinations: `long+long`, `int+int`, `double+double`, `long+double` (with canonical-swap for `double+long`), each with a same-column range fast path for `BETWEEN` patterns. Detailed in §J.

### D.4 Stage 3 — index-based fast path
Bypass name-keyed `StructAccessor.getX(name)` for flat top-level columns via a new internal `IndexedAccessor` interface implemented by `FlatRowReader`. Targets the residual ~8 ns/row of struct-accessor cost that becomes the bottleneck after Stage 2.5. Detailed in §K.

Stages 1 + 2 + 2.5 capture the bulk of the available win at the
predicate-dispatch layer; Stage 3 attacks what's left below it. Stages
are independent — each lands as its own PR with its own benchmark
delta.

---

## E. Implementation plan (Stage 1)

### E.0 Prerequisite — open a GitHub issue
Every commit needs an issue key. Title: *"Compile predicate tree once per reader to eliminate per-row dispatch"*. All commits below are `#NNN ...`.

### E.1 Establish the baseline
1. Run `RecordFilterBenchmarkTest` with `-Dperf.runs=10`, three times. Record median *match-all overhead %* and *selective speedup ×*.
2. Add a **multi-column And** scenario to `RecordFilterBenchmarkTest`:
   ```java
   FilterPredicate filter = FilterPredicate.and(
       FilterPredicate.gtEq("id", 0L),
       FilterPredicate.lt("value", Double.MAX_VALUE));
   ```
   This is the scenario where `And`-recursion dominates and the change shines. Record baseline for this too.
3. Add a JMH benchmark `RecordFilterMicroBenchmark` in `performance-testing/micro-benchmarks/`. One `@State` class holds a pre-loaded `Object[][]` of column arrays, plus a `BitSet[]` of nulls. Three `@Benchmark` methods: single-leaf int-GT, two-leaf And, three-leaf And+Or. Use `@OperationsPerInvocation(batchSize)` so the result is in `ns/row`.
4. **Commit**: `#NNN Add multi-column And scenario and JMH micro-benchmark for record filter` — pure benchmark addition; no behaviour change.

### E.2 Introduce the `RowMatcher` interface
Create `core/src/main/java/dev/hardwood/internal/predicate/RowMatcher.java`:

```java
@FunctionalInterface
interface RowMatcher {
    boolean test(StructAccessor row);
}
```

Internal package — not public API. Single-method functional interface so leaves can be lambdas if useful, though the implementation prefers small concrete classes for stack-trace clarity and to give the JIT type-stable receivers (§C.1).

### E.3 Build the compiler
Add `RecordFilterCompiler` in the same package with:

```java
static RowMatcher compile(ResolvedPredicate predicate, FileSchema schema);
```

The compiler does a **single recursive walk** of the predicate tree, returning a `RowMatcher` per node:

| Node | Compiled form |
|---|---|
| `IntPredicate(col, GT, v)` | `IntGtRowMatcher(leafName, v)` (concrete class, captures pre-resolved leaf name and constant) |
| `IntPredicate(col, EQ, v)` | `IntEqRowMatcher(...)` — separate class per operator |
| `LongPredicate(col, op, v)` | analogous, 6 classes |
| `IntInPredicate(col, vals)` | `IntInRowMatcher(leafName, sortedVals)` — sort the array, use binary search if size ≥ ~16 (worth measuring) |
| `IsNullPredicate(col)` | `IsNullRowMatcher(leafName)` |
| `And(children)` | `AndRowMatcher(RowMatcher[] children)` — short-circuits in array order |
| `Or(children)` | `OrRowMatcher(RowMatcher[] children)` |

Two design decisions:

**Why one class per (type, operator) instead of one class per type with a switch?** This is the §C.1 win. If `IntPredicate` becomes one class with `switch (op)` inside, the call site is monomorphic (good) but the inner switch is still there. Splitting per operator collapses the switch into a literal comparison — measurably cleaner JIT output.

**`And.children()` is a `List`; `AndRowMatcher.children` is an array.** Iterating an `Object[]` avoids `Iterator` allocation and is faster in tight loops. The cost is a one-time `toArray()` at compile time.

### E.4 Wire it through `FilteredRowReader`
Change `FilteredRowReader`'s constructor to take a `RowMatcher` instead of `(ResolvedPredicate, FileSchema)`. Compile happens at the call site that constructs `FilteredRowReader` (likely `ParquetFileReader.createRowReader(...)` around `core/.../reader/ParquetFileReader.java:249-262`).

Replace the call:

```java
if (RecordFilterEvaluator.matchesRow(predicate, delegate, schema)) { ... }
```

with:

```java
if (matcher.test(delegate)) { ... }
```

`matchesRow` and the comparison helpers in `RecordFilterEvaluator` stay for now — they are still useful for testing and as a fallback. They are not deleted in this commit; revisited in E.7.

### E.5 Tests — equivalence with the old path
This is critical. Add `RecordFilterCompilerTest` that for **every** `ResolvedPredicate` variant verifies:

```java
assertThat(matcher.test(row))
    .isEqualTo(RecordFilterEvaluator.matchesRow(predicate, row, schema));
```

across:
- A null value at the column
- A null value at the column for nested paths (where `acc == null` short-circuits)
- Both branches of every operator (EQ true, EQ false, etc.)
- Empty `And`/`Or` rejection (already guarded in records)
- Deeply nested compound predicates
- `IN` predicates with 1, 2, and 30 values (exercises the binary-search threshold if added)
- `BinaryPredicate` with both `signed=true` and `signed=false`

Per the project coding rules: *"Correctness is a top priority… fail early."* The old evaluator is the oracle; the new one must agree on every input.

All existing predicate-related tests must pass unchanged because semantics do not change.

**Commit**: `#NNN Compile predicate tree once per reader into row matchers`

### E.6 Measure
Re-run the three baseline scenarios and the JMH benchmark. Expected:
- Match-all overhead: down meaningfully (the win is per-row dispatch elimination).
- Selective speedup: largely unchanged (selectivity dominates; predicate cost is a small share).
- JMH multi-leaf And: largest relative improvement, since recursion + iterator allocation are eliminated.

If the numbers do not move, **stop and investigate**. Plausible reason: the `StructAccessor.isNull(name)` / `getInt(name)` lookups dominate and the bottleneck has not actually been eliminated — which pushes to E.7.

### E.7 Bypass name-keyed access for flat columns
For top-level non-nested columns (the common case), the leaf matchers hold a direct `int columnIndex` and read from `FlatRowReader` via a new internal accessor:

```java
// New, internal-only on FlatRowReader (or a narrow interface it implements)
boolean isNullAt(int columnIndex);
int getIntAt(int columnIndex);
long getLongAt(int columnIndex);
// ...
```

The compiler picks index-based matchers when the schema column has a flat path, and falls back to name-keyed matchers for nested paths. This eliminates the `HashMap`-keyed-by-string lookups, which (depending on profiling) may be a bigger win than the type-dispatch elimination.

**Commit**: `#NNN Use index-based column access in row matchers for flat schemas`

### E.8 Documentation
The matcher classes are internal — no public API change — so no doc update under `docs/content/` is needed for E.1–E.7. Update only if Stage 2 introduces a public-facing batched reader.

---

## F. Validation checklist (before declaring done)

1. All existing tests pass: `./mvnw verify` (with 180s timeout per command).
2. New equivalence test (E.5) passes for all variants.
3. `RecordFilterBenchmarkTest` median match-all overhead % is materially lower than baseline (target: ≥ 30% reduction in overhead; this is a hypothesis to confirm, not a contract).
4. `RecordFilterBenchmarkTest` selective scenario does not regress.
5. Multi-leaf JMH benchmark shows improvement on at least the `And` scenario.
6. `PageFilterBenchmarkTest` shows no regression.
7. No new public API surface (only internal classes).

---

## G. Risks and edge cases

- **`IsNullPredicate` semantics for missing nested paths**: in current code, `acc == null` (struct path missing) yields `true` for `IsNull`, `false` for `IsNotNull`. Compiled matchers must preserve this exactly — easy to get wrong with a naive null check on the leaf.
- **`BinaryPredicate.signed`** field: must be captured at compile time, not re-evaluated per row.
- **Empty `And`/`Or`**: already rejected at `ResolvedPredicate` construction; no risk, but a test exists.
- **Recursive predicate depth**: `compile()` is recursive — fine for any practical depth, but pathological 10k-deep trees could blow the stack. This matches the existing `matchesRow` behaviour; iterative deepening is not added.
- **JIT warmup in benchmarks**: small benchmarks under-report the win because the old code is also JIT'd warm. JMH's `@Warmup` handles this; the JUnit benchmark less so. Conclusions are not drawn from a single run.
- **Equivalence test must include `null` cases**: easy to forget. Failing here would silently change query results for production users.

---

## H. Capturing the baseline before any changes

The baseline must be captured on the same machine, in the same load conditions, that will be used to measure the post-change numbers. JIT timing is hardware- and JVM-sensitive; numbers from a different machine are not comparable.

Recommended workflow:

1. **Quiesce the machine** — close other heavy processes, plug in to power if on a laptop, disable thermal-throttling sources where possible. Filesystem cache should be warm; first run is discarded.
2. **Build and install `core` so the perf module sees the current `main`**:
   ```bash
   ./mvnw -pl core install -DskipTests
   ```
3. **Run the benchmark three times**, with 10 runs each to lower variance:
   ```bash
   for i in 1 2 3; do
     ./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
       -Dtest=RecordFilterBenchmarkTest -Dperf.runs=10
   done
   ```
   Apply a 180s Maven timeout per invocation. The 10M-row file is created on the first run only and reused thereafter at `performance-testing/end-to-end/target/record_filter_benchmark.parquet`.
4. **Record the median** of the three *match-all overhead %* values and the median *selective speedup ×* in this design doc (or a sibling notes file) as the locked baseline.
5. **After E.1 lands** (the multi-column `And` scenario and the JMH micro-benchmark), repeat the run to capture the additional baselines those scenarios produce. Those baselines are the comparison points for E.5/E.6.

The baseline run is owned by whoever will also run the post-change measurement — keeping both runs on the same machine, JVM, and OS state is more important than who specifically executes the commands. If the change is implemented in a CI-driven workflow, the baseline must be captured in the same CI job ahead of time.

---

## I. Code changes (Stage 1)

This section gives the concrete edits to apply, in commit order. Each subsection corresponds to a step in §E. The intent is that a reader can study this end-to-end and reconstruct the implementation.

Where a class follows the same shape across primitive types (Int / Long / Float / Double), the Int version is shown in full and the analogous variants are summarised. Where a copyright header is omitted in a snippet, use the standard project header from any existing source file.

### I.1 New file — `RowMatcher` interface

`core/src/main/java/dev/hardwood/internal/predicate/RowMatcher.java`

```java
package dev.hardwood.internal.predicate;

import dev.hardwood.row.StructAccessor;

/// Compiled, immutable representation of a [ResolvedPredicate] that evaluates
/// a single row in one virtual call.
///
/// A `RowMatcher` is built once per [dev.hardwood.reader.RowReader] by
/// [RecordFilterCompiler] and reused for every row. By holding all
/// invariant state (column indices, leaf names, struct paths, literal
/// operands, child arrays) in fields rather than recomputing per row, the
/// compiler eliminates the type and operator switches that
/// [RecordFilterEvaluator] performs.
///
/// Each leaf type (one for every combination of value type and operator)
/// is its own concrete class — or factory-produced lambda — so that JIT
/// call sites see a single receiver type and can inline aggressively.
@FunctionalInterface
public interface RowMatcher {

    /// Returns true if the given row matches this predicate.
    ///
    /// Implementations must be thread-safe for repeated invocation by the
    /// same reader (a `RowMatcher` is not shared across threads, but the
    /// JIT may invoke it from compiled code paths concurrently with safepoints).
    boolean test(StructAccessor row);
}
```

### I.2 New file — `RecordFilterCompiler`

`core/src/main/java/dev/hardwood/internal/predicate/RecordFilterCompiler.java`

The compiler walks the predicate tree once and returns one `RowMatcher` per node. For brevity, primitive leaves are produced via factory methods that return distinct lambdas per operator — each lambda compiles to a unique synthetic class, giving the JIT type-stable receivers without 30 hand-written classes.

```java
package dev.hardwood.internal.predicate;

import java.util.Arrays;
import java.util.List;

import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;

/// Compiles a [ResolvedPredicate] into a [RowMatcher] tree once per reader.
///
/// All field-name lookups, struct-path resolutions, and operator decisions
/// are performed at compile time; the produced matcher only reads values
/// and runs comparisons per row.
public final class RecordFilterCompiler {

    private RecordFilterCompiler() {}

    public static RowMatcher compile(ResolvedPredicate predicate, FileSchema schema) {
        return switch (predicate) {
            case ResolvedPredicate.IntPredicate p ->
                    intLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            case ResolvedPredicate.LongPredicate p ->
                    longLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            case ResolvedPredicate.FloatPredicate p ->
                    floatLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            case ResolvedPredicate.DoublePredicate p ->
                    doubleLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            case ResolvedPredicate.BooleanPredicate p ->
                    booleanLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            case ResolvedPredicate.BinaryPredicate p ->
                    binaryLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()),
                            p.op(), p.value(), p.signed());
            case ResolvedPredicate.IntInPredicate p ->
                    intInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values());
            case ResolvedPredicate.LongInPredicate p ->
                    longInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values());
            case ResolvedPredicate.BinaryInPredicate p ->
                    binaryInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values());
            case ResolvedPredicate.IsNullPredicate p ->
                    isNullLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()));
            case ResolvedPredicate.IsNotNullPredicate p ->
                    isNotNullLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()));
            case ResolvedPredicate.And and -> compileAnd(and.children(), schema);
            case ResolvedPredicate.Or or -> compileOr(or.children(), schema);
        };
    }

    // ==================== Compound matchers ====================

    private static RowMatcher compileAnd(List<ResolvedPredicate> children, FileSchema schema) {
        RowMatcher[] compiled = new RowMatcher[children.size()];
        for (int i = 0; i < compiled.length; i++) {
            compiled[i] = compile(children.get(i), schema);
        }
        if (compiled.length == 1) {
            return compiled[0];
        }
        return row -> {
            for (int i = 0; i < compiled.length; i++) {
                if (!compiled[i].test(row)) {
                    return false;
                }
            }
            return true;
        };
    }

    private static RowMatcher compileOr(List<ResolvedPredicate> children, FileSchema schema) {
        RowMatcher[] compiled = new RowMatcher[children.size()];
        for (int i = 0; i < compiled.length; i++) {
            compiled[i] = compile(children.get(i), schema);
        }
        if (compiled.length == 1) {
            return compiled[0];
        }
        return row -> {
            for (int i = 0; i < compiled.length; i++) {
                if (compiled[i].test(row)) {
                    return true;
                }
            }
            return false;
        };
    }

    // ==================== Leaf factories ====================
    //
    // Each factory returns a different lambda per operator. The `switch` on
    // op happens once at compile time; the returned lambda has the operator
    // baked in as a literal comparison — no per-row dispatch.
    //
    // `path` is the array of intermediate struct names (empty for top-level).
    // `name` is the leaf field name. Both are pre-resolved here.

    private static RowMatcher intLeaf(String[] path, String name, FilterPredicate.Operator op, int v) {
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) == v; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) != v; };
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) < v; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) <= v; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) > v; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) >= v; };
        };
    }

    private static RowMatcher longLeaf(String[] path, String name, FilterPredicate.Operator op, long v) {
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) == v; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) != v; };
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) < v; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) <= v; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) > v; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) >= v; };
        };
    }

    private static RowMatcher floatLeaf(String[] path, String name, FilterPredicate.Operator op, float v) {
        // Use Float.compare to match RecordFilterEvaluator.compareFloat semantics for NaN.
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) == 0; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) != 0; };
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) < 0; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) <= 0; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) > 0; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) >= 0; };
        };
    }

    private static RowMatcher doubleLeaf(String[] path, String name, FilterPredicate.Operator op, double v) {
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) == 0; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) != 0; };
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) < 0; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) <= 0; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) > 0; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) >= 0; };
        };
    }

    private static RowMatcher booleanLeaf(String[] path, String name, FilterPredicate.Operator op, boolean v) {
        // BooleanPredicate only honours EQ and NOT_EQ; matchesRow returns true for any other op.
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getBoolean(name) == v; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getBoolean(name) != v; };
            default -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name); };
        };
    }

    private static RowMatcher binaryLeaf(String[] path, String name, FilterPredicate.Operator op,
            byte[] v, boolean signed) {
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); if (a == null || a.isNull(name)) return false;
                    return compareBinary(a.getBinary(name), v, signed) == 0; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); if (a == null || a.isNull(name)) return false;
                    return compareBinary(a.getBinary(name), v, signed) != 0; };
            case LT -> row -> { StructAccessor a = resolve(row, path); if (a == null || a.isNull(name)) return false;
                    return compareBinary(a.getBinary(name), v, signed) < 0; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); if (a == null || a.isNull(name)) return false;
                    return compareBinary(a.getBinary(name), v, signed) <= 0; };
            case GT -> row -> { StructAccessor a = resolve(row, path); if (a == null || a.isNull(name)) return false;
                    return compareBinary(a.getBinary(name), v, signed) > 0; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); if (a == null || a.isNull(name)) return false;
                    return compareBinary(a.getBinary(name), v, signed) >= 0; };
        };
    }

    private static int compareBinary(byte[] left, byte[] right, boolean signed) {
        return signed
                ? BinaryComparator.compareSigned(left, right)
                : BinaryComparator.compareUnsigned(left, right);
    }

    private static RowMatcher intInLeaf(String[] path, String name, int[] values) {
        // Sorted copy enables binary search for medium/large IN lists. The sort is one-time at compile.
        int[] sorted = values.clone();
        Arrays.sort(sorted);
        boolean useBinarySearch = sorted.length >= 16;
        if (useBinarySearch) {
            return row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return Arrays.binarySearch(sorted, a.getInt(name)) >= 0;
            };
        }
        return row -> {
            StructAccessor a = resolve(row, path);
            if (a == null || a.isNull(name)) return false;
            int val = a.getInt(name);
            for (int i = 0; i < sorted.length; i++) {
                if (sorted[i] == val) return true;
            }
            return false;
        };
    }

    private static RowMatcher longInLeaf(String[] path, String name, long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        boolean useBinarySearch = sorted.length >= 16;
        if (useBinarySearch) {
            return row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return Arrays.binarySearch(sorted, a.getLong(name)) >= 0;
            };
        }
        return row -> {
            StructAccessor a = resolve(row, path);
            if (a == null || a.isNull(name)) return false;
            long val = a.getLong(name);
            for (int i = 0; i < sorted.length; i++) {
                if (sorted[i] == val) return true;
            }
            return false;
        };
    }

    private static RowMatcher binaryInLeaf(String[] path, String name, byte[][] values) {
        // Binary IN keeps linear scan (Arrays.equals is the hot path); a sorted+bsearch
        // variant could be added later if profiling shows large IN lists are common.
        byte[][] copy = values.clone();
        return row -> {
            StructAccessor a = resolve(row, path);
            if (a == null || a.isNull(name)) return false;
            byte[] val = a.getBinary(name);
            for (int i = 0; i < copy.length; i++) {
                if (Arrays.equals(val, copy[i])) return true;
            }
            return false;
        };
    }

    private static RowMatcher isNullLeaf(String[] path, String name) {
        return row -> {
            StructAccessor a = resolve(row, path);
            return a == null || a.isNull(name);
        };
    }

    private static RowMatcher isNotNullLeaf(String[] path, String name) {
        return row -> {
            StructAccessor a = resolve(row, path);
            return a != null && !a.isNull(name);
        };
    }

    // ==================== Path resolution ====================

    /// Walks the row through the captured intermediate struct path.
    /// Returns null if any intermediate struct is null.
    /// For top-level columns, `path` is empty and the row itself is returned.
    private static StructAccessor resolve(StructAccessor row, String[] path) {
        StructAccessor current = row;
        for (int i = 0; i < path.length; i++) {
            String segment = path[i];
            if (current.isNull(segment)) {
                return null;
            }
            current = current.getStruct(segment);
        }
        return current;
    }

    private static String[] pathSegments(FileSchema schema, int columnIndex) {
        List<String> elements = schema.getColumn(columnIndex).fieldPath().elements();
        if (elements.size() <= 1) {
            return EMPTY_PATH;
        }
        String[] out = new String[elements.size() - 1];
        for (int i = 0; i < out.length; i++) {
            out[i] = elements.get(i);
        }
        return out;
    }

    private static String leafName(FileSchema schema, int columnIndex) {
        return schema.getColumn(columnIndex).fieldPath().leafName();
    }

    private static final String[] EMPTY_PATH = new String[0];
}
```

Notes for study:

- **Path is captured as `String[]`**, not a `List<String>`, to avoid `Iterator` allocation on the hot path.
- **Top-level columns hit `EMPTY_PATH`**, so `resolve()`'s loop is skipped; HotSpot will inline the empty walk to a no-op.
- **`And.children()` and `Or.children()` are pre-arrayed** in `compiled` rather than iterated as `List` per row.
- **A 1-child `And`/`Or` collapses** to its single child — minor but common after predicate normalisation.
- **`IN` predicates with ≥ 16 values switch to binary search** — a guess at the threshold; revisit once the JMH benchmark exists.
- **`BooleanPredicate` with operators other than `EQ`/`NOT_EQ`** is treated as a non-null check, matching the existing `matchesRow` behaviour exactly.

### I.3 Modify `FilteredRowReader` to take a compiled matcher

`core/src/main/java/dev/hardwood/internal/reader/FilteredRowReader.java`

Replace the `(ResolvedPredicate, FileSchema)` pair with a single `RowMatcher`. The reader no longer imports `RecordFilterEvaluator` or `ResolvedPredicate`.

```diff
-import dev.hardwood.internal.predicate.RecordFilterEvaluator;
-import dev.hardwood.internal.predicate.ResolvedPredicate;
+import dev.hardwood.internal.predicate.RowMatcher;
 import dev.hardwood.reader.RowReader;
 ...
-import dev.hardwood.schema.FileSchema;

 public final class FilteredRowReader implements RowReader {

     private final RowReader delegate;
-    private final ResolvedPredicate predicate;
-    private final FileSchema schema;
+    private final RowMatcher matcher;

     private boolean hasMatch;

-    FilteredRowReader(RowReader delegate, ResolvedPredicate predicate, FileSchema schema) {
+    FilteredRowReader(RowReader delegate, RowMatcher matcher) {
         this.delegate = delegate;
-        this.predicate = predicate;
-        this.schema = schema;
+        this.matcher = matcher;
     }

     @Override
     public boolean hasNext() {
         while (delegate.hasNext()) {
             delegate.next();
-            if (RecordFilterEvaluator.matchesRow(predicate, delegate, schema)) {
+            if (matcher.test(delegate)) {
                 hasMatch = true;
                 return true;
             }
         }
         return false;
     }
```

Everything below the `next()` method (the long block of delegating accessors) is unchanged.

### I.4 Modify `FlatRowReader` to compile and pass the matcher

`core/src/main/java/dev/hardwood/internal/reader/FlatRowReader.java`, around line 148–154:

```diff
+import dev.hardwood.internal.predicate.RecordFilterCompiler;
+import dev.hardwood.internal.predicate.RowMatcher;
 ...
         FlatRowReader reader = new FlatRowReader(buffers, workers, schema, projectedSchema);
         reader.initialize();
         if (filter != null) {
-            return new FilteredRowReader(reader, filter, schema);
+            RowMatcher matcher = RecordFilterCompiler.compile(filter, schema);
+            return new FilteredRowReader(reader, matcher);
         }
         return reader;
```

### I.5 Modify `NestedRowReader` to compile and pass the matcher

`core/src/main/java/dev/hardwood/internal/reader/NestedRowReader.java`, around line 136 — same pattern as `FlatRowReader`:

```diff
+import dev.hardwood.internal.predicate.RecordFilterCompiler;
+import dev.hardwood.internal.predicate.RowMatcher;
 ...
         if (filter != null) {
-            return new FilteredRowReader(reader, filter, schema);
+            RowMatcher matcher = RecordFilterCompiler.compile(filter, schema);
+            return new FilteredRowReader(reader, matcher);
         }
         return reader;
```

### I.6 `RecordFilterEvaluator` — keep, mark internal

Do **not** delete `RecordFilterEvaluator` in this commit. It remains the equivalence oracle for the test in I.7 and a fallback if any future predicate variant is added before its compiled form exists. After I.7 lands and the equivalence test is green, a follow-up commit can collapse `RecordFilterEvaluator` to a thin wrapper that compiles on each call (preserving the public method signature) — or remove it entirely if no external code depends on it. Audit before deleting.

### I.7 New test — equivalence with the old evaluator

`core/src/test/java/dev/hardwood/internal/predicate/RecordFilterCompilerTest.java`

The test uses the existing `matchesRow` as an oracle: for every variant, the compiled matcher must produce the same boolean as the legacy evaluator on the same input. This guarantees the change is semantically transparent.

```java
package dev.hardwood.internal.predicate;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

class RecordFilterCompilerTest {

    // Use whichever in-memory schema/row builder the test code base already provides.
    // The pattern below assumes a TestRow / TestSchema helper exists; substitute with
    // the project's existing equivalent (look for similar tests on RecordFilterEvaluator).
    private final FileSchema schema = TestSchemas.flatPrimitives();   // id:long, value:double, flag:boolean, name:binary

    @Test
    void intPredicate_equivalentForAllOperators() {
        StructAccessor row = TestRows.row(schema, 42, 3.14, true, "hello".getBytes());
        for (FilterPredicate.Operator op : FilterPredicate.Operator.values()) {
            ResolvedPredicate p = new ResolvedPredicate.IntPredicate(0, op, 42);
            assertEquivalent(p, row);
            ResolvedPredicate p2 = new ResolvedPredicate.IntPredicate(0, op, 43);
            assertEquivalent(p2, row);
        }
    }

    @Test
    void longPredicate_equivalentForAllOperators() {
        StructAccessor row = TestRows.row(schema, 42L, 3.14, true, "hello".getBytes());
        for (FilterPredicate.Operator op : FilterPredicate.Operator.values()) {
            assertEquivalent(new ResolvedPredicate.LongPredicate(0, op, 42L), row);
            assertEquivalent(new ResolvedPredicate.LongPredicate(0, op, 43L), row);
        }
    }

    @Test
    void floatAndDoublePredicates_treatNaNLikeOracle() {
        StructAccessor row = TestRows.row(schema, 0, Double.NaN, false, new byte[0]);
        for (FilterPredicate.Operator op : FilterPredicate.Operator.values()) {
            assertEquivalent(new ResolvedPredicate.DoublePredicate(1, op, 1.0), row);
            assertEquivalent(new ResolvedPredicate.DoublePredicate(1, op, Double.NaN), row);
        }
    }

    @Test
    void booleanPredicate_eqAndNotEq() {
        StructAccessor row = TestRows.row(schema, 0, 0.0, true, new byte[0]);
        assertEquivalent(new ResolvedPredicate.BooleanPredicate(2, FilterPredicate.Operator.EQ, true), row);
        assertEquivalent(new ResolvedPredicate.BooleanPredicate(2, FilterPredicate.Operator.NOT_EQ, true), row);
        // Non-EQ/NEQ operators must match matchesRow's "non-null check" fallback.
        assertEquivalent(new ResolvedPredicate.BooleanPredicate(2, FilterPredicate.Operator.GT, true), row);
    }

    @Test
    void binaryPredicate_signedAndUnsigned() {
        byte[] negPrefix = { (byte) 0xFF };
        byte[] posPrefix = { 0x01 };
        StructAccessor row = TestRows.row(schema, 0, 0.0, false, negPrefix);
        for (FilterPredicate.Operator op : FilterPredicate.Operator.values()) {
            assertEquivalent(new ResolvedPredicate.BinaryPredicate(3, op, posPrefix, /*signed=*/true), row);
            assertEquivalent(new ResolvedPredicate.BinaryPredicate(3, op, posPrefix, /*signed=*/false), row);
        }
    }

    @Test
    void inPredicates_smallAndLargeLists() {
        StructAccessor row = TestRows.row(schema, 7, 0.0, false, new byte[0]);
        // small list (linear search path)
        assertEquivalent(new ResolvedPredicate.IntInPredicate(0, new int[] { 1, 2, 7 }), row);
        // large list (binary search path)
        int[] big = new int[64];
        for (int i = 0; i < big.length; i++) big[i] = i * 2;
        assertEquivalent(new ResolvedPredicate.IntInPredicate(0, big), row); // 7 not present
        big[big.length - 1] = 7;
        assertEquivalent(new ResolvedPredicate.IntInPredicate(0, big), row); // 7 present
    }

    @Test
    void nullHandling_topLevelAndNested() {
        StructAccessor flatNull = TestRows.rowWithNulls(schema, /*idNull=*/true, false, false, false);
        for (FilterPredicate.Operator op : FilterPredicate.Operator.values()) {
            assertEquivalent(new ResolvedPredicate.IntPredicate(0, op, 0), flatNull);
        }
        assertEquivalent(new ResolvedPredicate.IsNullPredicate(0), flatNull);
        assertEquivalent(new ResolvedPredicate.IsNotNullPredicate(0), flatNull);

        // Nested path with intermediate null struct — both forms must agree on `acc == null` short-circuit.
        FileSchema nested = TestSchemas.nestedAddress(); // person.address.zip:int
        StructAccessor nestedNullStruct = TestRows.nestedRowWithNullAddress(nested);
        assertEquivalent(new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.EQ, 12345), nestedNullStruct, nested);
        assertEquivalent(new ResolvedPredicate.IsNullPredicate(0), nestedNullStruct, nested);
        assertEquivalent(new ResolvedPredicate.IsNotNullPredicate(0), nestedNullStruct, nested);
    }

    @Test
    void compounds_andOrAndDeeplyNested() {
        StructAccessor row = TestRows.row(schema, 5, 1.0, true, new byte[0]);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.GT, 0),
                new ResolvedPredicate.Or(List.of(
                        new ResolvedPredicate.DoublePredicate(1, FilterPredicate.Operator.LT, 0.0),
                        new ResolvedPredicate.BooleanPredicate(2, FilterPredicate.Operator.EQ, true)))));
        assertEquivalent(p, row);
    }

    private void assertEquivalent(ResolvedPredicate predicate, StructAccessor row) {
        assertEquivalent(predicate, row, schema);
    }

    private void assertEquivalent(ResolvedPredicate predicate, StructAccessor row, FileSchema schemaInUse) {
        boolean expected = RecordFilterEvaluator.matchesRow(predicate, row, schemaInUse);
        boolean actual = RecordFilterCompiler.compile(predicate, schemaInUse).test(row);
        assertThat(actual).as("predicate=%s", predicate).isEqualTo(expected);
    }
}
```

The `TestSchemas` / `TestRows` helpers above are placeholders — substitute the project's existing builders. Look at how the current `RecordFilterEvaluatorTest` (or its sibling tests) constructs schemas and rows; reuse those.

### I.8 New benchmark — multi-column And scenario

Modify `performance-testing/end-to-end/src/test/java/dev/hardwood/perf/RecordFilterBenchmarkTest.java` to add a fourth scenario after the existing three. The structure follows the same `runs/times/rows` triplets pattern.

```diff
         // Selective filter (id < 1% of range — skip 99% of rows)
         long[] selectiveTimes = new long[runs];
         long[] selectiveRows = new long[runs];
         for (int i = 0; i < runs; i++) {
             long start = System.nanoTime();
             selectiveRows[i] = runSelectiveFilter();
             selectiveTimes[i] = System.nanoTime() - start;
         }

+        // Compound match-all And — exercises predicate-tree dispatch overhead.
+        long[] compoundTimes = new long[runs];
+        long[] compoundRows = new long[runs];
+        for (int i = 0; i < runs; i++) {
+            long start = System.nanoTime();
+            compoundRows[i] = runCompoundMatchAllFilter();
+            compoundTimes[i] = System.nanoTime() - start;
+        }
+
         // Print results
         ...
         printResults("Selective filter (id < 1%)", selectiveTimes, selectiveRows, runs);
+        System.out.println();
+        printResults("Compound match-all (id >= 0 AND value < +inf)", compoundTimes, compoundRows, runs);

         double avgNoFilter = avg(noFilterTimes) / 1_000_000.0;
         double avgMatchAll = avg(matchAllTimes) / 1_000_000.0;
         double avgSelective = avg(selectiveTimes) / 1_000_000.0;
+        double avgCompound = avg(compoundTimes) / 1_000_000.0;
         ...
+        System.out.printf("  Compound overhead: %.1f%% (%.0f ms → %.0f ms)%n",
+                100.0 * (avgCompound - avgNoFilter) / avgNoFilter, avgNoFilter, avgCompound);

         // Correctness
         assertThat(noFilterRows[0]).isEqualTo(TOTAL_ROWS);
         assertThat(matchAllRows[0]).isEqualTo(TOTAL_ROWS);
         assertThat(selectiveRows[0]).isLessThan(TOTAL_ROWS);
+        assertThat(compoundRows[0]).isEqualTo(TOTAL_ROWS);
     }
```

And the new helper method, alongside the existing ones:

```java
private long runCompoundMatchAllFilter() throws Exception {
    FilterPredicate filter = FilterPredicate.and(
            FilterPredicate.gtEq("id", 0L),
            FilterPredicate.lt("value", Double.MAX_VALUE));
    long count = 0;
    try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(BENCHMARK_FILE));
         RowReader rows = reader.createRowReader(filter)) {
        while (rows.hasNext()) {
            rows.next();
            count++;
        }
    }
    return count;
}
```

### I.9 New JMH benchmark — `RecordFilterMicroBenchmark`

`performance-testing/micro-benchmarks/src/main/java/dev/hardwood/benchmarks/RecordFilterMicroBenchmark.java`

Measures the inner predicate loop in isolation — no I/O, no Parquet decoding. Compares the legacy `RecordFilterEvaluator.matchesRow` against the compiled `RowMatcher` over a fabricated batch of rows held in memory. This is the cleanest signal for the dispatch-elimination win.

```java
package dev.hardwood.benchmarks;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import dev.hardwood.internal.predicate.RecordFilterCompiler;
import dev.hardwood.internal.predicate.RecordFilterEvaluator;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowMatcher;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms512m", "-Xmx512m" })
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@OperationsPerInvocation(RecordFilterMicroBenchmark.BATCH_SIZE)
public class RecordFilterMicroBenchmark {

    static final int BATCH_SIZE = 4096;

    @Param({ "single", "and2", "and3orChild" })
    public String shape;

    private FileSchema schema;
    private StructAccessor[] rows;            // pre-built batch
    private ResolvedPredicate predicate;
    private RowMatcher compiled;

    @Setup
    public void setup() {
        // Build a synthetic flat schema with id:long, value:double, flag:boolean.
        schema = TestSchemas.flatPrimitives(); // same helper used in the unit test
        rows = new StructAccessor[BATCH_SIZE];
        Random r = new Random(42);
        for (int i = 0; i < BATCH_SIZE; i++) {
            rows[i] = TestRows.row(schema,
                    r.nextLong() & 0xFFFF,
                    r.nextDouble() * 100.0,
                    r.nextBoolean(),
                    new byte[0]);
        }
        predicate = switch (shape) {
            case "single" -> new ResolvedPredicate.LongPredicate(0, FilterPredicate.Operator.GT_EQ, 0L);
            case "and2" -> new ResolvedPredicate.And(List.of(
                    new ResolvedPredicate.LongPredicate(0, FilterPredicate.Operator.GT_EQ, 0L),
                    new ResolvedPredicate.DoublePredicate(1, FilterPredicate.Operator.LT, Double.MAX_VALUE)));
            case "and3orChild" -> new ResolvedPredicate.And(List.of(
                    new ResolvedPredicate.LongPredicate(0, FilterPredicate.Operator.GT_EQ, 0L),
                    new ResolvedPredicate.Or(List.of(
                            new ResolvedPredicate.DoublePredicate(1, FilterPredicate.Operator.LT, 0.0),
                            new ResolvedPredicate.BooleanPredicate(2, FilterPredicate.Operator.EQ, true))),
                    new ResolvedPredicate.LongPredicate(0, FilterPredicate.Operator.LT, Long.MAX_VALUE)));
            default -> throw new IllegalArgumentException(shape);
        };
        compiled = RecordFilterCompiler.compile(predicate, schema);
    }

    @Benchmark
    public void legacy(Blackhole bh) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            bh.consume(RecordFilterEvaluator.matchesRow(predicate, rows[i], schema));
        }
    }

    @Benchmark
    public void compiled(Blackhole bh) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            bh.consume(compiled.test(rows[i]));
        }
    }
}
```

To run only this benchmark:

```bash
./mvnw -pl core install -DskipTests
./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
java -jar performance-testing/micro-benchmarks/target/benchmarks.jar RecordFilterMicroBenchmark
```

The JMH output reports `ns/op` per row (because of `@OperationsPerInvocation(BATCH_SIZE)`), parameterised by `shape`. Compare `legacy` vs `compiled` for each shape.

### I.10 Stage-1 commit sequence

| # | Files | Commit message |
|---|---|---|
| 1 | `RecordFilterBenchmarkTest.java` (compound scenario), new `RecordFilterMicroBenchmark.java` | `#NNN Add multi-column And scenario and JMH micro-benchmark for record filter` |
| 2 | New `RowMatcher.java`, new `RecordFilterCompiler.java`, modified `FilteredRowReader.java`, `FlatRowReader.java`, `NestedRowReader.java`, new `RecordFilterCompilerTest.java` | `#NNN Compile predicate tree once per reader into row matchers` |

Each commit must keep `./mvnw verify` green on its own.

---

## J. Stage 2 — fixed-arity AND/OR matchers

Stage 2 attacks the per-child loop in `compileAnd` / `compileOr`. Stage 1's
generic implementation walked an `Object[]` of children per row, paying for
an array bound check, an array load, and a virtual call per child:

```java
return row -> {
    for (int i = 0; i < compiled.length; i++) {
        if (!compiled[i].test(row)) return false;
    }
    return true;
};
```

The array-load + bound-check is a JIT inlining barrier. Stage 2 replaces it
with fixed-arity classes for arities 2, 3, and 4 — small final-field
classes with the children as named fields and a hand-written boolean
expression body:

```java
private static final class And3Matcher implements RowMatcher {
    private final RowMatcher a;
    private final RowMatcher b;
    private final RowMatcher c;

    And3Matcher(RowMatcher a, RowMatcher b, RowMatcher c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    @Override
    public boolean test(StructAccessor row) {
        return a.test(row) && b.test(row) && c.test(row);
    }
}
```

Symmetric `Or2Matcher`, `Or3Matcher`, `Or4Matcher` for OR. Arities ≥ 5
fall back to a generic `AndNMatcher` / `OrNMatcher` array walker (still
an improvement over the lambda+`Object[]` since the call site is
monomorphic).

`compileAnd` / `compileOr` dispatch on `children.size()`:

```java
return switch (compiled.length) {
    case 1 -> compiled[0];
    case 2 -> new And2Matcher(compiled[0], compiled[1]);
    case 3 -> new And3Matcher(compiled[0], compiled[1], compiled[2]);
    case 4 -> new And4Matcher(compiled[0], compiled[1], compiled[2], compiled[3]);
    default -> new AndNMatcher(compiled);
};
```

**Why this works.** With `final` fields and a stable receiver class per
call site (each leaf factory produces a distinct lambda class), HotSpot
inlines `a.test(row)` / `b.test(row)` / `c.test(row)` fully and fuses the
resulting machine code. JMH numbers: arity-3 AND drops from 6.4 ns/op
(Stage 1, array walk) to 0.57 ns/op (Stage 2, fixed-arity) — a **11.4×
improvement** at arity 3 alone, and **17.6× at arity 4**.

### J.1 Stage 2.5 — arity-2 leaf fusion

Stage 2's `And2Matcher` is a single concrete class shared across every
2-arity AND in the JVM. Its `test(row)` body calls `a.test(row)` and
`b.test(row)` — two *bytecode* call sites at fixed offsets inside that
class. HotSpot maintains an inline cache per bytecode location, not per
matcher instance. So when many query shapes flow through the same
process, the `a.test()` and `b.test()` sites observe more and more
receiver classes; once the count crosses three, the JIT abandons
inlining and falls back to itable dispatch.

For monomorphic per-query workloads this never bites: each query owns
its receiver classes and the call sites stay bimorphic at worst. But a
long-running JVM serving many ad-hoc queries — the realistic analytical
workload — *will* go megamorphic on these shared sites, and Stage 2's
nicely-fused machine code degrades silently.

Stage 2.5 routes the most common shapes through a fusion path
(`RecordFilterFusion`) that emits a *distinct synthetic lambda class
per shape*. Each `(typeA, opA, typeB, opB)` produces its own class
with both comparisons inlined as primitive bytecode operations and no
inner virtual call. There are no shared inner call sites left to
pollute. The benchmark results below confirm the win: under
megamorphic pressure (8 distinct shapes through the same outer
`matcher.test()` site), the fused path runs at 2.13 ns/op while the
generic And2Matcher path runs at 4.60 ns/op — a **2.16× speedup**,
which is the cleanest demonstration of fusion's value because it does
not exist at all in monomorphic measurements.

#### Supported type combinations

| Combination | Same-column range | Notes |
|---|---|---|
| `long + long` | yes | Range filter `id BETWEEN x AND y` is the most common shape this targets. |
| `int + int` | yes | Symmetric to long+long. |
| `double + double` | yes | Uses `Double.compare` to preserve NaN-aware semantics from the legacy evaluator. |
| `long + double` | n/a | Cross-column only — types differ. Also handles `double + long` by swapping to canonical order (AND is commutative for pure leaves). |

Every combination supports all 36 `(opA, opB)` pairs by 6×6 nested
`switch` returning literal-comparison lambdas. Operators not covered
by fusion (boolean predicates, binary/IN predicates, IsNull) fall
through to the generic `And2Matcher` path, as do compound children and
arity ≠ 2.

#### Why this design and not alternatives

Two simpler designs were considered and rejected:

- **Capture the per-operand comparison as a `LongPredicate` field.**
  This collapses 36 lambdas to 6, but reintroduces the megamorphic
  problem: the inner `cmpA.test(...)` call site shares bytecode across
  all `(opA, opB)` pairs and goes megamorphic exactly the way the
  generic path does. Defeats the whole point.
- **Hand-write 36 explicit named classes per type combo.** Equivalent
  to what we have but more verbose. Lambdas inside a nested switch are
  the most compact form that still produces a unique synthetic class
  per case arm.

#### Where the cost goes

A 6×6 switch per type combo is ~36 short single-line lambdas. With
four cross-column variants and three same-column range variants, the
fusion file contains 252 distinct lambdas. Each lambda is ~120
characters of bytecode after javac, so the total static footprint is
roughly the size of one mid-sized `RowReader` implementation —
acceptable for a compiler-only file that does not appear on any hot
path other than the one it accelerates.

### J.2 Validation

The Stage 1 equivalence test (`RecordFilterEvaluatorTest`'s
`matchesRow` helper that asserts legacy and compiled paths agree) is
re-used unchanged. A new `RecordFilterFusionTest` exercises every
fused type combination across all 36 `(opA, opB)` pairs (290
parametrized cases) plus null-column rejection, all by routing the
predicate through `RecordFilterCompiler.compile` and asserting the
fused matcher agrees with the legacy oracle. Leaf semantics are
unchanged, so the existing predicate test suite continues to catch any
divergence in non-fused paths.

### J.3 JMH measurement summary

See the companion `RECORD_FILTER_COMPILATION_RESULTS.md` for the full
table. Key numbers:

| Shape | Stage 1 ns/op | Stage 2 ns/op | Stage 2.5 ns/op |
|---|---:|---:|---:|
| single | 0.530 | 0.501 | 0.501 (unchanged — single-leaf, no fusion) |
| and2 (Long+Double, monomorphic) | 2.192 | 0.568 | 0.601 (within noise) |
| and3 | 6.429 | 0.566 | 0.572 (unchanged — arity > 2) |
| and4 | 11.445 | 0.652 | 0.657 (unchanged) |
| nested (and-of-or) | 10.553 | 0.924 | 1.047 (unchanged — arity > 2) |
| **megamorphic 2-arity AND (8 shapes)** | n/a | **4.60** | **2.13** |

The first five rows confirm fusion is a no-op on the monomorphic case
(C2 already fuses Stage 2 effectively); the last row is the new
result that justifies the work — a 2.16× win that is invisible in any
benchmark that runs only a single shape.

End-to-end wall-clock for compound predicates improves further
(105.7 → 97.6 ms, 1.08×) because even the production benchmark hits
multiple compiled `And2Matcher` instances over its lifetime; the
remaining cost is now in `StructAccessor.getLong(name)` / `isNull(name)`
rather than in predicate dispatch. Stage 3 targets that floor.

---

## K. Stage 3 — index-based fast path

After Stage 2.5 the predicate-dispatch layer is ~0.6 ns/op in
isolation, but the end-to-end compound predicate cost is still
~7.8 ns/row. The gap is `StructAccessor.getLong(name)` and
`isNull(name)` — each of which performs a name → index hash lookup
on the row. Stage 3 eliminates the lookup by translating the file
column index to the projected field index *at compile time* and
emitting matchers that read the row through a narrow internal
interface using the pre-resolved index.

### K.1 Internal interface — `IndexedAccessor`

`core/src/main/java/dev/hardwood/internal/reader/IndexedAccessor.java`

```java
package dev.hardwood.internal.reader;

/// Index-based fast-path accessor by *projected* field index, bypassing
/// the name → index hash lookup performed by StructAccessor.
public interface IndexedAccessor {
    boolean isNullAt(int projectedIndex);
    int getIntAt(int projectedIndex);
    long getLongAt(int projectedIndex);
    float getFloatAt(int projectedIndex);
    double getDoubleAt(int projectedIndex);
    boolean getBooleanAt(int projectedIndex);
    byte[] getBinaryAt(int projectedIndex);
}
```

The contract of every method is: the argument is the index of the
column in the row reader's *projection* (not the file schema). This
matches the existing `RowReader.getLong(int fieldIndex)` semantics —
`FlatRowReader` already stores values in projection order, so its
implementation methods are one-line delegates to the existing int
accessors.

### K.2 `FlatRowReader implements IndexedAccessor`

A single `implements IndexedAccessor` clause and seven one-line
delegate methods. No data-layout changes; the cost is recovered by
moving the file → projected index translation off the hot path.

### K.3 Compiler — the projection-aware overload

`RecordFilterCompiler` gains a 3-argument overload:

```java
public static RowMatcher compile(ResolvedPredicate predicate, FileSchema schema,
        ProjectedSchema projection)
```

Behaviour:

- For each leaf, check whether the column's `FieldPath` is a single
  element (top-level). If so *and* `projection != null`, translate the
  file column index to the projected field index via
  `projection.toProjectedIndex(...)` and emit an indexed leaf from
  `RecordFilterFusionIndexed`. Otherwise emit the original name-keyed
  leaf.
- For 2-arity AND nodes whose children are both top-level primitive
  leaves of compatible types, route through
  `RecordFilterFusionIndexed.tryFuseAnd2` — the indexed equivalent of
  Stage 2.5's fusion path. The fused matcher's body is `IndexedAccessor
  a = (IndexedAccessor) row;` followed by the inlined comparisons; no
  name lookup remains.
- Nested paths (path length > 1) keep using the name-keyed leaves.
  Indexed access is only valid for flat top-level columns.
- Compound matchers (`And`, `Or`) thread the projection through to
  their children unchanged.

The cast `(IndexedAccessor) row` is safe by construction: only
`FlatRowReader` calls the projection-aware overload, and
`FlatRowReader implements IndexedAccessor`. A misuse fails fast with a
`ClassCastException`.

### K.4 The two compile entry points

`FlatRowReader.create` calls the 3-arg overload (with its
`projectedSchema`); `NestedRowReader.create` keeps the 2-arg form. The
nested reader's int-based access has different semantics (per-batch
indexes that change as nested levels are traversed), so it deliberately
does not implement `IndexedAccessor` — its predicates use name-keyed
matchers exclusively.

### K.5 Code shape

`RecordFilterFusionIndexed` mirrors `RecordFilterFusion`'s 36-lambda
nested switches but with index-based access. Single-leaf indexed
factories live in the same file (one method per primitive type × 6
ops). Total: ~250 indexed lambdas plus 24 single-leaf indexed lambdas.
The file is large but mechanical, and unlike Stage 2.5 each lambda is
a single line: there is no path resolution to encode, only the cast
and the two comparisons.

### K.6 Validation

A new `RecordFilterIndexedTest` asserts that for every fusion shape
and primitive single-leaf, the indexed matcher (built via
`compile(predicate, schema, projection)`) and the name-keyed matcher
(built via the 2-arg overload) and the legacy `matchesRow` oracle all
agree on the same row. The test stub implements both `StructAccessor`
and `IndexedAccessor`, which lets the same row instance be evaluated
through either path. 126 parametrized cases plus null-handling cases.

### K.7 Measurement

End-to-end (10M rows, Apple Silicon, JDK 25), comparing Stage 2.5 →
Stage 3:

| Scenario | Stage 2.5 | Stage 3 | Speedup |
|---|---:|---:|---:|
| Match-all (single leaf) | 74.1 ms | **31.5 ms** | **2.35×** |
| Compound (2-leaf AND, fused) | 97.6 ms | **46.6 ms** | **2.10×** |
| Page+record combined | 5.1 ms | **3.9 ms** | **1.31×** |
| Selective (id < 1%) | 3.7 ms | 2.8 ms | (within noise) |

Predicate-only ns/row (subtracting the no-filter baseline of
~19.3 ms / 10 M = 1.93 ns/row of decode cost):

- Match-all: 5.48 ns → **1.22 ns** per row (4.5×)
- Compound: 7.83 ns → **2.73 ns** per row (2.87×)

Both numbers approach the JMH-isolated lower bound. The remaining
~1 ns/row is the cast plus the array bounds check and the
`BitSet.get` for the null mask — the genuine cost of reading two
fields out of memory.