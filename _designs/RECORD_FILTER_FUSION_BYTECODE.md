# Plan: Record Filter Compound-Matcher Fusion

**Status: Implemented**

## Context

After per-(type, operator) leaf factories and fixed-arity `AndN`/`OrN` matchers, the remaining hot-path cost in compound predicates is the inner virtual call inside `And2Matcher` / `Or2Matcher`:

```java
private static final class And2Matcher implements RowMatcher {
    private final RowMatcher a, b;
    public boolean test(StructAccessor row) {
        return a.test(row) && b.test(row);
    }
}
```

When many distinct queries flow through one JVM, the inner call sites `a.test(row)` and `b.test(row)` accumulate leaf classes and go megamorphic. HotSpot can no longer inline the leaves into the compound matcher and per-row cost regresses by 2–3×.

The fix is to compile each fusable arity-2 AND/OR into a single concrete class whose `test(...)` body inlines both leaf comparisons directly. There is no inner virtual call to be megamorphic about.

This design generates the fused classes at build time via a Java annotation processor. The matrix is fixed and covers the most common compound-predicate shapes; everything outside the matrix continues to use the existing fixed-arity matchers. Extending coverage to every fusable shape via runtime codegen is left as a Java 24+ follow-up.

---

## Step 1: Eligibility matrix

Same-column, same-type only. The matrix targets the predicate shapes that dominate analytical workloads — interval scans, anti-interval scans, and small equality lists:

| Family | Connective | Op pairs | Types | Count |
|---|---|---|---|---:|
| Closed / half-open interval | AND | `(>=, <=)`, `(>, <)`, `(>=, <)`, `(>, <=)` | int, long, double | 12 |
| Outside interval | OR | `(<, >)`, `(<=, >=)` | int, long, double | 6 |
| 2-element IN list | OR | `(==, ==)` | int, long | 2 |

Total: **20 tuples × 2 access modes (indexed + name-keyed) = 40 generated classes**.

Diff-column, cross-type, binary, boolean, float, and any op pair outside this list fall back to `And2Matcher` / `Or2Matcher`. The list is conservative on purpose; expanding it is a matter of editing the matrix constant in the processor.

The load-bearing constraint is that each tuple becomes a *distinct* class with a unique receiver type. That is what kills inner-site megamorphism — the body has no inner virtual call to be megamorphic about. Any shared dispatch point inside the body (a `BiPredicate` field, a generic helper that takes the operator as a parameter, a `switch` over op codes inside `test`) defeats fusion.

---

## Step 2: Generated class shape

Two access modes per tuple. Indexed access is used when the row reader exposes top-level fields through `RowReader.getX(int)`:

```java
package dev.hardwood.internal.predicate.fused;

final class FusedLongAndCsCs_GteLte implements RowMatcher {
    private final int idx;
    private final long va;
    private final long vb;

    FusedLongAndCsCs_GteLte(int idx, long va, long vb) {
        this.idx = idx;
        this.va = va;
        this.vb = vb;
    }

    @Override
    public boolean test(StructAccessor row) {
        RowReader r = (RowReader) row;
        if (r.isNull(idx)) return false;
        long v = r.getLong(idx);
        return v >= va && v <= vb;
    }
}
```

Name-keyed access is used for nested struct paths and for callers without a `RowReader`:

```java
final class FusedLongAndCsCs_GteLte_Named implements RowMatcher {
    private final String[] path;
    private final String name;
    private final long va;
    private final long vb;

    FusedLongAndCsCs_GteLte_Named(String[] path, String name, long va, long vb) {
        this.path = path;
        this.name = name;
        this.va = va;
        this.vb = vb;
    }

    @Override
    public boolean test(StructAccessor row) {
        StructAccessor s = RecordFilterCompiler.resolve(row, path);
        if (s == null || s.isNull(name)) return false;
        long v = s.getLong(name);
        return v >= va && v <= vb;
    }
}
```

`path` is `RecordFilterCompiler.EMPTY_PATH` for top-level columns; `resolve` short-circuits to the row itself in that case.

Class names follow `Fused<Type><Connective><Cols><OpPair>[_Named]`, where `<Cols>` is `CsCs` for same-column. The fixed naming scheme allows the registry to dispatch via a static switch with no reflection.

---

## Step 3: Annotation processor module

A new sibling Maven module `core-fusion-codegen` holds:

- `@GenerateFusedMatchers` — `RetentionPolicy.SOURCE` marker annotation, target `TYPE`.
- `FusedMatcherProcessor extends AbstractProcessor` — the processor. The 20-tuple matrix is a constant inside this class. On the first processing round, the processor walks the matrix and emits one `.java` per tuple per access mode via `Filer.createSourceFile(...)` into the `dev.hardwood.internal.predicate.fused` package.
- `META-INF/services/javax.annotation.processing.Processor` registration.

The module produces only one artifact, used at compile time by `core` and never shipped at runtime.

`core/pom.xml` declares the processor under `maven-compiler-plugin`'s `<annotationProcessorPaths>`. Generated sources land under `target/generated-sources/annotations/dev/hardwood/internal/predicate/fused/` and are compiled in the same javac round as the rest of `core`.

A single trigger element lives at `core/src/main/java/dev/hardwood/internal/predicate/FusedMatchers.java`:

```java
@GenerateFusedMatchers
final class FusedMatchers {
    private FusedMatchers() {}
}
```

The processor sees the annotation and emits all 40 generated classes.

---

## Step 4: Registry and dispatch

A hand-written `FusedMatcherRegistry` in `core` maps the canonicalised tuple to a static factory on the corresponding generated class:

```java
final class FusedMatcherRegistry {
    static RowMatcher lookup(ResolvedPredicate a, ResolvedPredicate b,
            FileSchema schema, IntUnaryOperator topLevelFieldIndex,
            boolean isAnd) {
        // canonicalise leaf order, classify (type, op-pair, accessMode)
        // switch over the 20-tuple matrix; null on miss
    }
}
```

`RecordFilterCompiler.compileAnd` / `compileOr`, when `FUSION_ENABLED && children.size() == 2`, query the registry; on a miss they fall through to the existing `compileAll` walker.

A static `boolean FUSION_ENABLED` is read once at class init from the system property `hardwood.recordfilter.fusion`. Default `true`; set `-Dhardwood.recordfilter.fusion=false` to bypass the registry and route every arity-2 AND/OR through `And2Matcher` / `Or2Matcher`. Internal-only knob, used by benchmarks to A/B the fusion contribution.

---

## Null and NaN semantics

Generated matchers preserve the same row-level semantics as the unfused leaf factories:

- **Intermediate struct null** — name-keyed bodies guard `RecordFilterCompiler.resolve(row, path) != null` before access.
- **Direct field null** — bodies guard `acc.isNull(name)` or `delegate.isNull(idx)` before reading the primitive.
- **AND short-circuit** — leaf B is not evaluated when leaf A is false or null.
- **OR short-circuit** — leaf B is not evaluated when leaf A is true; on null/false, leaf B is evaluated.
- **Double comparisons** — `Double.compare(D, D)I` to match the leaf-factory NaN ordering (NaN sorts above `+Infinity`). Double is excluded from the equality-IN family because NaN equality is never true; see Step 1.

---

## Validation

### Equivalence — `RecordFilterFusionTest` (new)

For every tuple in the matrix the test compiles the predicate through both code paths and checks the result against a hand-computed expected value:

1. Compile via `RecordFilterCompiler.compile(p, schema, projection::toProjectedIndex)` — indexed path → `Fused*` class.
2. Compile via `RecordFilterCompiler.compile(p, schema)` — name-keyed path → `Fused*_Named` class.
3. Compare both against the boolean expected value derived from the test inputs (interval membership, equality lists, null rejection, NaN ordering).

Each matcher's runtime class is asserted to confirm the right path was taken — the expected `Fused*` class when the tuple is in-matrix and fusion is enabled, `And2Matcher` / `Or2Matcher` when fusion is disabled or the tuple is off-matrix.

### Existing tests

`RecordFilterCompilerTest`, `RecordFilterIndexedTest` continue to pass unchanged — fusion is transparent because both compile paths produce equivalent matchers.

`./mvnw verify` continues to pass; no public API changes.

### Benchmarks

Two suites cover fusion:

- **`RecordFilterBenchmarkTest`** (`performance-testing/end-to-end`) — runs queries through `ParquetFileReader.buildRowReader().filter(...).build()` against a 10M-row file, so `FilteredRowReader.hasNext`'s outer call site sees one fused class per matrix-covered shape. Contenders: no-filter, match-all, selective, compound match-all (different columns, falls back to `And2Matcher`), page+record, fused AND interval (same column), fused OR anti-interval (same column).
- **`RecordFilterMicroBenchmark`** (`performance-testing/micro-benchmarks`, JMH) — isolates the per-row dispatch cost of `RecordFilterCompiler.compile` + `RowMatcher.test` against an in-memory `StructAccessor[]`. Includes the same-column fused shapes `and2Fused`, `or2Fused`, `intIn2Fused` alongside their unfused siblings.

Both suites have two arms per query, controlled by the system property:

- `-Dhardwood.recordfilter.fusion=true` (default) — APT registry resolves matrix-covered shapes; off-matrix shapes use the fallback.
- `-Dhardwood.recordfilter.fusion=false` — fixed-arity matchers only.

For JMH, the flag must be passed as a JVM argument (`java -Dhardwood.recordfilter.fusion=false -jar …`) because JMH forks a fresh JVM per fork.

### Measured impact

Recorded 2026-05-04 on Apple Silicon, JDK 25.0.3.

#### End-to-end (`RecordFilterBenchmarkTest`, 10M rows, 5 runs averaged)

| Contender | Fusion ON | Fusion OFF | Speedup |
|---|---:|---:|---:|
| Compound match-all (`id>=0 AND value<+inf`) — different columns, fallback path | 49.3 ms | 48.7 ms | 1.01× |
| **Fused AND interval** (`id>=0 AND id<MAX`) — same column, in-matrix | **36.5 ms** | 50.1 ms | **1.37×** |
| **Fused OR anti-interval** (`id<0 OR id>-1`) — same column, in-matrix | **36.7 ms** | 50.5 ms | **1.38×** |

Throughput on the fused contenders: ~273 M rows/s vs ~199 M rows/s on the same predicate forced through `And2Matcher` / `Or2Matcher`. The compound-match-all row barely moves between arms — the registry returns `null` for cross-column compounds in both modes, so the same `And2Matcher` runs.

#### JMH (`RecordFilterMicroBenchmark`, 1 fork × 3 warmup × 3 measurement × 4096 ops/invocation)

| Shape | Fusion ON | Fusion OFF |
|---|---:|---:|
| `single` (single long leaf) | 0.40 ns/op | 0.41 ns/op |
| `and2` (different columns, fallback) | 0.47 | 0.48 |
| `or2` (different columns, fallback) | 0.41 | 0.41 |
| `and2Fused` (same long col, `GT_EQ + LT`) | 0.43 | 0.42 |
| `or2Fused` (same long col, `LT + GT`) | 0.41 | 0.43 |
| `intIn2Fused` (same int col, `EQ + EQ`) | 0.44 | 0.43 |

JMH numbers are flat between arms — by design, each shape runs in an isolated fork, so the inner virtual call site inside `And2Matcher.test` stays monomorphic (one leaf class) and HotSpot inlines it as aggressively as the fused class. This is the expected result and confirms that fusion's payoff is *not* per-row arithmetic — it's the avoided megamorphism that arises when many distinct leaf classes funnel through one `a.test(...)` site over a JVM's lifetime, which is exactly the condition the e2e benchmark reproduces by running multiple compounds in a single JVM before the fused contenders.

---

## Risks and edge cases

- **Cast safety in indexed fusion.** Indexed bodies cast the row to `RowReader`. Safe by construction — the indexed compile path is reached only from `FlatRowReader` and `NestedRowReader`, both implementing `RowReader`.
- **Canonicalisation correctness.** Same-column AND/OR canonicalisation relies on commutativity. Both `And` and `Or` are commutative regardless of leaf order; the leaf factories use the same commutative semantics via short-circuit on first match/miss. Equivalence tests cover both orderings.
- **Annotation processor isolation.** The processor module is build-only; the runtime classpath of `hardwood-core.jar` does not contain `core-fusion-codegen` classes. Verified by inspecting the packaged jar contents in CI.
- **Generated source size.** 40 classes × ~30 lines each ≈ 1,200 lines under `target/generated-sources`. Compiles cleanly; no IDE indexing surprises observed elsewhere in the codebase with similarly sized generated trees.

---

## Future work

### Java 24+ runtime-codegen overlay (`java.lang.classfile`, JEP 484)

Extend coverage from the 20-tuple matrix to *every* fusable arity-2 AND/OR by emitting a per-query hidden class via `java.lang.classfile`. Shipped as a multi-release-JAR overlay so the Java 21 baseline is unaffected.

Coverage on the overlay:

- Same-column and diff-column.
- Same-type and cross-type numeric (int/long, int/double, long/double, canonicalised).
- Binary leaves, with the per-leaf `byte[]` constants captured in instance fields.
- Boolean and float, including NaN-aware float comparisons.
- Every op pair, not just the build-time short list.

The dispatcher splits via MR-JAR: base `core/src/main/java/dev/hardwood/internal/predicate/FusionDispatcher.java` delegates to `FusedMatcherRegistry.lookup(...)`. The overlay at `core/src/main/java24/dev/hardwood/internal/predicate/FusionDispatcher.java` delegates to a new `ClassFileCodegen.emit(...)` which builds the class via `ClassFile.of().build(...)` and defines it through `MethodHandles.lookup().defineHiddenClass(bytes, true)`. The overlay path does not consult the build-time registry — every fusable arity-2 AND/OR goes straight to JEP 484 codegen on Java 24+.

`core/pom.xml` already builds an MR-JAR; a new `java24/` source root with `<release>24</release>` mirrors the existing `java22/` block. The `java24/` root contains only `FusionDispatcher` and `ClassFileCodegen`; the base dispatcher, the registry, and the generated APT classes remain in the base for runtimes < 24.

First-call latency on the overlay is ~0.5–2 ms per `defineHiddenClass` (verifier + class definition); amortises away for analytical scans. A per-shape cache keyed by `(typeA, opA, vA, typeB, opB, vB, connective, accessMode, idx)` would eliminate it for repeated queries — also future work.

### Build-time matrix expansion

Adding boolean, float, binary, or diff-column tuples to the APT set if the gap between matrix-covered and uncovered shapes proves large enough to matter on Java 21 workloads. Mechanical change — extend the matrix constant in the processor and the registry switch.

### Arity-3+ fusion

Same emitter strategy extended to arity 3 / 4. Useful if `AndN` / `OrN` walker megamorphism at arity ≥ 3 is a measured cost.