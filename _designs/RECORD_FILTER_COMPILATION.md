# Plan: Record Filter Compilation

**Status: Implemented**

## Context

Record-level filtering evaluates a `ResolvedPredicate` tree against every decoded row that survived page-level pruning. The original implementation walked the predicate tree in [`RecordFilterEvaluator.matchesRow`] for *each* row, paying a sealed-type switch over 12 predicate kinds, an operator switch (6 operators), name-keyed accessor lookups (`StructAccessor.isNull(name)` and `getX(name)` both go through a `name → index` hash), and iterator allocation for every `And`/`Or` recursion. None of this work depends on the row — only on the predicate tree, which is fixed at reader-construction time.

This design replaces the per-row interpreter with a compiler that produces one `RowMatcher` per query. The compiler resolves field paths, picks the right operator, and bakes both into a small lambda whose call site is monomorphic per query — so HotSpot inlines the comparison directly. A subsequent layer translates top-level columns to projected-index access and routes through a narrow `IndexedAccessor` interface implemented by `FlatRowReader`, eliminating the `name → index` hash on the hot path.

The work is staged so each layer lands as its own change with its own measurable delta:

1. **Stage 1** — Compile the predicate tree into a `RowMatcher` graph. Hoist field-name lookups, struct-path resolution, and operator switches out of the per-row loop.
2. **Stage 3** — For top-level columns on a flat reader, bypass name-keyed accessors via projected-index access through `IndexedAccessor`.

---

## Step 1: Compile the Predicate Tree (Stage 1)

### New file: `internal/predicate/RowMatcher.java`

Functional interface returned by the compiler. Single method `boolean test(StructAccessor row)`. Internal — not part of the public API.

### New file: `internal/predicate/RecordFilterCompiler.java`

Walks a `ResolvedPredicate` tree once and returns one `RowMatcher` per node.

```java
public static RowMatcher compile(ResolvedPredicate predicate, FileSchema schema);
```

| Node | Compiled form |
|---|---|
| `IntPredicate(col, op, v)` | Lambda per operator with the leaf field name and operand baked in |
| `LongPredicate(col, op, v)` | Same, 6 lambdas (one per operator) |
| `Float`/`Double`/`Boolean`/`Binary` | Same shape; float/double use `Float.compare`/`Double.compare` to match the legacy NaN ordering |
| `Int`/`Long`/`BinaryIn` | One-time sort; binary search above 16 entries, linear scan below |
| `IsNull` / `IsNotNull` | Resolves intermediate struct path and returns `acc == null \|\| acc.isNull(name)` (and inverse) |
| `And(children)` | Recursively compile each child, then a generic `AndNMatcher` over a `RowMatcher[]` |
| `Or(children)` | Same as `And` but disjunctive |

Two concrete decisions that follow from the JIT-inlining argument:

- **One lambda per `(type, operator)` pair**, not one class per type with a switch inside. Splitting the switch out at compile time turns the per-row branch into a literal comparison.
- **`And`/`Or` children are stored as `RowMatcher[]`**, not `List<RowMatcher>`. Iterating an `Object[]` avoids `Iterator` allocation in the hot loop.

Path resolution: `pathSegments` extracts the intermediate struct names as a `String[]`; for top-level columns (`elements().size() ≤ 1`) the path is the shared `EMPTY_PATH` and the resolve loop is a no-op.

### Modify `internal/reader/FilteredRowReader.java`

Replace the `(ResolvedPredicate, FileSchema)` pair with a single `RowMatcher`. Inside `hasNext()`:

```java
while (delegate.hasNext()) {
    delegate.next();
    if (matcher.test(delegate)) {
        hasMatch = true;
        return true;
    }
}
```

### Modify `internal/reader/FlatRowReader.java` and `internal/reader/NestedRowReader.java`

Compile the predicate at reader-construction time and pass the resulting matcher to `FilteredRowReader`. The Stage 1 wiring uses the 2-arg `compile(predicate, schema)` overload from both readers.

### Equivalence

`RecordFilterEvaluator.matchesRow` is retained as the oracle. A new test asserts the compiled matcher returns the same boolean as the legacy evaluator for every predicate variant, every operator, both null branches, nested intermediate-null cases, deeply nested compounds, and the small/large `IN`-list paths.

**Files:**
- `core/src/main/java/dev/hardwood/internal/predicate/RowMatcher.java` (new)
- `core/src/main/java/dev/hardwood/internal/predicate/RecordFilterCompiler.java` (new)
- `core/src/main/java/dev/hardwood/internal/reader/FilteredRowReader.java` (modify — accept `RowMatcher`)
- `core/src/main/java/dev/hardwood/internal/reader/FlatRowReader.java` (modify — compile at construction)
- `core/src/main/java/dev/hardwood/internal/reader/NestedRowReader.java` (modify — compile at construction)
- `core/src/test/java/dev/hardwood/internal/predicate/RecordFilterCompilerTest.java` (new — equivalence with legacy oracle)

---

## Step 2: Index-Based Fast Path (Stage 3)

After Stage 1, the predicate-dispatch layer is significantly reduced. End-to-end, however, every leaf still pays for `StructAccessor.getLong(name)` and `isNull(name)`, both of which run a custom `name → index` hash on the row. For top-level columns, that index is known *at compile time* — the compiler just needs a way to express "read the value at projected index `i`" without going through the name-keyed API.

### New file: `internal/reader/IndexedAccessor.java`

Narrow internal interface declaring projected-index accessor methods:

```java
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

The argument is the column's index in the reader's *projection*, not the file schema. `FlatRowReader` already stores values in projection order, so the implementation is one-line delegates to the existing int-indexed accessor methods.

### Modify `internal/reader/FlatRowReader.java`

`implements IndexedAccessor`, with the seven delegate methods. No data-layout change.

### Modify `internal/predicate/RecordFilterCompiler.java`

Add a 3-arg overload:

```java
public static RowMatcher compile(ResolvedPredicate predicate, FileSchema schema, ProjectedSchema projection);
```

For each leaf, if `projection != null` and the column's `FieldPath` has length 1 (top-level), the compiler translates the file column index to the projected index via `projection.toProjectedIndex(...)` and emits an indexed-leaf lambda whose body casts the row to `IndexedAccessor` and reads through the projected index. Nested paths fall back to the name-keyed factories regardless — indexed access only applies to flat top-level columns.

Indexed leaves are inline factories on `RecordFilterCompiler` paralleling the name-keyed ones — one per `(type, operator)` pair, plus `IsNull` / `IsNotNull`. Compound matchers are unchanged: `compileAnd` and `compileOr` thread `projection` through to their children.

The cast `(IndexedAccessor) row` is safe by construction. Only `FlatRowReader.create` calls the 3-arg overload, and `FlatRowReader implements IndexedAccessor`. A misuse fails fast with `ClassCastException`.

### Why nested readers stay name-keyed

`NestedRowReader` exposes per-batch indexes that change as nested levels are traversed; it does not implement `IndexedAccessor`. Nested predicates therefore continue to use name-keyed leaves. This is also why the indexed path is gated on `isTopLevel(schema, columnIndex)` — even a flat reader's compiler must fall back to name-based access for any leaf whose path traverses a struct.

### Modify `internal/reader/FlatRowReader.java` (compile call)

Switch the constructor-time `RecordFilterCompiler.compile(filter, schema)` to the 3-arg overload, passing the reader's `projectedSchema`.

### Equivalence

`RecordFilterIndexedTest` exercises the indexed path. For every primitive single-leaf predicate and all 36 `(opA, opB)` pairs of compound shapes, it builds a stub row implementing both `StructAccessor` and `IndexedAccessor`, then asserts that the indexed compile path, the name-keyed compile path, and the legacy `RecordFilterEvaluator.matchesRow` oracle all return the same boolean.

**Files:**
- `core/src/main/java/dev/hardwood/internal/reader/IndexedAccessor.java` (new)
- `core/src/main/java/dev/hardwood/internal/reader/FlatRowReader.java` (modify — implement `IndexedAccessor`, use 3-arg compile)
- `core/src/main/java/dev/hardwood/internal/predicate/RecordFilterCompiler.java` (modify — add 3-arg overload, indexed-leaf factories)
- `core/src/test/java/dev/hardwood/internal/predicate/RecordFilterIndexedTest.java` (new — three-way equivalence)

---

## Validation

End-to-end: `performance-testing/end-to-end/.../RecordFilterBenchmarkTest` covers five scenarios over a synthetic 10M-row Parquet file:

| Scenario | Predicate | What it stresses |
|---|---|---|
| No filter | — | Read throughput floor |
| Match-all (1 leaf) | `id >= 0` | Single-leaf dispatch on every row |
| Selective (1%) | `id < 100_000` | Predicate cost amortised over rejected rows |
| Compound match-all | `id >= 0 AND value < +inf` | Two-leaf AND on every row |
| Page+record combined | `id BETWEEN ... AND value < 500` | Page filter prunes 99 %; record filter runs on the survivors |

JMH: `performance-testing/micro-benchmarks/.../RecordFilterMicroBenchmark` measures the predicate-only cost in isolation by pre-loading a 4096-row in-memory batch and running both the legacy `matchesRow` and the compiled `RowMatcher` against the same predicate in the same JVM.

A monorepo `./mvnw verify` continues to pass — the optimisation is internal-only, no public API changes, and `RecordFilterEvaluator` remains as the equivalence oracle for tests.

---

## Results

Numbers to be filled in after running `RecordFilterBenchmarkTest` and `RecordFilterMicroBenchmark` on this branch (legacy vs compiled).

---

## Risks and edge cases

- **`IsNull` semantics for missing nested paths.** In the legacy evaluator, `acc == null` (intermediate struct missing) yields `true` for `IsNull` and `false` for `IsNotNull`. The compiled matchers preserve this — `resolve()` returns `null` if any intermediate struct is null, and the leaf factories check `a == null` before any value access.
- **`BinaryPredicate.signed`.** Captured at compile time and baked into the chosen comparator; never re-checked per row.
- **`BooleanPredicate` with non-`EQ`/`NOT_EQ` operators.** The legacy evaluator returns `true` for any non-null boolean in this case; the compiled boolean leaves preserve this fallback in their `default` switch arm.
- **Empty `And`/`Or`.** Already rejected at `ResolvedPredicate` construction; the test suite covers it.
- **Indexed cast safety.** Only `FlatRowReader.create` invokes the 3-arg compile overload, and `FlatRowReader` implements `IndexedAccessor`. The cast is a contract between the compiler and the readers that opt into it.
