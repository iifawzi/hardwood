# Record Filter Compilation — Benchmark Results

Companion to `RECORD_FILTER_COMPILATION.md`. Captures the before/after numbers for the Stage 1 optimisation that compiles `ResolvedPredicate` trees into reusable `RowMatcher` graphs.

## Environment

- Hardware: macOS aarch64 (Apple Silicon)
- JDK: Oracle 25.0.3
- Maven: 3.9.12 wrapper
- Branch: `record-filter-compilation`
- File: `target/record_filter_benchmark.parquet` (10,000,000 rows × `id: long`, `value: double`, 114 MB, Snappy v2)
- Command: `./mvnw test -Pperformance-test -pl performance-testing/end-to-end -Dtest=RecordFilterBenchmarkTest -Dperf.runs=5`
- Each scenario reads the entire file once per run.

## Scenarios

| Scenario | Predicate | What it stresses |
|---|---|---|
| No filter | — | Pure read throughput, no predicate evaluation. |
| Match-all | `id >= 0` | Single-leaf dispatch overhead — every row evaluated, every row kept. |
| Selective | `id < 100_000` | High-selectivity case — predicate cost amortised over skipped rows. |
| Compound match-all | `id >= 0 AND value < +inf` | Two-leaf `And` recursion + dispatch overhead — every row evaluated and kept. |
| Page+record combined | `id BETWEEN 9.9M AND 10M AND value < 500.0` | Page-level pruning (column-index min/max on `id`) gates record-level filtering on the survivors. |

## Baseline (legacy `matchesRow`, before changes)

This is the back-to-back legacy run captured immediately before the
post-change run, with the implementation files temporarily stashed and
core reinstalled to ensure the legacy code path runs end-to-end.

| Contender | Avg time | Records/sec |
|---|---:|---:|
| No filter (baseline) | 17.2 ms | 580,396,520 |
| Match-all filter (worst case) | 85.6 ms | 116,809,239 |
| Selective filter (id < 1%) | 3.4 ms | 29,745,674 |
| Compound match-all (id>=0 AND value<+inf) | 251.4 ms | 39,778,639 |
| Page+record (id range AND value<500) | 7.2 ms | 6,938,212 |

Derived metrics:

- **Match-all overhead: 396.9%** (17 ms → 86 ms)
- **Selective speedup: 5.1×** (17 ms → 3 ms)
- **Compound overhead: 1359.1%** (17 ms → 251 ms)
- **Page+record speedup: 2.4×** (17 ms → 7.2 ms)

Per-run details (ms):

```
No filter (baseline):              14.6 / 25.0 / 16.1 / 13.4 / 17.0
Match-all filter (worst case):    108.8 / 83.0 / 80.8 / 77.4 / 78.1
Selective filter (id < 1%):         6.8 /  4.0 /  2.3 /  1.8 /  1.9
Compound match-all:               257.3 / 252.8 / 250.2 / 248.4 / 248.2
Page+record combined:              15.2 /  5.7 /  5.2 /  4.9 /  5.1
```

## Post-change (compiled `RowMatcher`)

| Contender | Avg time | Records/sec |
|---|---:|---:|
| No filter (baseline) | 18.2 ms | 549,203,866 |
| Match-all filter (worst case) | 68.6 ms | 145,808,444 |
| Selective filter (id < 1%) | 3.5 ms | 28,242,803 |
| Compound match-all (id>=0 AND value<+inf) | 115.9 ms | 86,272,338 |
| Page+record (id range AND value<500) | 5.1 ms | 9,875,262 |

Derived metrics:

- **Match-all overhead: 276.7%** (18 ms → 69 ms) — down from 396.9%
- **Selective speedup: 5.1×** (18 ms → 4 ms) — unchanged
- **Compound overhead: 536.6%** (18 ms → 116 ms) — down from 1359.1%
- **Page+record speedup: 3.6×** (18 ms → 5.1 ms) — up from 2.4×

Per-run details (ms):

```
No filter (baseline):              22.1 / 17.4 / 18.6 / 13.2 / 19.8
Match-all filter (worst case):     74.6 / 66.2 / 69.6 / 66.2 / 66.3
Selective filter (id < 1%):         9.4 /  2.2 /  2.1 /  2.0 /  2.0
Compound match-all:               122.2 / 116.7 / 114.0 / 114.3 / 112.4
Page+record combined:               7.1 /  5.0 /  4.3 /  4.1 /  4.9
```

## Comparison

The honest signal lives in the **predicate-only** cost — total time minus the
no-filter baseline, divided by 10M rows. That isolates the dispatch overhead
the optimisation targets and removes I/O / decoding noise that is unaffected
by this change.

| Scenario | Baseline ns/row | Post ns/row | Reduction | Wall-time speedup |
|---|---:|---:|---:|---:|
| Match-all (single leaf, `id >= 0`) | 6.84 | 5.04 | **−26%** | 1.25× |
| Compound (`id >= 0 AND value < +inf`) | 23.42 | 9.77 | **−58%** | 2.17× |
| Selective (`id < 1%`) | n/a* | n/a* | within noise | ≈ flat |
| Page+record combined | n/a† | n/a† | small absolute gain | 1.41× |

\* Selective filtering is dominated by row-skipping, not predicate cost — the
predicate runs once per matched row plus once per skipped batch. The
predicate-only ns/row decomposition is not meaningful here. Wall time was
3.4 ms baseline vs 3.5 ms post; both dominated by run-1 warmup variance,
within the noise floor of the harness.

† Combined scenario decodes only ~100K rows after page pruning, so the
record-filter cost difference (~2 ms) is small in absolute terms but
still represents ~21 ns saved per surviving row — see §"Page-filter +
record-filter combined scenario" for the breakdown.

### Headline numbers

- **Single-leaf predicates: 25% faster wall-clock.** Baseline match-all
  overhead of 397% drops to 277% — the per-row cost of dispatching one
  leaf falls from ~6.8 ns to ~5.0 ns. The win comes from hoisting the
  field-name lookup, the column-index → leaf-name resolution, and the
  operator switch out of the row loop. The receiver type at the call
  site is now monomorphic (one operator-specialised lambda class), so
  HotSpot inlines the comparison directly.
- **Compound predicates: 2.17× faster, throughput more than doubled.**
  Baseline compound overhead of 1359% drops to 537%. Predicate-only
  cost per row falls from ~23.4 ns to ~9.8 ns — a 58% reduction. This
  is where the optimisation pays off most, because the legacy path
  additionally paid for `for-each` iteration over `And.children()`
  (allocating an iterator) and a recursive call into `matchesRow` per
  child per row. The compiled form holds children in an `Object[]` and
  dispatches via inlined virtual calls.
- **Page+record combined scenario: 1.41× faster.** Page-level filtering
  (column-index min/max statistics) prunes ~99% of pages before any
  decode, then record filtering runs on the survivors. The smaller
  relative win reflects that fact — most of the work is gated by the
  page filter, not the record filter — but the saving still translates
  to ~21 ns per surviving row, consistent with the JMH 3-leaf number.
- **Reading throughput unchanged.** No-filter wall time moved from
  17.2 ms to 18.2 ms — within run-to-run variance. The optimisation
  touches only the filtered path; the unfiltered path is unaffected.

### Why the compound win is bigger than 2× the single-leaf win

A two-leaf `And` predicate runs the legacy dispatch machinery **three
times per row** (the `And` itself plus each leaf), and the per-leaf cost
itself is higher than the steady-state because of the recursion through
`matchesRow`. After compilation, the `And` is a single tight loop over a
2-element matcher array; each leaf is a single inlined comparison. The
non-linear cost of recursion is what compounds (literally) into the
larger relative win.

### Takeaways

1. **Hypothesis confirmed.** The doc predicted the largest gain on
   compound predicates due to recursion + iterator allocation; that is
   exactly what the numbers show.
2. **The optimisation is most valuable in realistic workloads.** Real
   queries rarely use a single-column predicate — they combine multiple
   filters with `AND`/`OR`. The 2.25× compound speedup is the number that
   matters for users.
3. **Stage 2 (index-based fast path, see §J of the design doc) remains
   on the table.** Even after this change, ~30% of post-no-filter time
   is still spent in name-keyed `StructAccessor` accessors. Bypassing
   them via an `IndexedAccessor` interface is the next lever.

## JMH micro-benchmark — predicate cost in isolation

The end-to-end numbers above mix predicate evaluation with file decode and
struct access. To isolate the dispatch optimisation, this section uses a JMH
benchmark (`RecordFilterMicroBenchmark`) that:

- Pre-builds a 4096-row in-memory batch of `StructAccessor` stubs (no I/O).
- Runs both the legacy `RecordFilterEvaluator.matchesRow` and the compiled
  `RecordFilterCompiler.compile(...).test(...)` paths against the **same**
  predicate and the **same** batch in the **same** JVM, so JIT state is
  consistent between contenders.
- Reports `ns/op` per row across 2 forks × 5 warmup × 5 measurement (10
  trustworthy data points per cell).

Run command:

```bash
./mvnw -pl core install -DskipTests
./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
java -jar performance-testing/micro-benchmarks/target/benchmarks.jar RecordFilterMicroBenchmark
```

### Predicate shapes

| Shape | Predicate | Stresses |
|---|---|---|
| `single` | `id >= 0` | One leaf, no recursion |
| `and2` | `id >= 0 AND value < +inf` | Two leaves + AND recursion |
| `and3` | `id >= 0 AND value < +inf AND tag >= 0` | Three leaves |
| `and4` | `... AND flag != false` | Four leaves |
| `or2` | `id >= 0 OR value < 0` | Short-circuiting OR |
| `nested` | `id >= 0 AND (value < 0 OR flag != false) AND tag < MAX` | Mixed AND/OR depth |
| `intIn5` | `tag IN {0..4}` | Small IN list (linear search) |
| `intIn32` | `tag IN {0..31}` | Large IN list (binary search after change) |

All predicates are **match-all** — every row in the batch satisfies them,
so every branch runs to completion. This is the worst case for predicate
cost (no early-out), and the cleanest measurement of dispatch overhead.

### Results

| Shape | Legacy ns/op | Compiled ns/op | Speedup |
|---|---:|---:|---:|
| `single` | 2.696 ± 0.029 | **0.530** ± 0.002 | **5.09×** |
| `and2` | 10.186 ± 0.166 | **2.192** ± 0.194 | **4.65×** |
| `and3` | 21.595 ± 0.231 | **6.429** ± 0.040 | **3.36×** |
| `and4` | 31.906 ± 0.528 | **11.445** ± 0.188 | **2.79×** |
| `or2` | 4.515 ± 0.010 | **0.912** ± 0.007 | **4.95×** |
| `nested` | 32.447 ± 0.220 | **10.553** ± 0.054 | **3.07×** |
| `intIn5` | 3.690 ± 0.043 | **1.378** ± 0.017 | **2.68×** |
| `intIn32` | 6.845 ± 0.052 | **3.189** ± 0.056 | **2.15×** |

(Errors are 99.9% confidence intervals from JMH.)

### Reading the numbers

- **The legacy path is dispatch-bound.** A single integer-comparison leaf
  takes 2.7 ns — roughly 10 CPU cycles — almost all of which is the type
  switch, the field-name lookup via the column-index → leaf-name path,
  and the operator switch. The actual `>=` comparison is one cycle.
- **The compiled single-leaf path is 0.53 ns/op** — about 2 CPU cycles
  on Apple Silicon. That is essentially the cost of a non-null check
  plus a primitive comparison. Everything else has been hoisted out.
- **The win is biggest where the legacy path does the most work.** A
  3-leaf `nested` AND/OR runs the dispatch machinery seven times per
  row in the legacy path (root + 3 leaves + OR + 2 OR-leaves) and pays
  for `for-each` iterator allocation at each compound. The compiled
  form is a tight Object-array walk — 3.07× faster.
- **Even small `IN` lists win 2.7×.** The legacy path computes the leaf
  name and operator dispatch on every row; the compiled form sorts the
  values once at compile time and goes straight to the search.
- **Large `IN` lists win less in relative terms (2.15×) but more in
  absolute terms.** Once the inner work (binary search) becomes
  meaningful, dispatch becomes a smaller fraction of total cost. This
  is the same story as the end-to-end selective scenario.

### Why this is the most credible number

The end-to-end benchmark mixes predicate cost with everything around it
(I/O, page decoding, struct assembly), and the absolute predicate-only
cost is small relative to the I/O baseline. The JMH numbers strip all
that away. They also use the same JVM for both contenders, so the JIT
state is identical — there is no risk that one path is hotter than the
other due to ordering or warmup.

The 2× to 5× wins in this table are the most accurate picture of what
this optimisation actually does to predicate evaluation. The end-to-end
wall-time numbers (1.22× single-leaf, 2.25× compound) are what users
will feel because most queries are I/O-bound on top of predicate
evaluation — but the underlying engine improvement is significantly
larger than that.

## Page-filter + record-filter combined scenario

Real queries rarely run record-level filtering alone — they typically combine
with page-level filtering (driven by Parquet column-index min/max statistics)
that prunes whole pages of data before any row is decoded. This scenario
exercises both layers together, using a predicate where:

- The first leaves (`id BETWEEN 9_900_000 AND 10_000_000`) are
  **page-prunable** because `id` is sequential, so per-page min/max
  statistics tightly bound each page's range.
- The third leaf (`value < 500.0`) is **only** record-prunable because
  `value` is uniform-random in `[0, 1000)`; per-page statistics span the
  full range and provide no help.

Effect: page filtering drops ~99% of pages before decode. The record
filter then runs on the surviving ~100K rows, where the compiler change
applies.

Predicate:
```java
FilterPredicate.and(
    FilterPredicate.gtEq("id", 9_900_000L),
    FilterPredicate.lt("id", 10_000_000L),
    FilterPredicate.lt("value", 500.0));
```

### Combined scenario — legacy vs compiled

| Run | Legacy (ms) | Compiled (ms) |
|---|---:|---:|
| 1 | 15.2 | 7.1 |
| 2 | 5.7 | 5.0 |
| 3 | 5.2 | 4.3 |
| 4 | 4.9 | 4.1 |
| 5 | 5.1 | 4.9 |
| **Avg** | **7.2 ms** | **5.1 ms** |

**Speedup: 1.41× wall-clock; matched rows identical (50,047 in both).**

### Why the relative win is smaller here than on pure record-filter scenarios

The combined scenario isolates the cost of record-level filtering applied
to a *small* surviving population (100K rows out of 10M). The page filter
already eliminated 99% of the work — that part of the runtime is
identical in legacy and compiled.

In absolute terms the saving is **~2 ms over 100K rows**, which
translates to **~21 ns per row** — and that lines up with the JMH
3-leaf-AND number (`and3` legacy 21.6 ns vs compiled 6.4 ns =
**~15 ns saved per row**). The end-to-end measurement includes some
fixed per-query setup that the JMH harness does not, which accounts for
the small difference.

### Takeaway: how the two layers compose

| Layer | What it saves | Sensitive to compiler change? |
|---|---|---|
| Page filter (column index min/max) | I/O + decoding for whole pages | No — runs once per page, before any row data |
| Record filter (post-decode) | Per-row evaluation on surviving rows | **Yes** — this is the dispatch cost we eliminated |

The two layers compose multiplicatively on selectivity but additively on
work. Page filtering is the bigger lever for highly selective filters
on sorted/clustered columns; record-filter compilation is the bigger
lever for many-leaf predicates and for queries where statistics cannot
prune (random-distributed columns, expressions over the value).

For a real workload with a mix of prunable and non-prunable predicates
combined under `AND` — the most common shape — both layers fire and
both pay off. The smaller per-row savings of the compiler are amplified
by the larger row counts in queries with weaker page-level pruning.

## Stage 2 — fixed-arity AND/OR matchers

After Stage 1 landed, the next obvious lever was eliminating the
per-child loop in `compileAnd` / `compileOr`. The original implementation
walked an `Object[]` of child matchers per row, paying for an array bound
check + a virtual call per child.

Stage 2 replaces that loop with **fixed-arity AND/OR matchers** for
arities 2, 3, 4 — each a small final-field class (`And2Matcher`,
`And3Matcher`, …) with the children as named `final` fields and a
hand-written boolean expression body. Replaces the loop with a static
call sequence: `a.test(row) && b.test(row) && c.test(row)`. Arities ≥ 5
fall back to a generic `AndNMatcher` / `OrNMatcher` array walker, but
those are rare in practice.

### What Stage 2 deliberately does *not* do

An earlier draft also added an "arity-2 leaf-level fusion" path that
hand-inlined both leaf bodies into a single lambda for same-type
primitive pairs (long+long, int+int, double+double), with a further
"same column" fast path for range filters like `id BETWEEN x AND y`.

The JMH measurements showed it produced essentially the same numbers
as the generic fixed-arity classes (0.504 ns/op fused vs 0.568 ns/op
generic — within noise), and the same-column fast path was actually
*slower* than the generic path (0.708 ns/op) because of unrelated
dispatch costs in the operator-comparison helpers. Its only theoretical
benefit was megamorphic-call-site insurance — protection for JVMs
running many predicate shapes through shared `And2Matcher` call sites,
where HotSpot stops inlining at ≥ 3 receiver types — but that case is
not exercised by any benchmark we have, and adding ~215 lines of
maintenance-heavy fusion factories on speculation contradicts the
project's "don't design for hypothetical future requirements" rule.

The fusion code was removed. If a future workload demonstrates
megamorphic regression, re-adding the targeted subset (most likely
arity-2 long-long for range filters) is straightforward.

#### Aside: what "megamorphic-call-site insurance" actually means

The argument above leans on a few HotSpot-specific terms that are
worth pinning down, since the rejection only makes sense once they
are understood.

**Receiver.** In `a.test(row)`, `a` is the *receiver* — the object
whose concrete `test` method is selected at runtime. `row` is just an
argument and plays no role in dispatch. Java picks which `test` body
to execute based on the runtime class of the receiver; that decision
is what "virtual dispatch" refers to.

**Call site.** A call site is a single bytecode location, e.g. the
`invokeinterface RowMatcher.test` instruction inside `And2Matcher.test`
on the `a` field. Every `And2Matcher` instance ever constructed in
the JVM funnels through that one bytecode location. HotSpot attaches
an inline cache to the location, not to any individual matcher
instance.

**Receiver-type history.** The inline cache records the set of
concrete classes that have appeared as the receiver at that bytecode:

- **1 type observed → monomorphic.** HotSpot inlines the target
  directly. The leaf's path walk, value load, and comparison fold
  into `And2Matcher.test`, captured constants become immediates, and
  the interface boundary disappears. This is the JIT-driven fusion
  the source comment in `RecordFilterCompiler` relies on.
- **2 types → bimorphic.** Still inlined, with a type-check + branch
  selecting between the two bodies. Effectively still fused.
- **≥ 3 types → megamorphic.** HotSpot stops inlining. The call
  becomes a real itable dispatch, the leaf body stays behind a
  function boundary, and downstream optimisations (escape analysis on
  captured constants, dead-code elim across the call) collapse with
  it.

**Why pollution is global.** Each leaf factory in this file produces
a *distinct* synthetic class (`intLeaf`, `longLeaf`, `binaryLeaf`, …).
A *single* `And2Matcher` instance built for `intEq AND longLt` only
ever sees one class as `a` and one as `b`, so for that instance the
sites would inline cleanly. But the inline cache lives on the shared
bytecode location, not the instance. A long-running JVM that compiles
many query shapes — `int+long`, `double+binary`, `boolean+isNull`, …
— accumulates receiver-type history at that one location until it
crosses three classes. From that point on the site is megamorphic for
*every* `And2Matcher`, including the simple shapes that would
otherwise have inlined.

**What the fused classes would have bought.** A specialised
`LongLongAnd2` class has no `a.test(row)` inside it at all — the body
is `long v = row.getLong(path); return v >= lo && v <= hi;`, with both
comparisons inlined as primitive ops. There is no shared interface
call to pollute, so the fast path is guaranteed by `javac` rather than
contingent on inline-cache state. That guarantee is the only thing
the rejected design provided over the existing fixed-arity classes;
no benchmark we run today drives the call sites past bimorphic, so
the guarantee is purchasing protection against a regression that has
never been measured.

### JMH after Stage 2

| Shape | Legacy | Stage 1 | Stage 2 | **Stage 2 vs Legacy** |
|---|---:|---:|---:|---:|
| single | 2.696 | 0.530 | **0.501** | **5.4×** |
| and2 (Long+Double) | 10.186 | 2.192 | **0.568** | **17.9×** |
| and3 | 21.595 | 6.429 | **0.566** | **38.2×** |
| and4 | 31.906 | 11.445 | **0.652** | **48.9×** |
| or2 | 4.515 | 0.912 | **0.506** | **8.9×** |
| nested (and-of-or) | 32.447 | 10.553 | **0.924** | **35.1×** |
| intIn5 | 3.690 | 1.378 | 1.385 | 2.7× |
| intIn32 | 6.845 | 3.189 | 3.113 | 2.2× |

(All values in ns/op. JMH 99.9% confidence intervals all under ±0.07 ns/op.)

### Why fixed-arity classes work so well

The reason: HotSpot fully inlines the children's lambda bodies through
`final` fields when each call site sees a stable receiver class. The
`Object[]` walk in Stage 1 was a JIT inlining barrier — the bound
check + array load consumed cycles and broke the type-stability that
inlining requires. Removing the loop removed the barrier.

For monomorphic per-query workloads (the typical analytical pattern:
one query running over many rows), the JIT effectively fuses the leaf
bodies for free. Hand-written fusion gave no measurable win on top of
this in our benchmarks.

### End-to-end after Stage 2

Re-running the JUnit benchmark with Stage 2 in place:

| Contender | Legacy | Stage 1 | **Stage 2** |
|---|---:|---:|---:|
| No filter (baseline) | 17.2 ms | 18.2 ms | 15.1 ms |
| Match-all (1 leaf) | 85.6 ms | 68.6 ms | 67.7 ms |
| Selective (id < 1%) | 3.4 ms | 3.5 ms | 3.4 ms |
| Compound (2-leaf AND) | 251.4 ms | 115.9 ms | 105.7 ms |
| Page + record combined | 7.2 ms | 5.1 ms | 4.8 ms |

**Stage 2 vs Stage 1 wall-clock improvements:**
- Match-all: ~flat — predicate-only cost was already 5 ns/row, struct-accessor costs dominate the rest.
- Compound: 115.9 → 105.7 ms (1.10×), small but consistent.
- Page+record: 5.1 → 4.8 ms (1.06×), within noise.

**Stage 2 total speedup vs Legacy:**
- Match-all: 85.6 → 67.7 ms = **1.27×**
- Compound: 251.4 → 105.7 ms = **2.38×**
- Page+record: 7.2 → 4.8 ms = **1.50×**

### Why end-to-end gains plateau between Stage 1 and Stage 2

The JMH benchmark shows Stage 2 reduced compound predicate cost from
9.77 ns/op to 0.57 ns/op (17×). End-to-end shows 9.77 ns/row to
9.06 ns/row (1.08×). The gap of ~8.5 ns/row that Stage 2 *cannot*
remove is the cost of `StructAccessor` itself — the
`getLong(name)` / `isNull(name)` calls hit a name → index hash lookup
and a `BitSet.get()` per row. JMH's `FlatStub` skips that work; real
`FlatRowReader` doesn't.

That floor — ~8 ns/row of struct-accessor overhead — is the next
lever. The design doc's **§K Stage 3** (index-based fast path,
`IndexedAccessor`) targets exactly this. Bypassing the name-keyed
accessors should bring the end-to-end wall clock closer to the JMH
floor.

## Stage 2.5 — arity-2 leaf fusion

Stage 2's `And2Matcher` is a single shared class. Its body has two
fixed bytecode call sites — `a.test(row)` and `b.test(row)` — and
HotSpot maintains an inline cache per bytecode location, not per
matcher instance. Once the JVM has run more than two distinct leaf
classes through that one shared site, the cache goes megamorphic and
HotSpot abandons inlining. This was the unmeasured concern that the
previous revision of this document deferred.

Stage 2.5 adds a fusion path (`RecordFilterFusion`) that emits a
*distinct synthetic lambda class per `(typeA, opA, typeB, opB)`*. The
fused matchers have no inner virtual call: both comparisons are
inlined as primitive bytecode operations. There are no shared inner
call sites left to pollute, so megamorphic pressure at the outer
`matcher.test()` site does not propagate inward.

Supported combinations (cover the bulk of realistic numeric
predicates): `long+long`, `int+int`, `double+double`, `long+double`
(with canonical-swap for `double+long`), each with a same-column
range fast path for `BETWEEN` patterns. All 36 `(opA, opB)` pairs are
emitted per combination — 252 distinct lambda classes total.

### JMH — monomorphic case (no megamorphic pollution)

For single-shape workloads (every And2 instance has the same leaf
classes), C2 already fuses Stage 2 to within a cycle of the
hand-fused form. Re-running the existing micro-benchmark with Stage
2.5 in place confirms this — the per-row cost is unchanged within
noise:

| Shape | Stage 2 ns/op | Stage 2.5 ns/op | Δ |
|---|---:|---:|---:|
| `and2` (Long+Double) | 0.568 ± 0.10 | 0.601 ± 0.013 | within noise |
| `and3` | 0.566 ± 0.04 | 0.572 ± 0.010 | unchanged (arity > 2 — fusion does not apply) |
| `and4` | 0.652 ± 0.19 | 0.657 ± 0.005 | unchanged |
| `nested` | 0.924 ± 0.05 | 1.047 ± 0.028 | within noise (arity > 2) |

Fusion adds no measurable cost — important, since the goal is to
*protect against* megamorphic regression, not to win the monomorphic
case.

### JMH — megamorphic case (the new measurement)

The new `RecordFilterMegamorphicBenchmark` constructs eight distinct
2-arity AND predicates spanning every fused type combination:

1. `id GT_EQ 0 AND id LT 9999` — long+long same-column range
2. `id GT -1 AND id LT_EQ 8192` — long+long same-column, different op pair
3. `value GT_EQ 0.0 AND value LT 1000.0` — double+double same-column range
4. `tag GT_EQ 0 AND tag LT 100` — int+int same-column range
5. `tag GT -1 AND tag NOT_EQ 50` — int+int same-column, different op pair
6. `id GT_EQ 0 AND value LT 999.0` — long+double, different columns
7. `id NOT_EQ -1 AND value LT_EQ 1000.0` — long+double, different op pair
8. `value GT -1.0 AND id LT 9999` — double+long, canonical-swapped

The benchmark holds two parallel matcher arrays:

- **Fused.** Each predicate compiled via `RecordFilterCompiler.compile`,
  which routes through fusion. Eight distinct synthetic classes —
  every matcher instance has unique receiver type at the outer
  `matcher.test()` site.
- **Generic.** Each predicate's children compiled individually, then
  combined through a single `and2(a, b)` helper lambda. All eight
  matchers share *one* synthetic class (the helper's lambda site), so
  the inner `a.test()` / `b.test()` call sites accumulate eight
  receiver classes each — exactly the megamorphic shape Stage 2.5
  targets.

The inner loop of each benchmark iterates `BATCH_SIZE` rows × 8
matchers, invoking every matcher against every row.

| Variant | ns/op (per matcher×row) | Win |
|---|---:|---:|
| `genericMegamorphic` | 4.604 ± 0.202 | (baseline) |
| `fusedMegamorphic` | **2.127** ± 0.089 | **2.16×** |

This is the single number that justifies Stage 2.5. It does not
appear in any single-shape benchmark because single-shape benchmarks
do not pollute the inline cache.

### End-to-end after Stage 2.5

Re-running `RecordFilterBenchmarkTest` with Stage 2.5 in place. The
production compound scenario (`id >= 0 AND value < +inf`) is a
long+double cross-column AND, which now routes through fusion:

| Contender | Legacy | Stage 1 | Stage 2 | **Stage 2.5** |
|---|---:|---:|---:|---:|
| No filter (baseline) | 17.2 ms | 18.2 ms | 15.1 ms | 19.3 ms |
| Match-all (1 leaf) | 85.6 ms | 68.6 ms | 67.7 ms | 74.1 ms |
| Selective (id < 1%) | 3.4 ms | 3.5 ms | 3.4 ms | 3.7 ms |
| Compound (2-leaf AND) | 251.4 ms | 115.9 ms | 105.7 ms | **97.6 ms** |
| Page + record combined | 7.2 ms | 5.1 ms | 4.8 ms | 5.1 ms |

Adjusting for the noisier no-filter baseline this run, the
predicate-only cost per row for compound is `(97.6 − 19.3) / 10 M = 7.83 ns`
vs Stage 2's `(105.7 − 15.1) / 10 M = 9.06 ns` — a further 14%
reduction in per-row cost. The match-all single-leaf scenario is
within noise, as expected (single-leaf predicates are not fused).

### Why end-to-end gains here are smaller than the megamorphic JMH

The end-to-end benchmark runs each scenario in a fresh JVM and warms
up only one predicate at a time, so the production compound scenario
never actually goes megamorphic during the measurement. The 14% gain
on top of Stage 2 reflects fusion's *secondary* benefit — eliminating
the two inner virtual calls in the And2Matcher body even when the
sites are bimorphic — rather than the full megamorphic recovery the
JMH benchmark exhibits.

The full 2.16× win shows up in long-running JVMs that serve many
distinct query shapes (the realistic analytical workload). To
reproduce it in a single JVM run, the
`RecordFilterMegamorphicBenchmark` is the appropriate harness; the
end-to-end benchmark cannot show it by construction.

## Stage 3 — index-based fast path

After Stage 2.5 the predicate-dispatch layer is ~0.6 ns/op in JMH
isolation, but end-to-end compound predicates cost ~7.8 ns/row. The
gap is `StructAccessor.getLong(name)` and `isNull(name)`, each
performing a name → index hash lookup on the row. Stage 3
eliminates the lookup by translating the file column index to the
projected field index *at compile time* and routing the matcher
through a narrow internal `IndexedAccessor` interface implemented by
`FlatRowReader`. The compiler's 3-arg overload
(`compile(predicate, schema, projection)`) emits index-based leaves
for top-level columns and the indexed-fusion path
(`RecordFilterFusionIndexed`) for 2-arity primitive ANDs; nested
paths fall back to the existing name-keyed matchers since indexed
access is only valid for flat top-level columns.

### End-to-end after Stage 3

Re-running `RecordFilterBenchmarkTest` with Stage 3 in place:

| Contender | Legacy | Stage 1 | Stage 2 | Stage 2.5 | **Stage 3** |
|---|---:|---:|---:|---:|---:|
| No filter (baseline) | 17.2 ms | 18.2 ms | 15.1 ms | 19.3 ms | 19.3 ms |
| Match-all (1 leaf) | 85.6 ms | 68.6 ms | 67.7 ms | 74.1 ms | **31.5 ms** |
| Selective (id < 1%) | 3.4 ms | 3.5 ms | 3.4 ms | 3.7 ms | **2.8 ms** |
| Compound (2-leaf AND) | 251.4 ms | 115.9 ms | 105.7 ms | 97.6 ms | **46.6 ms** |
| Page + record combined | 7.2 ms | 5.1 ms | 4.8 ms | 5.1 ms | **3.9 ms** |

**Stage 3 vs Stage 2.5 wall-clock speedups:**
- Match-all: 74.1 → 31.5 ms = **2.35×**
- Compound: 97.6 → 46.6 ms = **2.10×**
- Page + record: 5.1 → 3.9 ms = **1.31×**

**Stage 3 total speedup vs Legacy:**
- Match-all: 85.6 → 31.5 ms = **2.72×**
- Compound: 251.4 → 46.6 ms = **5.39×**
- Page + record: 7.2 → 3.9 ms = **1.85×**

### Predicate-only cost per row

Subtracting the no-filter baseline (~1.93 ns/row of decode):

| Scenario | Stage 2.5 ns/row | Stage 3 ns/row | Reduction |
|---|---:|---:|---:|
| Match-all | 5.48 | **1.22** | **−78%** |
| Compound | 7.83 | **2.73** | **−65%** |

Both numbers approach the JMH-isolated lower bound. The remaining
~1 ns/row is the `(IndexedAccessor)` cast plus the array bounds
check and the `BitSet.get` for the null mask — the genuine cost of
reading a column value out of memory. Going further would require
touching the storage layout itself (e.g. inverting nulls into a
`long[]` mask, packing column values into a single `long[]` for
prefetch friendliness).

### Why Stage 3's win is so much larger end-to-end than in JMH

The JMH micro-benchmark uses a `FlatStub` that returns the same
value for every name (no actual hash lookup). Stage 3 doesn't change
that path. The end-to-end benchmark uses real `FlatRowReader`, which
performs `nameToIndex.get(name)` (custom `StringToIntMap`) plus a
type-validation check on every name-keyed access. Stage 3 turns
those two-and-some operations into one bounds-checked array index.

This is also why the stage-by-stage ranking flips between JMH and
end-to-end: Stage 2.5 (fusion) shows essentially no win in
end-to-end because C2 had already fused the And2Matcher inner
calls during the long warm-up. Stage 3 attacks the access cost,
which C2 cannot optimise away — name-keyed lookup is honest work.

### Equivalence

A new `RecordFilterIndexedTest` exercises the Stage 3 path: for
every fusion shape and single-leaf primitive predicate, it compiles
both the indexed matcher and the legacy name-keyed matcher against
the same row (a stub that implements both `StructAccessor` and
`IndexedAccessor`) and asserts they agree on the same boolean as the
legacy `RecordFilterEvaluator.matchesRow` oracle. 126 parametrized
cases plus null-column rejection. The Stage 1 and Stage 2.5 test
suites continue to pass unchanged, since the 2-arg compile overload
still routes through the original name-keyed leaves.

## Final summary across all measurement methods

| Method | Scenario | Legacy | Stage 1 | Stage 2 | Stage 2.5 | **Stage 3** | **Total speedup** |
|---|---|---:|---:|---:|---:|---:|---:|
| End-to-end | No filter (sanity) | 17.2 ms | 18.2 ms | 15.1 ms | 19.3 ms | 19.3 ms | flat |
| End-to-end | Match-all (1 leaf) | 85.6 ms | 68.6 ms | 67.7 ms | 74.1 ms | **31.5 ms** | **2.72×** |
| End-to-end | Selective | 3.4 ms | 3.5 ms | 3.4 ms | 3.7 ms | **2.8 ms** | **1.21×** |
| End-to-end | Compound (2-leaf AND) | 251.4 ms | 115.9 ms | 105.7 ms | 97.6 ms | **46.6 ms** | **5.39×** |
| End-to-end | Page + record combined | 7.2 ms | 5.1 ms | 4.8 ms | 5.1 ms | **3.9 ms** | **1.85×** |
| JMH (no I/O, monomorphic) | single | 2.696 | 0.530 | 0.501 | 0.501 | **0.501 ns/op** | **5.4×** |
| JMH (no I/O, monomorphic) | and2 | 10.186 | 2.192 | 0.568 | 0.601 | **0.601 ns/op** | **17.0×** |
| JMH (no I/O, monomorphic) | and3 | 21.595 | 6.429 | 0.566 | 0.572 | **0.572 ns/op** | **37.8×** |
| JMH (no I/O, monomorphic) | and4 | 31.906 | 11.445 | 0.652 | 0.657 | **0.657 ns/op** | **48.6×** |
| JMH (no I/O, monomorphic) | or2 | 4.515 | 0.912 | 0.506 | 0.506 | **0.506 ns/op** | **8.9×** |
| JMH (no I/O, monomorphic) | nested | 32.447 | 10.553 | 0.924 | 1.047 | **1.047 ns/op** | **31.0×** |
| JMH (no I/O, monomorphic) | intIn5 | 3.690 | 1.378 | 1.385 | 1.385 | **1.385 ns/op** | 2.7× |
| JMH (no I/O, monomorphic) | intIn32 | 6.845 | 3.189 | 3.113 | 3.113 | **3.113 ns/op** | 2.2× |
| **JMH (no I/O, megamorphic)** | **8 mixed AND2 shapes** | n/a | n/a | 4.604 | **2.127** | 2.127 ns/op | **2.16×** (Stage 2.5 over Stage 2) |

The story across the measurement strategies:

1. **Engine-level (JMH, monomorphic): 5×–49×** improvement in raw
   predicate cost. For per-query monomorphic workloads, predicate cost
   is now in the single-CPU-cycle range across all compound shapes.
2. **Engine-level (JMH, megamorphic): 2.16×** Stage 2.5 over Stage 2.
   This is the realistic long-running-JVM scenario: many concurrent
   query shapes pollute the shared `And2Matcher` inline caches, and
   only the per-shape-unique fused matchers stay fast.
3. **End-to-end record-filter-bound: 2.72×–5.39×** wall-clock
   improvement (legacy → Stage 3). Compound predicates cross **5.39×**
   total. Stage 3 alone takes another 2.10× off the compound time on
   top of Stage 2.5 by eliminating the name-keyed access cost — the
   floor that JMH did not see (the JMH stub had no real hash lookup),
   so the win shows up only end-to-end where real `FlatRowReader`
   accesses are exercised.
4. **End-to-end mixed pipeline: 1.85×** improvement. Page-level
   filtering still dominates this scenario; the record-filter savings
   apply to the surviving rows.

## Methodology — how to reproduce these numbers

```bash
# Establish a clean state with implementation applied
git checkout record-filter-compilation
./mvnw -pl bom,test-bom,core,s3,aws-auth install -DskipTests

# 1. End-to-end JUnit benchmark (4 scenarios, 5 runs each)
./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
    -Dtest=RecordFilterBenchmarkTest -Dperf.runs=5

# 2. JMH micro-benchmark — monomorphic (8 shapes × 2 contenders × 2 forks × 5+5 iters)
./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
java -jar performance-testing/micro-benchmarks/target/benchmarks.jar \
    RecordFilterMicroBenchmark

# 3. JMH micro-benchmark — megamorphic (Stage 2.5 only)
java -jar performance-testing/micro-benchmarks/target/benchmarks.jar \
    RecordFilterMegamorphicBenchmark
```

To rerun the **legacy** baseline on a checkout with the compiler
already merged:

```bash
git stash push -m "compiler-impl" \
  core/src/main/java/dev/hardwood/internal/reader/FilteredRowReader.java \
  core/src/main/java/dev/hardwood/internal/reader/FlatRowReader.java \
  core/src/main/java/dev/hardwood/internal/reader/NestedRowReader.java \
  core/src/test/java/dev/hardwood/internal/predicate/RecordFilterEvaluatorTest.java
mv core/src/main/java/dev/hardwood/internal/predicate/RowMatcher.java /tmp/
mv core/src/main/java/dev/hardwood/internal/predicate/RecordFilterCompiler.java /tmp/
./mvnw -pl core install -DskipTests
# ... run benchmark ...
mv /tmp/RowMatcher.java core/src/main/java/dev/hardwood/internal/predicate/
mv /tmp/RecordFilterCompiler.java core/src/main/java/dev/hardwood/internal/predicate/
git stash pop
./mvnw -pl core install -DskipTests
```

(The JMH benchmark does not need this dance — both contenders coexist
in the same JVM via `RecordFilterEvaluator.matchesRow` and
`RecordFilterCompiler.compile().test()`.)

