/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import dev.hardwood.internal.predicate.RecordFilterCompiler;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowMatcher;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;

/// Megamorphic-call-site benchmark: demonstrates the fusion path's robustness
/// when many different 2-leaf AND shapes flow through the JVM and pollute the
/// inline caches that the generic [RecordFilterCompiler] `And2Matcher` relies
/// on for inlining.
///
/// **Setup.** Eight distinct primitive 2-leaf AND predicates are constructed,
/// covering every fused type combination (long+long same/diff, int+int
/// same/diff, double+double same/diff, long+double, double+long-canonical).
/// For each predicate:
/// - The `fused` array holds the matcher returned by `RecordFilterCompiler.compile`,
///   which routes through `RecordFilterFusion` and produces a distinct synthetic
///   lambda class per (typeA, opA, typeB, opB) — no shared inner virtual calls.
/// - The `generic` array holds an equivalent matcher built by compiling each
///   leaf separately and combining them through a single `and2(a, b)` helper
///   lambda, which is the same shape as the production `And2Matcher`. All 8
///   matchers share the same `a.test()` / `b.test()` bytecode locations, so
///   the inline caches accumulate 8 receiver classes each → megamorphic.
///
/// **Inner loop.** Each benchmark iterates BATCH_SIZE rows × 8 matchers,
/// invoking every matcher against every row. The outer matcher.test() site
/// is megamorphic in both variants (8 receiver classes); the difference is
/// what happens inside the body — fused matchers have no inner virtual call,
/// generic matchers have two that themselves go megamorphic.
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar RecordFilterMegamorphicBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms512m", "-Xmx512m" })
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@OperationsPerInvocation(RecordFilterMegamorphicBenchmark.BATCH_SIZE
        * RecordFilterMegamorphicBenchmark.SHAPE_COUNT)
public class RecordFilterMegamorphicBenchmark {

    static final int BATCH_SIZE = 4096;
    static final int SHAPE_COUNT = 8;

    private FileSchema schema;
    private StructAccessor[] rows;
    private RowMatcher[] fused;
    private RowMatcher[] generic;

    @Setup
    public void setup() {
        schema = buildSchema();
        rows = buildRows(BATCH_SIZE, 42L);
        ResolvedPredicate[] predicates = buildPredicates();
        fused = new RowMatcher[predicates.length];
        generic = new RowMatcher[predicates.length];
        for (int i = 0; i < predicates.length; i++) {
            fused[i] = RecordFilterCompiler.compile(predicates[i], schema);
            generic[i] = compileGenericAnd2(predicates[i], schema);
        }
    }

    @Benchmark
    public void fusedMegamorphic(Blackhole bh) {
        RowMatcher[] ms = fused;
        StructAccessor[] batch = rows;
        for (int i = 0; i < batch.length; i++) {
            StructAccessor r = batch[i];
            for (int j = 0; j < ms.length; j++) {
                bh.consume(ms[j].test(r));
            }
        }
    }

    @Benchmark
    public void genericMegamorphic(Blackhole bh) {
        RowMatcher[] ms = generic;
        StructAccessor[] batch = rows;
        for (int i = 0; i < batch.length; i++) {
            StructAccessor r = batch[i];
            for (int j = 0; j < ms.length; j++) {
                bh.consume(ms[j].test(r));
            }
        }
    }

    // ==================== Predicate construction ====================

    /// Eight distinct shapes covering every fused type combo. Each must
    /// produce a distinct (typeA, opA, typeB, opB) tuple so the fused
    /// matchers exercise eight distinct synthetic classes.
    private static ResolvedPredicate[] buildPredicates() {
        return new ResolvedPredicate[] {
                // long+long same-column range
                new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                        new ResolvedPredicate.LongPredicate(0, Operator.LT, 9_999L))),
                // long+long same-column range, different op pair
                new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.LongPredicate(0, Operator.GT, -1L),
                        new ResolvedPredicate.LongPredicate(0, Operator.LT_EQ, 8_192L))),
                // double+double same-column range
                new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.DoublePredicate(1, Operator.GT_EQ, 0.0),
                        new ResolvedPredicate.DoublePredicate(1, Operator.LT, 1_000.0))),
                // int+int same-column range
                new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.IntPredicate(2, Operator.GT_EQ, 0),
                        new ResolvedPredicate.IntPredicate(2, Operator.LT, 100))),
                // int+int same-column range, different op pair
                new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.IntPredicate(2, Operator.GT, -1),
                        new ResolvedPredicate.IntPredicate(2, Operator.NOT_EQ, 50))),
                // long + double, different columns
                new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                        new ResolvedPredicate.DoublePredicate(1, Operator.LT, 999.0))),
                // long + double, different columns, different op pair
                new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.LongPredicate(0, Operator.NOT_EQ, -1L),
                        new ResolvedPredicate.DoublePredicate(1, Operator.LT_EQ, 1_000.0))),
                // double + long, different columns (canonical-swap into long+double)
                new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.DoublePredicate(1, Operator.GT, -1.0),
                        new ResolvedPredicate.LongPredicate(0, Operator.LT, 9_999L))),
        };
    }

    /// Constructs a generic-shape arity-2 AND matcher: each child is compiled
    /// individually, then they are combined through a single `and2(a, b)`
    /// helper lambda. The fusion path is bypassed by never letting the
    /// compiler see the [ResolvedPredicate.And] envelope.
    ///
    /// Every call to `and2(...)` returns a lambda from the same source
    /// location, so all such matchers share the same synthetic class and
    /// the same `a.test()` / `b.test()` bytecode locations — the very
    /// shape that goes megamorphic when many leaf-class combinations flow
    /// through the JVM.
    private static RowMatcher compileGenericAnd2(ResolvedPredicate p, FileSchema schema) {
        if (!(p instanceof ResolvedPredicate.And and) || and.children().size() != 2) {
            throw new IllegalArgumentException("expected arity-2 AND, got " + p);
        }
        RowMatcher a = RecordFilterCompiler.compile(and.children().get(0), schema);
        RowMatcher b = RecordFilterCompiler.compile(and.children().get(1), schema);
        return and2(a, b);
    }

    private static RowMatcher and2(RowMatcher a, RowMatcher b) {
        return row -> a.test(row) && b.test(row);
    }

    // ==================== Schema and row stubs ====================

    private static FileSchema buildSchema() {
        SchemaElement root = new SchemaElement("root", null, null, null, 4, null, null, null, null, null);
        SchemaElement id = new SchemaElement("id", PhysicalType.INT64, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement value = new SchemaElement("value", PhysicalType.DOUBLE, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement tag = new SchemaElement("tag", PhysicalType.INT32, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement flag = new SchemaElement("flag", PhysicalType.BOOLEAN, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, id, value, tag, flag));
    }

    private static StructAccessor[] buildRows(int n, long seed) {
        StructAccessor[] out = new StructAccessor[n];
        Random rng = new Random(seed);
        for (int i = 0; i < n; i++) {
            out[i] = new FlatStub(i, rng.nextDouble() * 1000.0, rng.nextInt(100), rng.nextBoolean());
        }
        return out;
    }

    /// Minimal row stub. Single concrete class — keeps `getLong` / `getDouble`
    /// / `getInt` call sites monomorphic, so any megamorphism in the
    /// benchmark numbers comes from the matcher dispatch, not the row.
    private static final class FlatStub implements StructAccessor {
        private final long idVal;
        private final double valueVal;
        private final int tagVal;
        private final boolean flagVal;

        FlatStub(long idVal, double valueVal, int tagVal, boolean flagVal) {
            this.idVal = idVal;
            this.valueVal = valueVal;
            this.tagVal = tagVal;
            this.flagVal = flagVal;
        }

        @Override public boolean isNull(String name) { return false; }
        @Override public long getLong(String name) { return idVal; }
        @Override public double getDouble(String name) { return valueVal; }
        @Override public int getInt(String name) { return tagVal; }
        @Override public boolean getBoolean(String name) { return flagVal; }

        @Override public float getFloat(String name) { throw new UnsupportedOperationException(); }
        @Override public String getString(String name) { throw new UnsupportedOperationException(); }
        @Override public byte[] getBinary(String name) { throw new UnsupportedOperationException(); }
        @Override public LocalDate getDate(String name) { throw new UnsupportedOperationException(); }
        @Override public LocalTime getTime(String name) { throw new UnsupportedOperationException(); }
        @Override public Instant getTimestamp(String name) { throw new UnsupportedOperationException(); }
        @Override public BigDecimal getDecimal(String name) { throw new UnsupportedOperationException(); }
        @Override public UUID getUuid(String name) { throw new UnsupportedOperationException(); }
        @Override public PqInterval getInterval(String name) { throw new UnsupportedOperationException(); }
        @Override public PqStruct getStruct(String name) { throw new UnsupportedOperationException(); }
        @Override public PqIntList getListOfInts(String name) { throw new UnsupportedOperationException(); }
        @Override public PqLongList getListOfLongs(String name) { throw new UnsupportedOperationException(); }
        @Override public PqDoubleList getListOfDoubles(String name) { throw new UnsupportedOperationException(); }
        @Override public PqList getList(String name) { throw new UnsupportedOperationException(); }
        @Override public PqMap getMap(String name) { throw new UnsupportedOperationException(); }
        @Override public PqVariant getVariant(String name) { throw new UnsupportedOperationException(); }
        @Override public Object getValue(String name) { throw new UnsupportedOperationException(); }
        @Override public int getFieldCount() { return 4; }
        @Override public String getFieldName(int index) {
            return switch (index) {
                case 0 -> "id";
                case 1 -> "value";
                case 2 -> "tag";
                case 3 -> "flag";
                default -> throw new IndexOutOfBoundsException(index);
            };
        }
    }
}
