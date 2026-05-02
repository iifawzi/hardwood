/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
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
import dev.hardwood.internal.predicate.RecordFilterEvaluator;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowMatcher;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Measures predicate-evaluation cost under a deliberate inline-cache
/// pollution. Twelve distinct arity-2 predicate shapes are compiled and
/// then iterated row-by-row in the same hot loop, so the inner virtual
/// call site at `matcher.test(row)` accumulates many receiver types and
/// goes megamorphic.
///
/// Run twice for the two-way comparison documented in
/// `_designs/RECORD_FILTER_FUSION_BYTECODE.md`:
///
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
///
/// # BC fusion enabled (default)
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar \
///   RecordFilterMegamorphicBenchmark -rf json -rff fused.json
///
/// # No fusion (Stage 1–3 fixed-arity matchers, deliberately megamorphic)
/// java -Dhardwood.recordfilter.fusion=false \
///   -jar performance-testing/micro-benchmarks/target/benchmarks.jar \
///   RecordFilterMegamorphicBenchmark -rf json -rff generic.json
/// ```
///
/// The `legacyMegamorphic` arm is independent of the system property —
/// it always evaluates through [RecordFilterEvaluator#matchesRow] and
/// reflects pre-Stage-1 behaviour.
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
    static final int SHAPE_COUNT = 12;

    private FileSchema schema;
    private ProjectedSchema projection;
    private RowReader[] rows;
    private ResolvedPredicate[] predicates;
    private RowMatcher[] compiledIndexed;

    @Setup
    public void setup() {
        schema = buildSchema();
        projection = ProjectedSchema.create(schema, ColumnProjection.all());
        rows = buildRows(BATCH_SIZE, 42L);
        predicates = buildShapes();
        compiledIndexed = new RowMatcher[predicates.length];
        for (int i = 0; i < predicates.length; i++) {
            compiledIndexed[i] = RecordFilterCompiler.compile(predicates[i], schema, projection::toProjectedIndex);
        }
        if (predicates.length != SHAPE_COUNT) {
            throw new IllegalStateException("expected " + SHAPE_COUNT + " shapes, got " + predicates.length);
        }
    }

    /// Per-row interleave: the inner call site `ms[s].test(row)` is hit
    /// `BATCH_SIZE * SHAPE_COUNT` times with `s` varying every iteration,
    /// so the JIT can't unroll the predicate index away and the bytecode
    /// location accumulates all 12 receiver types. That is what drives
    /// the inline-cache megamorphic on the outer site.
    @Benchmark
    public void legacyMegamorphic(Blackhole bh) {
        ResolvedPredicate[] ps = predicates;
        FileSchema s = schema;
        RowReader[] batch = rows;
        int ns = ps.length;
        for (int r = 0; r < batch.length; r++) {
            RowReader row = batch[r];
            for (int p = 0; p < ns; p++) {
                bh.consume(RecordFilterEvaluator.matchesRow(ps[p], row, s));
            }
        }
    }

    @Benchmark
    public void compiledMegamorphic(Blackhole bh) {
        RowMatcher[] ms = compiledIndexed;
        RowReader[] batch = rows;
        int ns = ms.length;
        for (int r = 0; r < batch.length; r++) {
            RowReader row = batch[r];
            for (int p = 0; p < ns; p++) {
                bh.consume(ms[p].test(row));
            }
        }
    }

    // ==================== Fixtures ====================

    private static FileSchema buildSchema() {
        SchemaElement root = new SchemaElement("root", null, null, null, 5, null, null, null, null, null);
        SchemaElement id = new SchemaElement("id", PhysicalType.INT64, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement value = new SchemaElement("value", PhysicalType.DOUBLE, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement tag = new SchemaElement("tag", PhysicalType.INT32, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement flag = new SchemaElement("flag", PhysicalType.BOOLEAN, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement bin = new SchemaElement("bin", PhysicalType.BYTE_ARRAY, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, id, value, tag, flag, bin));
    }

    private static RowReader[] buildRows(int n, long seed) {
        RowReader[] out = new RowReader[n];
        Random rng = new Random(seed);
        byte[][] bins = sampleBinaries(rng, 16);
        for (int i = 0; i < n; i++) {
            long idVal = rng.nextLong(10_000_000L);
            double valueVal = rng.nextDouble() * 1000.0;
            int tagVal = rng.nextInt(100);
            boolean flagVal = rng.nextBoolean();
            byte[] binVal = bins[rng.nextInt(bins.length)];
            out[i] = new FlatStub(idVal, valueVal, tagVal, flagVal, binVal);
        }
        return out;
    }

    private static byte[][] sampleBinaries(Random rng, int n) {
        byte[][] out = new byte[n][];
        for (int i = 0; i < n; i++) {
            out[i] = String.format("k%03d", rng.nextInt(1000)).getBytes(StandardCharsets.UTF_8);
        }
        return out;
    }

    /// 12 shapes covering the eligibility matrix in
    /// `_designs/RECORD_FILTER_FUSION_BYTECODE.md`. Column indices: 0=id,
    /// 1=value, 2=tag, 3=flag, 4=bin.
    private static ResolvedPredicate[] buildShapes() {
        List<ResolvedPredicate> out = new ArrayList<>();
        // id BETWEEN 1M and 4M  — long+long same-col AND
        out.add(new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 1_000_000L),
                new ResolvedPredicate.LongPredicate(0, Operator.LT, 4_000_000L))));
        // tag BETWEEN 0 and 50  — int+int same-col AND
        out.add(new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.IntPredicate(2, Operator.GT_EQ, 0),
                new ResolvedPredicate.IntPredicate(2, Operator.LT, 50))));
        // value BETWEEN 0 and 500  — double+double same-col AND
        out.add(new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.DoublePredicate(1, Operator.GT_EQ, 0.0),
                new ResolvedPredicate.DoublePredicate(1, Operator.LT, 500.0))));
        // id < 500K OR id > 9.5M  — long+long same-col OR
        out.add(new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.LT, 500_000L),
                new ResolvedPredicate.LongPredicate(0, Operator.GT, 9_500_000L))));
        // tag = 5 OR tag = 47  — int+int same-col OR
        out.add(new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.IntPredicate(2, Operator.EQ, 5),
                new ResolvedPredicate.IntPredicate(2, Operator.EQ, 47))));
        // id < 5M AND value < 500  — long+double diff-col AND
        out.add(new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.LT, 5_000_000L),
                new ResolvedPredicate.DoublePredicate(1, Operator.LT, 500.0))));
        // tag < 50 AND value < 500  — int+double diff-col AND
        out.add(new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.IntPredicate(2, Operator.LT, 50),
                new ResolvedPredicate.DoublePredicate(1, Operator.LT, 500.0))));
        // id < 5M AND tag > 5  — int+long diff-col AND (canonical-swap)
        out.add(new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.LT, 5_000_000L),
                new ResolvedPredicate.IntPredicate(2, Operator.GT, 5))));
        // id >= 5M OR value > 500  — long+double diff-col OR
        out.add(new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 5_000_000L),
                new ResolvedPredicate.DoublePredicate(1, Operator.GT, 500.0))));
        // tag <= 50 OR id > 5M  — int+long diff-col OR
        out.add(new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.IntPredicate(2, Operator.LT_EQ, 50),
                new ResolvedPredicate.LongPredicate(0, Operator.GT, 5_000_000L))));
        // flag == true AND flag != false (degenerate) — boolean+boolean diff-col AND
        // Need different boolean columns; we only have 'flag'. Use a workaround:
        // emulate with `flag == true AND flag != false` on the same col — falls
        // out of the diff-col fusion path and exercises the same-col fallback
        // (which the lambda matrix would have accepted but BC does not).
        // For the megamorphic benchmark, what matters is that the matcher
        // class is unique per shape and the inner call site sees it; the
        // fallback And2Matcher provides that.
        out.add(new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.BooleanPredicate(3, Operator.EQ, true),
                new ResolvedPredicate.BooleanPredicate(3, Operator.NOT_EQ, false))));
        // bin BETWEEN k200 and k800  — binary+binary same-col AND
        out.add(new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.BinaryPredicate(4, Operator.GT_EQ,
                        "k200".getBytes(StandardCharsets.UTF_8), false),
                new ResolvedPredicate.BinaryPredicate(4, Operator.LT,
                        "k800".getBytes(StandardCharsets.UTF_8), false))));
        return out.toArray(new ResolvedPredicate[0]);
    }

    /// Five-field RowReader stub. Only the accessors hit by the benchmark
    /// shapes are implemented — everything else throws and the call sites
    /// stay monomorphic.
    private static final class FlatStub implements RowReader {
        private final long idVal;
        private final double valueVal;
        private final int tagVal;
        private final boolean flagVal;
        private final byte[] binVal;

        FlatStub(long idVal, double valueVal, int tagVal, boolean flagVal, byte[] binVal) {
            this.idVal = idVal;
            this.valueVal = valueVal;
            this.tagVal = tagVal;
            this.flagVal = flagVal;
            this.binVal = binVal;
        }

        @Override public boolean isNull(String name) { return false; }
        @Override public boolean isNull(int idx) { return false; }
        @Override public long getLong(String name) { return idVal; }
        @Override public long getLong(int idx) { return idVal; }
        @Override public double getDouble(String name) { return valueVal; }
        @Override public double getDouble(int idx) { return valueVal; }
        @Override public int getInt(String name) { return tagVal; }
        @Override public int getInt(int idx) { return tagVal; }
        @Override public boolean getBoolean(String name) { return flagVal; }
        @Override public boolean getBoolean(int idx) { return flagVal; }
        @Override public byte[] getBinary(String name) { return binVal; }
        @Override public byte[] getBinary(int idx) { return binVal; }

        @Override public boolean hasNext() { throw new UnsupportedOperationException(); }
        @Override public void next() { throw new UnsupportedOperationException(); }
        @Override public void close() { }

        @Override public float getFloat(String name) { throw new UnsupportedOperationException(); }
        @Override public float getFloat(int idx) { throw new UnsupportedOperationException(); }
        @Override public String getString(String name) { throw new UnsupportedOperationException(); }
        @Override public String getString(int idx) { throw new UnsupportedOperationException(); }
        @Override public LocalDate getDate(String name) { throw new UnsupportedOperationException(); }
        @Override public LocalDate getDate(int idx) { throw new UnsupportedOperationException(); }
        @Override public LocalTime getTime(String name) { throw new UnsupportedOperationException(); }
        @Override public LocalTime getTime(int idx) { throw new UnsupportedOperationException(); }
        @Override public Instant getTimestamp(String name) { throw new UnsupportedOperationException(); }
        @Override public Instant getTimestamp(int idx) { throw new UnsupportedOperationException(); }
        @Override public BigDecimal getDecimal(String name) { throw new UnsupportedOperationException(); }
        @Override public BigDecimal getDecimal(int idx) { throw new UnsupportedOperationException(); }
        @Override public UUID getUuid(String name) { throw new UnsupportedOperationException(); }
        @Override public UUID getUuid(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqInterval getInterval(String name) { throw new UnsupportedOperationException(); }
        @Override public PqInterval getInterval(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqStruct getStruct(String name) { throw new UnsupportedOperationException(); }
        @Override public PqStruct getStruct(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqIntList getListOfInts(String name) { throw new UnsupportedOperationException(); }
        @Override public PqIntList getListOfInts(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqLongList getListOfLongs(String name) { throw new UnsupportedOperationException(); }
        @Override public PqLongList getListOfLongs(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqDoubleList getListOfDoubles(String name) { throw new UnsupportedOperationException(); }
        @Override public PqDoubleList getListOfDoubles(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqList getList(String name) { throw new UnsupportedOperationException(); }
        @Override public PqList getList(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqMap getMap(String name) { throw new UnsupportedOperationException(); }
        @Override public PqMap getMap(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqVariant getVariant(String name) { throw new UnsupportedOperationException(); }
        @Override public Object getValue(String name) { throw new UnsupportedOperationException(); }
        @Override public Object getValue(int idx) { throw new UnsupportedOperationException(); }
        @Override public int getFieldCount() { return 5; }
        @Override public String getFieldName(int index) {
            return switch (index) {
                case 0 -> "id";
                case 1 -> "value";
                case 2 -> "tag";
                case 3 -> "flag";
                case 4 -> "bin";
                default -> throw new IndexOutOfBoundsException(index);
            };
        }
    }
}
