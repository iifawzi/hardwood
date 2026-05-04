/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

import static org.assertj.core.api.Assertions.assertThat;

/// Equivalence and dispatch-class checks for the build-time fusion path.
///
/// For every supported tuple in the matrix the test confirms that
/// (1) the indexed-compiled and name-keyed-compiled matchers agree with
/// the hand-computed expected value on representative rows,
/// (2) the indexed dispatch produces the expected `Fused*` class so we
/// know the registry actually fired (and not a fallback), and
/// (3) commutative reordering of the two leaves resolves to the same
/// fused class. Off-matrix shapes (e.g. `NOT_EQ AND NOT_EQ`) keep falling
/// back to the fixed-arity matchers.
class RecordFilterFusionTest {

    // ==================== Long matrix ====================

    @ParameterizedTest(name = "long {0} → {1}")
    @MethodSource("longMatrix")
    void longTuple(LongCase c, String expectedFusedSimpleName) {
        FileSchema schema = oneLongSchema();
        ProjectedSchema projection = projectAll(schema);
        ResolvedPredicate p = c.predicate();
        // Class identity — only the indexed path is registry-routed today; name-keyed
        // compile flows through the existing leaf factories. Both must still be correct.
        assertFusedClass(p, schema, projection, expectedFusedSimpleName);
        // Reversed-order leaves resolve to the same fused class (commutativity).
        assertFusedClass(c.reversedPredicate(), schema, projection, expectedFusedSimpleName);
        // Functional equivalence on a value that satisfies the predicate.
        assertEquivalent(p, longRow(c.satisfying), schema, projection, true);
        assertEquivalent(c.reversedPredicate(), longRow(c.satisfying), schema, projection, true);
        // And on a value that does not.
        assertEquivalent(p, longRow(c.notSatisfying), schema, projection, false);
        // Null column always evaluates to false.
        assertEquivalent(p, nullLongRow(), schema, projection, false);
    }

    static Stream<Arguments> longMatrix() {
        return Stream.of(
                Arguments.of(new LongCase(Operator.GT_EQ, 10L, Operator.LT_EQ, 20L, true, 15L, 25L), "FusedLongAndCsCs_GteLte"),
                Arguments.of(new LongCase(Operator.GT, 10L, Operator.LT, 20L, true, 15L, 10L), "FusedLongAndCsCs_GtLt"),
                Arguments.of(new LongCase(Operator.GT_EQ, 10L, Operator.LT, 20L, true, 10L, 20L), "FusedLongAndCsCs_GteLt"),
                Arguments.of(new LongCase(Operator.GT, 10L, Operator.LT_EQ, 20L, true, 20L, 10L), "FusedLongAndCsCs_GtLte"),
                Arguments.of(new LongCase(Operator.LT, 10L, Operator.GT, 20L, false, 5L, 15L), "FusedLongOrCsCs_LtGt"),
                Arguments.of(new LongCase(Operator.LT_EQ, 10L, Operator.GT_EQ, 20L, false, 10L, 15L), "FusedLongOrCsCs_LteGte"),
                Arguments.of(new LongCase(Operator.EQ, 7L, Operator.EQ, 11L, false, 7L, 8L), "FusedLongOrCsCs_EqEq"));
    }

    // ==================== Int matrix ====================

    @ParameterizedTest(name = "int {0} → {1}")
    @MethodSource("intMatrix")
    void intTuple(IntCase c, String expectedFusedSimpleName) {
        FileSchema schema = oneIntSchema();
        ProjectedSchema projection = projectAll(schema);
        ResolvedPredicate p = c.predicate();
        assertFusedClass(p, schema, projection, expectedFusedSimpleName);
        assertFusedClass(c.reversedPredicate(), schema, projection, expectedFusedSimpleName);
        assertEquivalent(p, intRow(c.satisfying), schema, projection, true);
        assertEquivalent(p, intRow(c.notSatisfying), schema, projection, false);
        assertEquivalent(p, nullIntRow(), schema, projection, false);
    }

    static Stream<Arguments> intMatrix() {
        return Stream.of(
                Arguments.of(new IntCase(Operator.GT_EQ, 10, Operator.LT_EQ, 20, true, 15, 25), "FusedIntAndCsCs_GteLte"),
                Arguments.of(new IntCase(Operator.GT, 10, Operator.LT, 20, true, 15, 10), "FusedIntAndCsCs_GtLt"),
                Arguments.of(new IntCase(Operator.GT_EQ, 10, Operator.LT, 20, true, 10, 20), "FusedIntAndCsCs_GteLt"),
                Arguments.of(new IntCase(Operator.GT, 10, Operator.LT_EQ, 20, true, 20, 10), "FusedIntAndCsCs_GtLte"),
                Arguments.of(new IntCase(Operator.LT, 10, Operator.GT, 20, false, 5, 15), "FusedIntOrCsCs_LtGt"),
                Arguments.of(new IntCase(Operator.LT_EQ, 10, Operator.GT_EQ, 20, false, 10, 15), "FusedIntOrCsCs_LteGte"),
                Arguments.of(new IntCase(Operator.EQ, 7, Operator.EQ, 11, false, 7, 8), "FusedIntOrCsCs_EqEq"));
    }

    // ==================== Double matrix ====================

    @ParameterizedTest(name = "double {0} → {1}")
    @MethodSource("doubleMatrix")
    void doubleTuple(DoubleCase c, String expectedFusedSimpleName) {
        FileSchema schema = oneDoubleSchema();
        ProjectedSchema projection = projectAll(schema);
        ResolvedPredicate p = c.predicate();
        assertFusedClass(p, schema, projection, expectedFusedSimpleName);
        assertFusedClass(c.reversedPredicate(), schema, projection, expectedFusedSimpleName);
        assertEquivalent(p, doubleRow(c.satisfying), schema, projection, true);
        assertEquivalent(p, doubleRow(c.notSatisfying), schema, projection, false);
        assertEquivalent(p, nullDoubleRow(), schema, projection, false);
        // NaN — Double.compare puts NaN above +Infinity. For any AND interval the
        // upper bound fails and the row is rejected; for any OR outside-interval
        // the upper-bound branch succeeds and the row is accepted.
        assertEquivalent(p, doubleRow(Double.NaN), schema, projection, !c.isAnd);
    }

    static Stream<Arguments> doubleMatrix() {
        return Stream.of(
                Arguments.of(new DoubleCase(Operator.GT_EQ, 1.0, Operator.LT_EQ, 2.0, true, 1.5, 2.5), "FusedDoubleAndCsCs_GteLte"),
                Arguments.of(new DoubleCase(Operator.GT, 1.0, Operator.LT, 2.0, true, 1.5, 1.0), "FusedDoubleAndCsCs_GtLt"),
                Arguments.of(new DoubleCase(Operator.GT_EQ, 1.0, Operator.LT, 2.0, true, 1.0, 2.0), "FusedDoubleAndCsCs_GteLt"),
                Arguments.of(new DoubleCase(Operator.GT, 1.0, Operator.LT_EQ, 2.0, true, 2.0, 1.0), "FusedDoubleAndCsCs_GtLte"),
                Arguments.of(new DoubleCase(Operator.LT, 1.0, Operator.GT, 2.0, false, 0.5, 1.5), "FusedDoubleOrCsCs_LtGt"),
                Arguments.of(new DoubleCase(Operator.LT_EQ, 1.0, Operator.GT_EQ, 2.0, false, 1.0, 1.5), "FusedDoubleOrCsCs_LteGte"));
    }

    // ==================== Off-matrix fallbacks ====================

    @Test
    void offMatrixTupleFallsBackToAnd2() {
        FileSchema schema = oneLongSchema();
        ProjectedSchema projection = projectAll(schema);
        // (NOT_EQ, NOT_EQ) — not in the build-time matrix.
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.NOT_EQ, 5L),
                new ResolvedPredicate.LongPredicate(0, Operator.NOT_EQ, 7L)));
        RowMatcher m = RecordFilterCompiler.compile(p, schema, projection::toProjectedIndex);
        assertThat(m.getClass().getSimpleName()).startsWith("And2Matcher");
        assertEquivalent(p, longRow(3L), schema, projection, true);
        assertEquivalent(p, longRow(5L), schema, projection, false);
    }

    @Test
    void diffColumnAndFallsBackToAnd2() {
        // Two-column schema; predicate references both columns. Diff-column shapes
        // are uncovered by the build-time matrix even when types match.
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c0 = new SchemaElement("a", PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement("b", PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, c0, c1));
        ProjectedSchema projection = projectAll(schema);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                new ResolvedPredicate.LongPredicate(1, Operator.LT, 100L)));
        RowMatcher m = RecordFilterCompiler.compile(p, schema, projection::toProjectedIndex);
        assertThat(m.getClass().getSimpleName()).startsWith("And2Matcher");
    }

    // ==================== Helpers ====================

    private static void assertEquivalent(ResolvedPredicate predicate, RowReader row,
            FileSchema schema, ProjectedSchema projection, boolean expected) {
        boolean compiledName = RecordFilterCompiler.compile(predicate, schema).test(row);
        boolean compiledIndexed = RecordFilterCompiler.compile(predicate, schema, projection::toProjectedIndex).test(row);
        assertThat(compiledName).as("compiled-name vs expected for %s", predicate).isEqualTo(expected);
        assertThat(compiledIndexed).as("compiled-indexed vs expected for %s", predicate).isEqualTo(expected);
    }

    private static void assertFusedClass(ResolvedPredicate predicate, FileSchema schema,
            ProjectedSchema projection, String expectedSimpleName) {
        RowMatcher m = RecordFilterCompiler.compile(predicate, schema, projection::toProjectedIndex);
        assertThat(m.getClass().getSimpleName())
                .as("indexed dispatch for %s should produce %s, got %s",
                        predicate, expectedSimpleName, m.getClass().getName())
                .isEqualTo(expectedSimpleName);
    }

    private static ProjectedSchema projectAll(FileSchema schema) {
        return ProjectedSchema.create(schema, ColumnProjection.all());
    }

    private static FileSchema oneLongSchema() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement c = new SchemaElement("a", PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c));
    }

    private static FileSchema oneIntSchema() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement c = new SchemaElement("a", PhysicalType.INT32, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c));
    }

    private static FileSchema oneDoubleSchema() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement c = new SchemaElement("a", PhysicalType.DOUBLE, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c));
    }

    private static StubRow longRow(long v) { return new StubRow().withLong(v, false); }
    private static StubRow nullLongRow() { return new StubRow().withLong(0L, true); }
    private static StubRow intRow(int v) { return new StubRow().withInt(v, false); }
    private static StubRow nullIntRow() { return new StubRow().withInt(0, true); }
    private static StubRow doubleRow(double v) { return new StubRow().withDouble(v, false); }
    private static StubRow nullDoubleRow() { return new StubRow().withDouble(0.0, true); }

    // ==================== Test cases ====================

    record LongCase(Operator opA, long va, Operator opB, long vb, boolean isAnd,
            long satisfying, long notSatisfying) {
        ResolvedPredicate predicate() {
            return compound(isAnd,
                    new ResolvedPredicate.LongPredicate(0, opA, va),
                    new ResolvedPredicate.LongPredicate(0, opB, vb));
        }

        ResolvedPredicate reversedPredicate() {
            return compound(isAnd,
                    new ResolvedPredicate.LongPredicate(0, opB, vb),
                    new ResolvedPredicate.LongPredicate(0, opA, va));
        }
    }

    record IntCase(Operator opA, int va, Operator opB, int vb, boolean isAnd,
            int satisfying, int notSatisfying) {
        ResolvedPredicate predicate() {
            return compound(isAnd,
                    new ResolvedPredicate.IntPredicate(0, opA, va),
                    new ResolvedPredicate.IntPredicate(0, opB, vb));
        }

        ResolvedPredicate reversedPredicate() {
            return compound(isAnd,
                    new ResolvedPredicate.IntPredicate(0, opB, vb),
                    new ResolvedPredicate.IntPredicate(0, opA, va));
        }
    }

    record DoubleCase(Operator opA, double va, Operator opB, double vb, boolean isAnd,
            double satisfying, double notSatisfying) {
        ResolvedPredicate predicate() {
            return compound(isAnd,
                    new ResolvedPredicate.DoublePredicate(0, opA, va),
                    new ResolvedPredicate.DoublePredicate(0, opB, vb));
        }

        ResolvedPredicate reversedPredicate() {
            return compound(isAnd,
                    new ResolvedPredicate.DoublePredicate(0, opB, vb),
                    new ResolvedPredicate.DoublePredicate(0, opA, va));
        }
    }

    private static ResolvedPredicate compound(boolean isAnd,
            ResolvedPredicate a, ResolvedPredicate b) {
        return isAnd
                ? new ResolvedPredicate.And(List.of(a, b))
                : new ResolvedPredicate.Or(List.of(a, b));
    }

    // ==================== Stub row ====================

    private static final class StubRow implements RowReader {
        private long longValue;
        private int intValue;
        private double doubleValue;
        private boolean isNull;

        StubRow withLong(long v, boolean isNull) {
            this.longValue = v;
            this.isNull = isNull;
            return this;
        }

        StubRow withInt(int v, boolean isNull) {
            this.intValue = v;
            this.isNull = isNull;
            return this;
        }

        StubRow withDouble(double v, boolean isNull) {
            this.doubleValue = v;
            this.isNull = isNull;
            return this;
        }

        @Override public boolean isNull(String name) { return isNull; }
        @Override public boolean isNull(int idx) { return isNull; }

        @Override public long getLong(String name) { return longValue; }
        @Override public long getLong(int idx) { return longValue; }
        @Override public int getInt(String name) { return intValue; }
        @Override public int getInt(int idx) { return intValue; }
        @Override public double getDouble(String name) { return doubleValue; }
        @Override public double getDouble(int idx) { return doubleValue; }

        @Override public boolean hasNext() { throw new UnsupportedOperationException(); }
        @Override public void next() { throw new UnsupportedOperationException(); }
        @Override public void close() { }
        @Override public int getFieldCount() { return 1; }
        @Override public String getFieldName(int index) { return "a"; }

        @Override public float getFloat(String name) { throw new UnsupportedOperationException(); }
        @Override public boolean getBoolean(String name) { throw new UnsupportedOperationException(); }
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

        @Override public float getFloat(int idx) { throw new UnsupportedOperationException(); }
        @Override public boolean getBoolean(int idx) { throw new UnsupportedOperationException(); }
        @Override public String getString(int idx) { throw new UnsupportedOperationException(); }
        @Override public byte[] getBinary(int idx) { throw new UnsupportedOperationException(); }
        @Override public LocalDate getDate(int idx) { throw new UnsupportedOperationException(); }
        @Override public LocalTime getTime(int idx) { throw new UnsupportedOperationException(); }
        @Override public Instant getTimestamp(int idx) { throw new UnsupportedOperationException(); }
        @Override public BigDecimal getDecimal(int idx) { throw new UnsupportedOperationException(); }
        @Override public UUID getUuid(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqInterval getInterval(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqStruct getStruct(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqIntList getListOfInts(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqLongList getListOfLongs(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqDoubleList getListOfDoubles(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqList getList(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqMap getMap(int idx) { throw new UnsupportedOperationException(); }
        @Override public Object getValue(int idx) { throw new UnsupportedOperationException(); }
    }
}
