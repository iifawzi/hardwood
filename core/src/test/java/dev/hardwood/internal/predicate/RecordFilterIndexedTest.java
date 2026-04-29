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

import dev.hardwood.internal.reader.IndexedAccessor;
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
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Equivalence tests for the indexed compile path. For every predicate
/// shape the indexed matcher (built via
/// `RecordFilterCompiler.compile(predicate, schema, projection)`) must
/// agree with the name-based matcher and with the legacy
/// [RecordFilterEvaluator] oracle.
///
/// Rows here implement both [StructAccessor] (for name-based access) and
/// [IndexedAccessor] (for the projected-index path) so the same row can
/// flow through both matchers.
class RecordFilterIndexedTest {

    // ==================== Single leaves ====================

    @ParameterizedTest(name = "long {0} {1} on a={2} → {3}")
    @MethodSource("singleLongCases")
    void singleLong(Operator op, long v, long aVal, boolean expected) {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoLongIndexedRow row = new TwoLongIndexedRow("a", aVal, false, "b", 0L, false);
        ResolvedPredicate p = new ResolvedPredicate.LongPredicate(0, op, v);
        assertEquivalent(p, row, schema, projection, expected);
    }

    static Stream<Arguments> singleLongCases() {
        return Stream.of(
                Arguments.of(Operator.EQ, 50L, 50L, true),
                Arguments.of(Operator.EQ, 50L, 51L, false),
                Arguments.of(Operator.NOT_EQ, 50L, 51L, true),
                Arguments.of(Operator.LT, 50L, 49L, true),
                Arguments.of(Operator.LT_EQ, 50L, 50L, true),
                Arguments.of(Operator.GT, 50L, 51L, true),
                Arguments.of(Operator.GT_EQ, 50L, 50L, true));
    }

    @ParameterizedTest(name = "int {0} {1} on a={2} → {3}")
    @MethodSource("singleIntCases")
    void singleInt(Operator op, int v, int aVal, boolean expected) {
        FileSchema schema = twoIntSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoIntIndexedRow row = new TwoIntIndexedRow("a", aVal, false, "b", 0, false);
        ResolvedPredicate p = new ResolvedPredicate.IntPredicate(0, op, v);
        assertEquivalent(p, row, schema, projection, expected);
    }

    static Stream<Arguments> singleIntCases() {
        return Stream.of(
                Arguments.of(Operator.EQ, 50, 50, true),
                Arguments.of(Operator.LT, 50, 49, true),
                Arguments.of(Operator.GT_EQ, 50, 50, true));
    }

    @ParameterizedTest(name = "double {0} {1} on a={2} → {3}")
    @MethodSource("singleDoubleCases")
    void singleDouble(Operator op, double v, double aVal, boolean expected) {
        FileSchema schema = twoDoubleSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoDoubleIndexedRow row = new TwoDoubleIndexedRow("a", aVal, false, "b", 0.0, false);
        ResolvedPredicate p = new ResolvedPredicate.DoublePredicate(0, op, v);
        assertEquivalent(p, row, schema, projection, expected);
    }

    static Stream<Arguments> singleDoubleCases() {
        return Stream.of(
                Arguments.of(Operator.EQ, 1.5, 1.5, true),
                Arguments.of(Operator.LT, 1.5, 1.4, true),
                Arguments.of(Operator.GT, 1.5, 1.6, true),
                // NaN ordering — Double.compare semantics: NaN is greater than +Inf.
                Arguments.of(Operator.GT, 100.0, Double.NaN, true),
                Arguments.of(Operator.LT, Double.NaN, 1.0, true));
    }

    @Test
    void singleLeafNullColumnRejects() {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoLongIndexedRow row = new TwoLongIndexedRow("a", 0L, true, "b", 0L, false);
        for (Operator op : Operator.values()) {
            assertEquivalent(new ResolvedPredicate.LongPredicate(0, op, 0L), row, schema, projection, false);
        }
        assertEquivalent(new ResolvedPredicate.IsNullPredicate(0), row, schema, projection, true);
        assertEquivalent(new ResolvedPredicate.IsNotNullPredicate(0), row, schema, projection, false);
    }

    // ==================== Indexed fusion ====================

    @ParameterizedTest(name = "long {0} {1} AND long {2} {3} on (a={4}, b={5}) → {6}")
    @MethodSource("longLongFusionCases")
    void longLongFusion(Operator opA, long vA, Operator opB, long vB, long aVal, long bVal, boolean expected) {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoLongIndexedRow row = new TwoLongIndexedRow("a", aVal, false, "b", bVal, false);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, opA, vA),
                new ResolvedPredicate.LongPredicate(1, opB, vB)));
        assertEquivalent(p, row, schema, projection, expected);
    }

    static Stream<Arguments> longLongFusionCases() {
        return allOpPairs((opA, opB) -> Arguments.of(opA, 10L, opB, 100L, 12L, 100L,
                expected(opA, 12, 10) && expected(opB, 100, 100)));
    }

    @ParameterizedTest(name = "long range {0} {1} AND {2} {3} on a={4} → {5}")
    @MethodSource("longRangeFusionCases")
    void longRangeFusion(Operator opA, long vA, Operator opB, long vB, long aVal, boolean expected) {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoLongIndexedRow row = new TwoLongIndexedRow("a", aVal, false, "b", 0L, false);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, opA, vA),
                new ResolvedPredicate.LongPredicate(0, opB, vB)));
        assertEquivalent(p, row, schema, projection, expected);
    }

    static Stream<Arguments> longRangeFusionCases() {
        return allOpPairs((opA, opB) -> Arguments.of(opA, 10L, opB, 100L, 50L,
                expected(opA, 50, 10) && expected(opB, 50, 100)));
    }

    @ParameterizedTest(name = "long {0} {1} AND double {2} {3} on (a={4}, b={5}) → {6}")
    @MethodSource("longDoubleFusionCases")
    void longDoubleFusion(Operator opA, long vA, Operator opB, double vB, long aVal, double bVal, boolean expected) {
        FileSchema schema = longDoubleSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        LongDoubleIndexedRow row = new LongDoubleIndexedRow("a", aVal, false, "b", bVal, false);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, opA, vA),
                new ResolvedPredicate.DoublePredicate(1, opB, vB)));
        assertEquivalent(p, row, schema, projection, expected);
    }

    static Stream<Arguments> longDoubleFusionCases() {
        return allOpPairs((opA, opB) -> Arguments.of(opA, 10L, opB, 9.5, 12L, 9.5,
                expected(opA, 12, 10) && expectedD(opB, 9.5, 9.5)));
    }

    @Test
    void fusionWithNullsRejects() {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                new ResolvedPredicate.LongPredicate(1, Operator.LT, 1000L)));
        TwoLongIndexedRow nullA = new TwoLongIndexedRow("a", 5L, true, "b", 100L, false);
        TwoLongIndexedRow nullB = new TwoLongIndexedRow("a", 5L, false, "b", 100L, true);
        assertEquivalent(p, nullA, schema, projection, false);
        assertEquivalent(p, nullB, schema, projection, false);
    }

    // ==================== Compound (3-arity passes through indexed leaves) ====================

    @Test
    void threeArityAndUsesIndexedLeaves() {
        FileSchema schema = threeLongSchema("a", "b", "c");
        ProjectedSchema projection = projectAll(schema);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                new ResolvedPredicate.LongPredicate(1, Operator.LT, 1000L),
                new ResolvedPredicate.LongPredicate(2, Operator.NOT_EQ, -1L)));
        ThreeLongIndexedRow row = new ThreeLongIndexedRow(5L, 100L, 50L);
        assertEquivalent(p, row, schema, projection, true);
    }

    // ==================== Helpers ====================

    private static void assertEquivalent(ResolvedPredicate predicate, StructAccessor row, FileSchema schema,
            ProjectedSchema projection, boolean expected) {
        boolean legacy = RecordFilterEvaluator.matchesRow(predicate, row, schema);
        boolean compiledName = RecordFilterCompiler.compile(predicate, schema).test(row);
        boolean compiledIndexed = RecordFilterCompiler.compile(predicate, schema, projection).test(row);
        assertThat(compiledName).as("legacy/compiled-name disagreed for %s", predicate).isEqualTo(legacy);
        assertThat(compiledIndexed).as("legacy/compiled-indexed disagreed for %s", predicate).isEqualTo(legacy);
        assertThat(legacy).as("legacy oracle disagreed with expected for %s", predicate).isEqualTo(expected);
    }

    private static boolean expected(Operator op, long lhs, long rhs) {
        return switch (op) {
            case EQ -> lhs == rhs;
            case NOT_EQ -> lhs != rhs;
            case LT -> lhs < rhs;
            case LT_EQ -> lhs <= rhs;
            case GT -> lhs > rhs;
            case GT_EQ -> lhs >= rhs;
        };
    }

    private static boolean expectedD(Operator op, double lhs, double rhs) {
        return switch (op) {
            case EQ -> Double.compare(lhs, rhs) == 0;
            case NOT_EQ -> Double.compare(lhs, rhs) != 0;
            case LT -> Double.compare(lhs, rhs) < 0;
            case LT_EQ -> Double.compare(lhs, rhs) <= 0;
            case GT -> Double.compare(lhs, rhs) > 0;
            case GT_EQ -> Double.compare(lhs, rhs) >= 0;
        };
    }

    private static Stream<Arguments> allOpPairs(java.util.function.BiFunction<Operator, Operator, Arguments> b) {
        Stream.Builder<Arguments> out = Stream.builder();
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(b.apply(opA, opB));
            }
        }
        return out.build();
    }

    private static ProjectedSchema projectAll(FileSchema schema) {
        // Project every column in original order — the test rows are constructed
        // with the same column order, so projected index == file column index.
        return ProjectedSchema.create(schema, ColumnProjection.all());
    }

    private static FileSchema twoLongSchema(String n1, String n2) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(n1, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(n2, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2));
    }

    private static FileSchema twoIntSchema(String n1, String n2) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(n1, PhysicalType.INT32, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(n2, PhysicalType.INT32, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2));
    }

    private static FileSchema twoDoubleSchema(String n1, String n2) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(n1, PhysicalType.DOUBLE, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(n2, PhysicalType.DOUBLE, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2));
    }

    private static FileSchema longDoubleSchema(String n1, String n2) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(n1, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(n2, PhysicalType.DOUBLE, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2));
    }

    private static FileSchema threeLongSchema(String n1, String n2, String n3) {
        SchemaElement root = new SchemaElement("root", null, null, null, 3, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(n1, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(n2, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        SchemaElement c3 = new SchemaElement(n3, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2, c3));
    }

    // ==================== Stub rows ====================

    private static final class TwoLongIndexedRow extends BaseIndexedRow {
        private final long v0;
        private final long v1;
        TwoLongIndexedRow(String n0, long v0, boolean null0, String n1, long v1, boolean null1) {
            super(new String[] { n0, n1 }, new boolean[] { null0, null1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public long getLong(String name) { return getLongAt(indexOf(name)); }
        @Override public long getLongAt(int idx) { return idx == 0 ? v0 : v1; }
    }

    private static final class TwoIntIndexedRow extends BaseIndexedRow {
        private final int v0;
        private final int v1;
        TwoIntIndexedRow(String n0, int v0, boolean null0, String n1, int v1, boolean null1) {
            super(new String[] { n0, n1 }, new boolean[] { null0, null1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public int getInt(String name) { return getIntAt(indexOf(name)); }
        @Override public int getIntAt(int idx) { return idx == 0 ? v0 : v1; }
    }

    private static final class TwoDoubleIndexedRow extends BaseIndexedRow {
        private final double v0;
        private final double v1;
        TwoDoubleIndexedRow(String n0, double v0, boolean null0, String n1, double v1, boolean null1) {
            super(new String[] { n0, n1 }, new boolean[] { null0, null1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public double getDouble(String name) { return getDoubleAt(indexOf(name)); }
        @Override public double getDoubleAt(int idx) { return idx == 0 ? v0 : v1; }
    }

    private static final class LongDoubleIndexedRow extends BaseIndexedRow {
        private final long longVal;
        private final double dblVal;
        LongDoubleIndexedRow(String n0, long longVal, boolean null0, String n1, double dblVal, boolean null1) {
            super(new String[] { n0, n1 }, new boolean[] { null0, null1 });
            this.longVal = longVal;
            this.dblVal = dblVal;
        }
        @Override public long getLong(String name) { return getLongAt(indexOf(name)); }
        @Override public double getDouble(String name) { return getDoubleAt(indexOf(name)); }
        @Override public long getLongAt(int idx) { return longVal; }
        @Override public double getDoubleAt(int idx) { return dblVal; }
    }

    private static final class ThreeLongIndexedRow extends BaseIndexedRow {
        private final long v0;
        private final long v1;
        private final long v2;
        ThreeLongIndexedRow(long v0, long v1, long v2) {
            super(new String[] { "a", "b", "c" }, new boolean[] { false, false, false });
            this.v0 = v0;
            this.v1 = v1;
            this.v2 = v2;
        }
        @Override public long getLong(String name) { return getLongAt(indexOf(name)); }
        @Override public long getLongAt(int idx) {
            return switch (idx) {
                case 0 -> v0;
                case 1 -> v1;
                case 2 -> v2;
                default -> throw new IndexOutOfBoundsException(idx);
            };
        }
    }

    /// Common stub plumbing for both `StructAccessor` and `IndexedAccessor`.
    /// Subclasses override only the typed accessors they need; everything else
    /// throws `UnsupportedOperationException`, which makes test failures point
    /// at the predicate path that called the wrong accessor.
    private abstract static class BaseIndexedRow implements StructAccessor, IndexedAccessor {
        private final String[] names;
        private final boolean[] nulls;

        BaseIndexedRow(String[] names, boolean[] nulls) {
            this.names = names;
            this.nulls = nulls;
        }

        protected int indexOf(String name) {
            for (int i = 0; i < names.length; i++) {
                if (names[i].equals(name)) return i;
            }
            throw new IllegalArgumentException(name);
        }

        @Override public boolean isNull(String name) { return nulls[indexOf(name)]; }
        @Override public boolean isNullAt(int idx) { return nulls[idx]; }

        @Override public int getInt(String name) { throw new UnsupportedOperationException(name); }
        @Override public long getLong(String name) { throw new UnsupportedOperationException(name); }
        @Override public float getFloat(String name) { throw new UnsupportedOperationException(name); }
        @Override public double getDouble(String name) { throw new UnsupportedOperationException(name); }
        @Override public boolean getBoolean(String name) { throw new UnsupportedOperationException(name); }
        @Override public String getString(String name) { throw new UnsupportedOperationException(name); }
        @Override public byte[] getBinary(String name) { throw new UnsupportedOperationException(name); }
        @Override public LocalDate getDate(String name) { throw new UnsupportedOperationException(name); }
        @Override public LocalTime getTime(String name) { throw new UnsupportedOperationException(name); }
        @Override public Instant getTimestamp(String name) { throw new UnsupportedOperationException(name); }
        @Override public BigDecimal getDecimal(String name) { throw new UnsupportedOperationException(name); }
        @Override public UUID getUuid(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqInterval getInterval(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqStruct getStruct(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqIntList getListOfInts(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqLongList getListOfLongs(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqDoubleList getListOfDoubles(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqList getList(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqMap getMap(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqVariant getVariant(String name) { throw new UnsupportedOperationException(name); }
        @Override public Object getValue(String name) { throw new UnsupportedOperationException(name); }
        @Override public int getFieldCount() { return names.length; }
        @Override public String getFieldName(int index) { return names[index]; }

        @Override public int getIntAt(int idx) { throw new UnsupportedOperationException(); }
        @Override public long getLongAt(int idx) { throw new UnsupportedOperationException(); }
        @Override public float getFloatAt(int idx) { throw new UnsupportedOperationException(); }
        @Override public double getDoubleAt(int idx) { throw new UnsupportedOperationException(); }
        @Override public boolean getBooleanAt(int idx) { throw new UnsupportedOperationException(); }
        @Override public byte[] getBinaryAt(int idx) { throw new UnsupportedOperationException(); }
    }
}
