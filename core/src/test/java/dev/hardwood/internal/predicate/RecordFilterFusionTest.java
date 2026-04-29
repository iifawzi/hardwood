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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

import static org.assertj.core.api.Assertions.assertThat;

/// Equivalence tests for the arity-2 AND fusion paths in [RecordFilterFusion].
///
/// Each test compiles a 2-leaf AND predicate (which the compiler routes to the
/// fused path) and asserts the compiled matcher returns the same boolean as
/// the legacy [RecordFilterEvaluator.matchesRow] oracle on the same row.
/// All 36 (opA, opB) combinations are exercised for every fused type combo.
class RecordFilterFusionTest {

    // ==================== Long + Long, different columns ====================

    @ParameterizedTest(name = "long {0} {1} AND long {2} {3} on (a={4}, b={5}) → {6}")
    @MethodSource("longLongDiffCases")
    void longLongDifferentColumn(Operator opA, long vA, Operator opB, long vB, long aVal, long bVal,
            boolean expected) {
        FileSchema schema = twoLongSchema("a", "b");
        StructAccessor row = twoLongStub("a", aVal, "b", bVal);
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, opA, vA),
                new ResolvedPredicate.LongPredicate(1, opB, vB)));
        assertEquivalent(predicate, row, schema, expected);
    }

    static Stream<Arguments> longLongDiffCases() {
        // Cover all 36 op pairs against a representative (a, b) that exercises
        // both branches of every operator. Use values around (10, 100) with a
        // fixed (12, 100) row; every combo's expected result follows directly
        // from the operator semantics.
        return allOpPairs(opA -> opB -> Arguments.of(opA, 10L, opB, 100L, 12L, 100L,
                expected(opA, 12, 10) && expected(opB, 100, 100)));
    }

    // ==================== Long + Long, same column (range) ====================

    @ParameterizedTest(name = "long {0} {1} AND long {2} {3} on a={4} → {5}")
    @MethodSource("longRangeCases")
    void longSameColumnRange(Operator opA, long vA, Operator opB, long vB, long aVal, boolean expected) {
        FileSchema schema = longSchema("a");
        StructAccessor row = longStub("a", aVal, false);
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, opA, vA),
                new ResolvedPredicate.LongPredicate(0, opB, vB)));
        assertEquivalent(predicate, row, schema, expected);
    }

    static Stream<Arguments> longRangeCases() {
        // Row value 50 against bounds 10 (lower) and 100 (upper); every (opA, opB)
        // combo's outcome derives directly from the operators applied to 50.
        return allOpPairs(opA -> opB -> Arguments.of(opA, 10L, opB, 100L, 50L,
                expected(opA, 50, 10) && expected(opB, 50, 100)));
    }

    // ==================== Int + Int, different columns ====================

    @ParameterizedTest(name = "int {0} {1} AND int {2} {3} on (a={4}, b={5}) → {6}")
    @MethodSource("intIntDiffCases")
    void intIntDifferentColumn(Operator opA, int vA, Operator opB, int vB, int aVal, int bVal,
            boolean expected) {
        FileSchema schema = twoIntSchemaRequired("a", "b");
        StructAccessor row = twoIntStub("a", aVal, "b", bVal);
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.IntPredicate(0, opA, vA),
                new ResolvedPredicate.IntPredicate(1, opB, vB)));
        assertEquivalent(predicate, row, schema, expected);
    }

    static Stream<Arguments> intIntDiffCases() {
        return allOpPairs(opA -> opB -> Arguments.of(opA, 10, opB, 100, 12, 100,
                expected(opA, 12, 10) && expected(opB, 100, 100)));
    }

    // ==================== Int + Int, same column (range) ====================

    @ParameterizedTest(name = "int {0} {1} AND int {2} {3} on a={4} → {5}")
    @MethodSource("intRangeCases")
    void intSameColumnRange(Operator opA, int vA, Operator opB, int vB, int aVal, boolean expected) {
        FileSchema schema = intSchema("a");
        StructAccessor row = intStub("a", aVal, false);
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.IntPredicate(0, opA, vA),
                new ResolvedPredicate.IntPredicate(0, opB, vB)));
        assertEquivalent(predicate, row, schema, expected);
    }

    static Stream<Arguments> intRangeCases() {
        return allOpPairs(opA -> opB -> Arguments.of(opA, 10, opB, 100, 50,
                expected(opA, 50, 10) && expected(opB, 50, 100)));
    }

    // ==================== Double + Double, different columns ====================

    @ParameterizedTest(name = "double {0} {1} AND double {2} {3} on (a={4}, b={5}) → {6}")
    @MethodSource("doubleDoubleDiffCases")
    void doubleDoubleDifferentColumn(Operator opA, double vA, Operator opB, double vB,
            double aVal, double bVal, boolean expected) {
        FileSchema schema = twoDoubleSchema("a", "b");
        StructAccessor row = twoDoubleStub("a", aVal, "b", bVal);
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.DoublePredicate(0, opA, vA),
                new ResolvedPredicate.DoublePredicate(1, opB, vB)));
        assertEquivalent(predicate, row, schema, expected);
    }

    static Stream<Arguments> doubleDoubleDiffCases() {
        return allOpPairs(opA -> opB -> Arguments.of(opA, 1.5, opB, 9.5, 2.0, 9.5,
                expectedD(opA, 2.0, 1.5) && expectedD(opB, 9.5, 9.5)));
    }

    // ==================== Double + Double, same column (range) ====================

    @ParameterizedTest(name = "double {0} {1} AND double {2} {3} on a={4} → {5}")
    @MethodSource("doubleRangeCases")
    void doubleSameColumnRange(Operator opA, double vA, Operator opB, double vB,
            double aVal, boolean expected) {
        FileSchema schema = doubleSchema("a");
        StructAccessor row = doubleStub("a", aVal, false);
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.DoublePredicate(0, opA, vA),
                new ResolvedPredicate.DoublePredicate(0, opB, vB)));
        assertEquivalent(predicate, row, schema, expected);
    }

    static Stream<Arguments> doubleRangeCases() {
        return allOpPairs(opA -> opB -> Arguments.of(opA, 1.5, opB, 9.5, 5.0,
                expectedD(opA, 5.0, 1.5) && expectedD(opB, 5.0, 9.5)));
    }

    // ==================== Long + Double, different columns ====================

    @ParameterizedTest(name = "long {0} {1} AND double {2} {3} on (a={4}, b={5}) → {6}")
    @MethodSource("longDoubleDiffCases")
    void longDoubleDifferentColumn(Operator opA, long vA, Operator opB, double vB,
            long aVal, double bVal, boolean expected) {
        FileSchema schema = longDoubleSchema("a", "b");
        StructAccessor row = longDoubleStub("a", aVal, "b", bVal);
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, opA, vA),
                new ResolvedPredicate.DoublePredicate(1, opB, vB)));
        assertEquivalent(predicate, row, schema, expected);
    }

    static Stream<Arguments> longDoubleDiffCases() {
        return allOpPairs(opA -> opB -> Arguments.of(opA, 10L, opB, 9.5, 12L, 9.5,
                expected(opA, 12, 10) && expectedD(opB, 9.5, 9.5)));
    }

    // ==================== Double + Long, different columns (canonical-swap) ====================

    @ParameterizedTest(name = "double {0} {1} AND long {2} {3} on (a={4}, b={5}) → {6}")
    @MethodSource("doubleLongDiffCases")
    void doubleLongDifferentColumn(Operator opA, double vA, Operator opB, long vB,
            double aVal, long bVal, boolean expected) {
        // Compiler swaps to canonical (long, double) order; result must still match.
        FileSchema schema = doubleLongSchema("a", "b");
        StructAccessor row = doubleLongStub("a", aVal, "b", bVal);
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.DoublePredicate(0, opA, vA),
                new ResolvedPredicate.LongPredicate(1, opB, vB)));
        assertEquivalent(predicate, row, schema, expected);
    }

    static Stream<Arguments> doubleLongDiffCases() {
        return allOpPairs(opA -> opB -> Arguments.of(opA, 1.5, opB, 100L, 2.0, 100L,
                expectedD(opA, 2.0, 1.5) && expected(opB, 100, 100)));
    }

    // ==================== Null handling ====================

    @ParameterizedTest(name = "long+long fused with null {0}")
    @MethodSource("nullColumnCases")
    void nullColumnRejectsFusedMatch(String nullColumn) {
        FileSchema schema = twoLongSchema("a", "b");
        StructAccessor row = twoLongStubWithNulls("a", 12L, "b", 100L,
                "a".equals(nullColumn), "b".equals(nullColumn));
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                new ResolvedPredicate.LongPredicate(1, Operator.LT, 1000L)));
        // Fused matcher must short-circuit on null and return false (legacy behaviour).
        assertEquivalent(predicate, row, schema, false);
    }

    static Stream<Arguments> nullColumnCases() {
        return Stream.of(Arguments.of("a"), Arguments.of("b"));
    }

    // ==================== Helpers ====================

    private static void assertEquivalent(ResolvedPredicate predicate, StructAccessor row, FileSchema schema,
            boolean expected) {
        boolean legacy = RecordFilterEvaluator.matchesRow(predicate, row, schema);
        boolean compiled = RecordFilterCompiler.compile(predicate, schema).test(row);
        assertThat(compiled)
                .as("legacy/compiled disagreement for %s", predicate)
                .isEqualTo(legacy);
        assertThat(legacy)
                .as("legacy oracle disagreed with expected outcome for %s", predicate)
                .isEqualTo(expected);
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

    private static Stream<Arguments> allOpPairs(java.util.function.Function<Operator,
            java.util.function.Function<Operator, Arguments>> builder) {
        Stream.Builder<Arguments> out = Stream.builder();
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(builder.apply(opA).apply(opB));
            }
        }
        return out.build();
    }

    // ==================== Schemas ====================

    private static FileSchema intSchema(String name) {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement col = new SchemaElement(name, PhysicalType.INT32, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, col));
    }

    private static FileSchema longSchema(String name) {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement col = new SchemaElement(name, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, col));
    }

    private static FileSchema doubleSchema(String name) {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement col = new SchemaElement(name, PhysicalType.DOUBLE, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, col));
    }

    private static FileSchema twoLongSchema(String n1, String n2) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(n1, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(n2, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2));
    }

    private static FileSchema twoIntSchemaRequired(String n1, String n2) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(n1, PhysicalType.INT32, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(n2, PhysicalType.INT32, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2));
    }

    private static FileSchema twoDoubleSchema(String n1, String n2) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(n1, PhysicalType.DOUBLE, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(n2, PhysicalType.DOUBLE, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2));
    }

    private static FileSchema longDoubleSchema(String longName, String dblName) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(longName, PhysicalType.INT64, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(dblName, PhysicalType.DOUBLE, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2));
    }

    private static FileSchema doubleLongSchema(String dblName, String longName) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(dblName, PhysicalType.DOUBLE, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(longName, PhysicalType.INT64, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2));
    }

    // ==================== Stubs ====================

    private static StructAccessor intStub(String name, int value, boolean isNull) {
        return new SingleFieldStub(name, isNull) {
            @Override public int getInt(String n) { return value; }
        };
    }

    private static StructAccessor longStub(String name, long value, boolean isNull) {
        return new SingleFieldStub(name, isNull) {
            @Override public long getLong(String n) { return value; }
        };
    }

    private static StructAccessor doubleStub(String name, double value, boolean isNull) {
        return new SingleFieldStub(name, isNull) {
            @Override public double getDouble(String n) { return value; }
        };
    }

    private static StructAccessor twoIntStub(String n1, int v1, String n2, int v2) {
        return new TwoFieldStub(n1, n2, false, false) {
            @Override public int getInt(String name) { return name.equals(n1) ? v1 : v2; }
        };
    }

    private static StructAccessor twoLongStub(String n1, long v1, String n2, long v2) {
        return new TwoFieldStub(n1, n2, false, false) {
            @Override public long getLong(String name) { return name.equals(n1) ? v1 : v2; }
        };
    }

    private static StructAccessor twoLongStubWithNulls(String n1, long v1, String n2, long v2,
            boolean null1, boolean null2) {
        return new TwoFieldStub(n1, n2, null1, null2) {
            @Override public long getLong(String name) { return name.equals(n1) ? v1 : v2; }
        };
    }

    private static StructAccessor twoDoubleStub(String n1, double v1, String n2, double v2) {
        return new TwoFieldStub(n1, n2, false, false) {
            @Override public double getDouble(String name) { return name.equals(n1) ? v1 : v2; }
        };
    }

    private static StructAccessor longDoubleStub(String longName, long longVal, String dblName, double dblVal) {
        return new TwoFieldStub(longName, dblName, false, false) {
            @Override public long getLong(String name) { return longVal; }
            @Override public double getDouble(String name) { return dblVal; }
        };
    }

    private static StructAccessor doubleLongStub(String dblName, double dblVal, String longName, long longVal) {
        return new TwoFieldStub(dblName, longName, false, false) {
            @Override public double getDouble(String name) { return dblVal; }
            @Override public long getLong(String name) { return longVal; }
        };
    }

    /// Base stub for 2-column rows; subclasses override only the typed accessors they need.
    private abstract static class TwoFieldStub implements StructAccessor {
        private final String n1;
        private final String n2;
        private final boolean null1;
        private final boolean null2;

        TwoFieldStub(String n1, String n2, boolean null1, boolean null2) {
            this.n1 = n1;
            this.n2 = n2;
            this.null1 = null1;
            this.null2 = null2;
        }

        @Override public boolean isNull(String name) {
            if (name.equals(n1)) return null1;
            if (name.equals(n2)) return null2;
            throw new IllegalArgumentException(name);
        }
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
        @Override public int getFieldCount() { return 2; }
        @Override public String getFieldName(int index) { return index == 0 ? n1 : n2; }
    }

    /// Base stub for 1-column rows.
    private abstract static class SingleFieldStub implements StructAccessor {
        private final String fieldName;
        private final boolean isNull;

        SingleFieldStub(String fieldName, boolean isNull) {
            this.fieldName = fieldName;
            this.isNull = isNull;
        }

        @Override public boolean isNull(String name) { return isNull; }
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
        @Override public int getFieldCount() { return 1; }
        @Override public String getFieldName(int index) { return fieldName; }
    }
}
