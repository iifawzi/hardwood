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

/// Equivalence tests for the bytecode-generated arity-2 fusion in
/// [RecordFilterFusionBC]. For every fused tuple `(typeA, opA, vA, typeB,
/// opB, vB, sameColumn, isAnd, accessMode)`, asserts that the BC matcher
/// agrees with the legacy [RecordFilterEvaluator] oracle.
///
/// Same-column fusion requires both leaves to reference the same physical
/// column; rows here are constructed accordingly. Diff-column fusion is
/// already covered by [RecordFilterIndexedTest]; this file targets the
/// same-column body shape that the BC dispatcher fires on.
class RecordFilterFusionBCTest {

    private static final Operator[] OPS = {
            Operator.EQ, Operator.NOT_EQ, Operator.LT, Operator.LT_EQ, Operator.GT, Operator.GT_EQ
    };

    // ==================== Long+Long same-column ====================

    static Stream<Arguments> longSameColCases() {
        Stream.Builder<Arguments> out = Stream.builder();
        long[] values = { Long.MIN_VALUE, -1L, 0L, 1L, 100L, Long.MAX_VALUE };
        long[] vAs = { 0L, 50L };
        long[] vBs = { 100L, -10L };
        for (boolean isAnd : new boolean[] { true, false }) {
            for (Operator opA : OPS) {
                for (Operator opB : OPS) {
                    for (long vA : vAs) {
                        for (long vB : vBs) {
                            for (long v : values) {
                                out.add(Arguments.of(isAnd, opA, vA, opB, vB, v, false));
                            }
                            out.add(Arguments.of(isAnd, opA, vA, opB, vB, 0L, true)); // null leaf
                        }
                    }
                }
            }
        }
        return out.build();
    }

    @ParameterizedTest(name = "long {1}/{2} {3}/{4} v={5} null={6} and={0}")
    @MethodSource("longSameColCases")
    void longSameColumnFusionMatchesOracle(boolean isAnd, Operator opA, long vA, Operator opB, long vB,
            long v, boolean isNull) {
        FileSchema schema = oneLongSchema("a");
        ProjectedSchema projection = projectAll(schema);
        ResolvedPredicate predicate = compoundOf(isAnd,
                new ResolvedPredicate.LongPredicate(0, opA, vA),
                new ResolvedPredicate.LongPredicate(0, opB, vB));
        OneLongIndexedRow row = new OneLongIndexedRow(v, isNull);
        assertEquivalent(predicate, row, schema, projection);
    }

    // ==================== Int+Int same-column ====================

    static Stream<Arguments> intSameColCases() {
        Stream.Builder<Arguments> out = Stream.builder();
        int[] values = { Integer.MIN_VALUE, -1, 0, 1, 100, Integer.MAX_VALUE };
        int[] vAs = { 0, 50 };
        int[] vBs = { 100, -10 };
        for (boolean isAnd : new boolean[] { true, false }) {
            for (Operator opA : OPS) {
                for (Operator opB : OPS) {
                    for (int vA : vAs) {
                        for (int vB : vBs) {
                            for (int v : values) {
                                out.add(Arguments.of(isAnd, opA, vA, opB, vB, v, false));
                            }
                            out.add(Arguments.of(isAnd, opA, vA, opB, vB, 0, true));
                        }
                    }
                }
            }
        }
        return out.build();
    }

    @ParameterizedTest(name = "int {1}/{2} {3}/{4} v={5} null={6} and={0}")
    @MethodSource("intSameColCases")
    void intSameColumnFusionMatchesOracle(boolean isAnd, Operator opA, int vA, Operator opB, int vB,
            int v, boolean isNull) {
        FileSchema schema = oneIntSchema("a");
        ProjectedSchema projection = projectAll(schema);
        ResolvedPredicate predicate = compoundOf(isAnd,
                new ResolvedPredicate.IntPredicate(0, opA, vA),
                new ResolvedPredicate.IntPredicate(0, opB, vB));
        OneIntIndexedRow row = new OneIntIndexedRow(v, isNull);
        assertEquivalent(predicate, row, schema, projection);
    }

    // ==================== Double+Double same-column ====================

    static Stream<Arguments> doubleSameColCases() {
        Stream.Builder<Arguments> out = Stream.builder();
        double[] values = { Double.NEGATIVE_INFINITY, -1.5, 0.0, 1.5, 100.0,
                Double.POSITIVE_INFINITY, Double.NaN };
        for (boolean isAnd : new boolean[] { true, false }) {
            for (Operator opA : OPS) {
                for (Operator opB : OPS) {
                    for (double v : values) {
                        out.add(Arguments.of(isAnd, opA, 0.0, opB, 100.0, v, false));
                    }
                    out.add(Arguments.of(isAnd, opA, 0.0, opB, 100.0, 0.0, true));
                }
            }
        }
        return out.build();
    }

    @ParameterizedTest(name = "double {1}/{2} {3}/{4} v={5} null={6} and={0}")
    @MethodSource("doubleSameColCases")
    void doubleSameColumnFusionMatchesOracle(boolean isAnd, Operator opA, double vA, Operator opB, double vB,
            double v, boolean isNull) {
        FileSchema schema = oneDoubleSchema("a");
        ProjectedSchema projection = projectAll(schema);
        ResolvedPredicate predicate = compoundOf(isAnd,
                new ResolvedPredicate.DoublePredicate(0, opA, vA),
                new ResolvedPredicate.DoublePredicate(0, opB, vB));
        OneDoubleIndexedRow row = new OneDoubleIndexedRow(v, isNull);
        assertEquivalent(predicate, row, schema, projection);
    }

    // ==================== Helpers ====================

    private static ResolvedPredicate compoundOf(boolean isAnd, ResolvedPredicate a, ResolvedPredicate b) {
        return isAnd ? new ResolvedPredicate.And(List.of(a, b)) : new ResolvedPredicate.Or(List.of(a, b));
    }

    private static void assertEquivalent(ResolvedPredicate predicate, RowReader row, FileSchema schema,
            ProjectedSchema projection) {
        boolean legacy = RecordFilterEvaluator.matchesRow(predicate, row, schema);
        boolean compiledIndexed = RecordFilterCompiler
                .compile(predicate, schema, projection::toProjectedIndex).test(row);
        boolean compiledName = RecordFilterCompiler.compile(predicate, schema).test(row);
        assertThat(compiledIndexed).as("BC-indexed disagreed with legacy for %s", predicate).isEqualTo(legacy);
        assertThat(compiledName).as("name-keyed disagreed with legacy for %s", predicate).isEqualTo(legacy);
    }

    private static ProjectedSchema projectAll(FileSchema schema) {
        return ProjectedSchema.create(schema, ColumnProjection.all());
    }

    private static FileSchema oneLongSchema(String name) {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement leaf = new SchemaElement(name, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, leaf));
    }

    private static FileSchema oneIntSchema(String name) {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement leaf = new SchemaElement(name, PhysicalType.INT32, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, leaf));
    }

    private static FileSchema oneDoubleSchema(String name) {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement leaf = new SchemaElement(name, PhysicalType.DOUBLE, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, leaf));
    }

    // ==================== Row stubs ====================

    private static final class OneLongIndexedRow extends BaseIndexedRow {
        private final long v;
        OneLongIndexedRow(long v, boolean isNull) {
            super(new String[] { "a" }, new boolean[] { isNull });
            this.v = v;
        }
        @Override public long getLong(String name) { return v; }
        @Override public long getLong(int idx) { return v; }
    }

    private static final class OneIntIndexedRow extends BaseIndexedRow {
        private final int v;
        OneIntIndexedRow(int v, boolean isNull) {
            super(new String[] { "a" }, new boolean[] { isNull });
            this.v = v;
        }
        @Override public int getInt(String name) { return v; }
        @Override public int getInt(int idx) { return v; }
    }

    private static final class OneDoubleIndexedRow extends BaseIndexedRow {
        private final double v;
        OneDoubleIndexedRow(double v, boolean isNull) {
            super(new String[] { "a" }, new boolean[] { isNull });
            this.v = v;
        }
        @Override public double getDouble(String name) { return v; }
        @Override public double getDouble(int idx) { return v; }
    }

    private abstract static class BaseIndexedRow implements RowReader {
        private final String[] names;
        private final boolean[] nulls;

        BaseIndexedRow(String[] names, boolean[] nulls) {
            this.names = names;
            this.nulls = nulls;
        }

        @Override public boolean isNull(String name) { return nulls[indexOf(name)]; }
        @Override public boolean isNull(int idx) { return nulls[idx]; }

        protected int indexOf(String name) {
            for (int i = 0; i < names.length; i++) {
                if (names[i].equals(name)) return i;
            }
            throw new IllegalArgumentException(name);
        }

        @Override public boolean hasNext() { throw new UnsupportedOperationException(); }
        @Override public void next() { throw new UnsupportedOperationException(); }
        @Override public void close() { }

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

        @Override public int getInt(int idx) { throw new UnsupportedOperationException(); }
        @Override public long getLong(int idx) { throw new UnsupportedOperationException(); }
        @Override public float getFloat(int idx) { throw new UnsupportedOperationException(); }
        @Override public double getDouble(int idx) { throw new UnsupportedOperationException(); }
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
