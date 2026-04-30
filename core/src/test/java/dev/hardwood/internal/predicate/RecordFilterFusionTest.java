/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
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
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Three-way equivalence: fused matcher (via [RecordFilterCompiler] with
/// fusion enabled) must agree with the legacy [RecordFilterEvaluator] oracle
/// for every fused (combo × connective × opA × opB × same/diff column ×
/// null / non-null) tuple. The indexed compile path is also exercised
/// where applicable, so each test covers the fused-name and fused-indexed
/// matchers in addition to the oracle.
class RecordFilterFusionTest {

    // ==================== long+long ====================

    @ParameterizedTest(name = "long+long AND diff: a {0} {1} AND b {2} {3} on a={4},b={5}")
    @MethodSource("longLongDiffCases")
    void longLongAndDiff(Operator opA, long vA, Operator opB, long vB, long aVal, long bVal) {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoLongRow row = new TwoLongRow(aVal, false, bVal, false);
        ResolvedPredicate p = and(
                new ResolvedPredicate.LongPredicate(0, opA, vA),
                new ResolvedPredicate.LongPredicate(1, opB, vB));
        boolean expected = applyLong(opA, aVal, vA) && applyLong(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    @ParameterizedTest(name = "long+long OR diff: a {0} {1} OR b {2} {3} on a={4},b={5}")
    @MethodSource("longLongDiffCases")
    void longLongOrDiff(Operator opA, long vA, Operator opB, long vB, long aVal, long bVal) {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoLongRow row = new TwoLongRow(aVal, false, bVal, false);
        ResolvedPredicate p = or(
                new ResolvedPredicate.LongPredicate(0, opA, vA),
                new ResolvedPredicate.LongPredicate(1, opB, vB));
        boolean expected = applyLong(opA, aVal, vA) || applyLong(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    @ParameterizedTest(name = "long+long range AND: col {0} {1} AND col {2} {3} on val={4}")
    @MethodSource("longRangeCases")
    void longRangeAnd(Operator opA, long vA, Operator opB, long vB, long val) {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoLongRow row = new TwoLongRow(val, false, 0L, false);
        ResolvedPredicate p = and(
                new ResolvedPredicate.LongPredicate(0, opA, vA),
                new ResolvedPredicate.LongPredicate(0, opB, vB));
        boolean expected = applyLong(opA, val, vA) && applyLong(opB, val, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    @ParameterizedTest(name = "long+long range OR: col {0} {1} OR col {2} {3} on val={4}")
    @MethodSource("longRangeCases")
    void longRangeOr(Operator opA, long vA, Operator opB, long vB, long val) {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoLongRow row = new TwoLongRow(val, false, 0L, false);
        ResolvedPredicate p = or(
                new ResolvedPredicate.LongPredicate(0, opA, vA),
                new ResolvedPredicate.LongPredicate(0, opB, vB));
        boolean expected = applyLong(opA, val, vA) || applyLong(opB, val, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    @Test
    void longLongAndNullLeavesShortCircuit() {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        ResolvedPredicate p = and(
                new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                new ResolvedPredicate.LongPredicate(1, Operator.LT, 1000L));
        assertEquivalent(p, new TwoLongRow(5L, true, 100L, false), schema, projection, false);
        assertEquivalent(p, new TwoLongRow(5L, false, 100L, true), schema, projection, false);
    }

    @Test
    void longLongOrNullLeafEvaluatesOther() {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        ResolvedPredicate p = or(
                new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 100L),
                new ResolvedPredicate.LongPredicate(1, Operator.LT, 1000L));
        assertEquivalent(p, new TwoLongRow(5L, true, 100L, false), schema, projection, true);
        assertEquivalent(p, new TwoLongRow(5L, true, 9999L, false), schema, projection, false);
        assertEquivalent(p, new TwoLongRow(5L, true, 100L, true), schema, projection, false);
    }

    static Stream<Arguments> longLongDiffCases() {
        List<Arguments> out = new ArrayList<>();
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(Arguments.of(opA, 50L, opB, 100L, 50L, 100L));
                out.add(Arguments.of(opA, 50L, opB, 100L, 49L, 99L));
                out.add(Arguments.of(opA, 50L, opB, 100L, 51L, 101L));
            }
        }
        return out.stream();
    }

    static Stream<Arguments> longRangeCases() {
        List<Arguments> out = new ArrayList<>();
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(Arguments.of(opA, 50L, opB, 100L, 75L));
                out.add(Arguments.of(opA, 50L, opB, 100L, 50L));
                out.add(Arguments.of(opA, 50L, opB, 100L, 100L));
                out.add(Arguments.of(opA, 50L, opB, 100L, 49L));
                out.add(Arguments.of(opA, 50L, opB, 100L, 101L));
            }
        }
        return out.stream();
    }

    // ==================== int+int ====================

    @ParameterizedTest(name = "int+int AND diff: a {0} {1} AND b {2} {3} on a={4},b={5}")
    @MethodSource("intIntDiffCases")
    void intIntAndDiff(Operator opA, int vA, Operator opB, int vB, int aVal, int bVal) {
        FileSchema schema = twoIntSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoIntRow row = new TwoIntRow(aVal, false, bVal, false);
        ResolvedPredicate p = and(
                new ResolvedPredicate.IntPredicate(0, opA, vA),
                new ResolvedPredicate.IntPredicate(1, opB, vB));
        boolean expected = applyInt(opA, aVal, vA) && applyInt(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    @ParameterizedTest(name = "int+int OR diff: a {0} {1} OR b {2} {3} on a={4},b={5}")
    @MethodSource("intIntDiffCases")
    void intIntOrDiff(Operator opA, int vA, Operator opB, int vB, int aVal, int bVal) {
        FileSchema schema = twoIntSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoIntRow row = new TwoIntRow(aVal, false, bVal, false);
        ResolvedPredicate p = or(
                new ResolvedPredicate.IntPredicate(0, opA, vA),
                new ResolvedPredicate.IntPredicate(1, opB, vB));
        boolean expected = applyInt(opA, aVal, vA) || applyInt(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    @ParameterizedTest(name = "int+int range: col {0} {1} ANDOR col {2} {3} on val={4}")
    @MethodSource("intRangeCases")
    void intRangeAnd(Operator opA, int vA, Operator opB, int vB, int val) {
        FileSchema schema = twoIntSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoIntRow row = new TwoIntRow(val, false, 0, false);
        ResolvedPredicate pAnd = and(
                new ResolvedPredicate.IntPredicate(0, opA, vA),
                new ResolvedPredicate.IntPredicate(0, opB, vB));
        boolean expectedAnd = applyInt(opA, val, vA) && applyInt(opB, val, vB);
        assertEquivalent(pAnd, row, schema, projection, expectedAnd);
        ResolvedPredicate pOr = or(
                new ResolvedPredicate.IntPredicate(0, opA, vA),
                new ResolvedPredicate.IntPredicate(0, opB, vB));
        boolean expectedOr = applyInt(opA, val, vA) || applyInt(opB, val, vB);
        assertEquivalent(pOr, row, schema, projection, expectedOr);
    }

    static Stream<Arguments> intIntDiffCases() {
        List<Arguments> out = new ArrayList<>();
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(Arguments.of(opA, 50, opB, 100, 50, 100));
                out.add(Arguments.of(opA, 50, opB, 100, 49, 99));
                out.add(Arguments.of(opA, 50, opB, 100, 51, 101));
            }
        }
        return out.stream();
    }

    static Stream<Arguments> intRangeCases() {
        List<Arguments> out = new ArrayList<>();
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(Arguments.of(opA, 50, opB, 100, 75));
                out.add(Arguments.of(opA, 50, opB, 100, 50));
                out.add(Arguments.of(opA, 50, opB, 100, 100));
            }
        }
        return out.stream();
    }

    // ==================== double+double ====================

    @ParameterizedTest(name = "double+double AND diff: {0} {1} AND {2} {3} on {4},{5}")
    @MethodSource("doubleDiffCases")
    void doubleDoubleAndDiff(Operator opA, double vA, Operator opB, double vB, double aVal, double bVal) {
        FileSchema schema = twoDoubleSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoDoubleRow row = new TwoDoubleRow(aVal, false, bVal, false);
        ResolvedPredicate p = and(
                new ResolvedPredicate.DoublePredicate(0, opA, vA),
                new ResolvedPredicate.DoublePredicate(1, opB, vB));
        boolean expected = applyDouble(opA, aVal, vA) && applyDouble(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    @ParameterizedTest(name = "double+double OR diff: {0} {1} OR {2} {3} on {4},{5}")
    @MethodSource("doubleDiffCases")
    void doubleDoubleOrDiff(Operator opA, double vA, Operator opB, double vB, double aVal, double bVal) {
        FileSchema schema = twoDoubleSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoDoubleRow row = new TwoDoubleRow(aVal, false, bVal, false);
        ResolvedPredicate p = or(
                new ResolvedPredicate.DoublePredicate(0, opA, vA),
                new ResolvedPredicate.DoublePredicate(1, opB, vB));
        boolean expected = applyDouble(opA, aVal, vA) || applyDouble(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    @ParameterizedTest(name = "double range: col {0} {1} ... {2} {3} on {4}")
    @MethodSource("doubleRangeCases")
    void doubleRange(Operator opA, double vA, Operator opB, double vB, double val) {
        FileSchema schema = twoDoubleSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoDoubleRow row = new TwoDoubleRow(val, false, 0.0, false);
        assertEquivalent(and(new ResolvedPredicate.DoublePredicate(0, opA, vA),
                new ResolvedPredicate.DoublePredicate(0, opB, vB)), row, schema, projection,
                applyDouble(opA, val, vA) && applyDouble(opB, val, vB));
        assertEquivalent(or(new ResolvedPredicate.DoublePredicate(0, opA, vA),
                new ResolvedPredicate.DoublePredicate(0, opB, vB)), row, schema, projection,
                applyDouble(opA, val, vA) || applyDouble(opB, val, vB));
    }

    static Stream<Arguments> doubleDiffCases() {
        List<Arguments> out = new ArrayList<>();
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(Arguments.of(opA, 1.5, opB, 2.5, 1.5, 2.5));
                out.add(Arguments.of(opA, 1.5, opB, 2.5, 1.4, 2.4));
                // NaN ordering — Double.compare puts NaN above +Infinity.
                out.add(Arguments.of(opA, 1.0, opB, 1.0, Double.NaN, 1.0));
                out.add(Arguments.of(opA, 1.0, opB, 1.0, 1.0, Double.NaN));
            }
        }
        return out.stream();
    }

    static Stream<Arguments> doubleRangeCases() {
        List<Arguments> out = new ArrayList<>();
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(Arguments.of(opA, 1.5, opB, 2.5, 2.0));
                out.add(Arguments.of(opA, 1.5, opB, 2.5, 1.5));
                out.add(Arguments.of(opA, 1.5, opB, 2.5, Double.NaN));
            }
        }
        return out.stream();
    }

    // ==================== boolean+boolean ====================

    @ParameterizedTest(name = "boolean+boolean AND diff: a {0} {1} AND b {2} {3} on a={4},b={5}")
    @MethodSource("booleanDiffCases")
    void booleanBooleanAndDiff(Operator opA, boolean vA, Operator opB, boolean vB, boolean aVal, boolean bVal) {
        FileSchema schema = twoBooleanSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoBooleanRow row = new TwoBooleanRow(aVal, false, bVal, false);
        ResolvedPredicate p = and(
                new ResolvedPredicate.BooleanPredicate(0, opA, vA),
                new ResolvedPredicate.BooleanPredicate(1, opB, vB));
        boolean expected = applyBoolean(opA, aVal, vA) && applyBoolean(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    @ParameterizedTest(name = "boolean+boolean OR diff: a {0} {1} OR b {2} {3} on a={4},b={5}")
    @MethodSource("booleanDiffCases")
    void booleanBooleanOrDiff(Operator opA, boolean vA, Operator opB, boolean vB, boolean aVal, boolean bVal) {
        FileSchema schema = twoBooleanSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoBooleanRow row = new TwoBooleanRow(aVal, false, bVal, false);
        ResolvedPredicate p = or(
                new ResolvedPredicate.BooleanPredicate(0, opA, vA),
                new ResolvedPredicate.BooleanPredicate(1, opB, vB));
        boolean expected = applyBoolean(opA, aVal, vA) || applyBoolean(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    static Stream<Arguments> booleanDiffCases() {
        List<Arguments> out = new ArrayList<>();
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(Arguments.of(opA, true, opB, true, true, true));
                out.add(Arguments.of(opA, true, opB, false, false, true));
                out.add(Arguments.of(opA, false, opB, true, true, false));
            }
        }
        return out.stream();
    }

    // ==================== binary+binary ====================

    @ParameterizedTest(name = "binary+binary AND diff: {0} {1} AND {2} {3}")
    @MethodSource("binaryDiffCases")
    void binaryBinaryAndDiff(Operator opA, byte[] vA, Operator opB, byte[] vB, byte[] aVal, byte[] bVal) {
        FileSchema schema = twoBinarySchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoBinaryRow row = new TwoBinaryRow(aVal, false, bVal, false);
        ResolvedPredicate p = and(
                new ResolvedPredicate.BinaryPredicate(0, opA, vA, false),
                new ResolvedPredicate.BinaryPredicate(1, opB, vB, false));
        boolean expected = applyBinary(opA, aVal, vA, false) && applyBinary(opB, bVal, vB, false);
        assertEquivalent(p, row, schema, projection, expected);
    }

    @ParameterizedTest(name = "binary+binary OR diff: {0} {1} OR {2} {3}")
    @MethodSource("binaryDiffCases")
    void binaryBinaryOrDiff(Operator opA, byte[] vA, Operator opB, byte[] vB, byte[] aVal, byte[] bVal) {
        FileSchema schema = twoBinarySchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoBinaryRow row = new TwoBinaryRow(aVal, false, bVal, false);
        ResolvedPredicate p = or(
                new ResolvedPredicate.BinaryPredicate(0, opA, vA, false),
                new ResolvedPredicate.BinaryPredicate(1, opB, vB, false));
        boolean expected = applyBinary(opA, aVal, vA, false) || applyBinary(opB, bVal, vB, false);
        assertEquivalent(p, row, schema, projection, expected);
    }

    @ParameterizedTest(name = "binary range: col {0} {1} ... {2} {3}")
    @MethodSource("binaryRangeCases")
    void binaryRange(Operator opA, byte[] vA, Operator opB, byte[] vB, byte[] val) {
        FileSchema schema = twoBinarySchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoBinaryRow row = new TwoBinaryRow(val, false, new byte[0], false);
        assertEquivalent(and(new ResolvedPredicate.BinaryPredicate(0, opA, vA, false),
                new ResolvedPredicate.BinaryPredicate(0, opB, vB, false)), row, schema, projection,
                applyBinary(opA, val, vA, false) && applyBinary(opB, val, vB, false));
        assertEquivalent(or(new ResolvedPredicate.BinaryPredicate(0, opA, vA, false),
                new ResolvedPredicate.BinaryPredicate(0, opB, vB, false)), row, schema, projection,
                applyBinary(opA, val, vA, false) || applyBinary(opB, val, vB, false));
    }

    static Stream<Arguments> binaryDiffCases() {
        List<Arguments> out = new ArrayList<>();
        byte[] abc = "abc".getBytes(StandardCharsets.UTF_8);
        byte[] xyz = "xyz".getBytes(StandardCharsets.UTF_8);
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(Arguments.of(opA, abc, opB, xyz, abc, xyz));
                out.add(Arguments.of(opA, abc, opB, xyz, xyz, abc));
            }
        }
        return out.stream();
    }

    static Stream<Arguments> binaryRangeCases() {
        List<Arguments> out = new ArrayList<>();
        byte[] abc = "abc".getBytes(StandardCharsets.UTF_8);
        byte[] xyz = "xyz".getBytes(StandardCharsets.UTF_8);
        byte[] mid = "mno".getBytes(StandardCharsets.UTF_8);
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(Arguments.of(opA, abc, opB, xyz, mid));
                out.add(Arguments.of(opA, abc, opB, xyz, abc));
                out.add(Arguments.of(opA, abc, opB, xyz, xyz));
            }
        }
        return out.stream();
    }

    // ==================== cross-type ====================

    @ParameterizedTest(name = "long+double AND: a {0} {1} AND b {2} {3} on {4},{5}")
    @MethodSource("longDoubleCases")
    void longDoubleAnd(Operator opA, long vA, Operator opB, double vB, long aVal, double bVal) {
        FileSchema schema = longDoubleSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        LongDoubleRow row = new LongDoubleRow(aVal, false, bVal, false);
        ResolvedPredicate p = and(
                new ResolvedPredicate.LongPredicate(0, opA, vA),
                new ResolvedPredicate.DoublePredicate(1, opB, vB));
        boolean expected = applyLong(opA, aVal, vA) && applyDouble(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
        // Swapped order — canonicalisation must produce the same matcher result.
        ResolvedPredicate swapped = and(
                new ResolvedPredicate.DoublePredicate(1, opB, vB),
                new ResolvedPredicate.LongPredicate(0, opA, vA));
        assertEquivalent(swapped, row, schema, projection, expected);
    }

    @ParameterizedTest(name = "long+double OR: a {0} {1} OR b {2} {3} on {4},{5}")
    @MethodSource("longDoubleCases")
    void longDoubleOr(Operator opA, long vA, Operator opB, double vB, long aVal, double bVal) {
        FileSchema schema = longDoubleSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        LongDoubleRow row = new LongDoubleRow(aVal, false, bVal, false);
        ResolvedPredicate p = or(
                new ResolvedPredicate.LongPredicate(0, opA, vA),
                new ResolvedPredicate.DoublePredicate(1, opB, vB));
        boolean expected = applyLong(opA, aVal, vA) || applyDouble(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    static Stream<Arguments> longDoubleCases() {
        List<Arguments> out = new ArrayList<>();
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(Arguments.of(opA, 50L, opB, 1.5, 50L, 1.5));
                out.add(Arguments.of(opA, 50L, opB, 1.5, 49L, 1.4));
            }
        }
        return out.stream();
    }

    @ParameterizedTest(name = "int+long AND: a {0} {1} AND b {2} {3} on {4},{5}")
    @MethodSource("intLongCases")
    void intLongAnd(Operator opA, int vA, Operator opB, long vB, int aVal, long bVal) {
        FileSchema schema = intLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        IntLongRow row = new IntLongRow(aVal, false, bVal, false);
        ResolvedPredicate p = and(
                new ResolvedPredicate.IntPredicate(0, opA, vA),
                new ResolvedPredicate.LongPredicate(1, opB, vB));
        boolean expected = applyInt(opA, aVal, vA) && applyLong(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    @ParameterizedTest(name = "int+long OR: a {0} {1} OR b {2} {3} on {4},{5}")
    @MethodSource("intLongCases")
    void intLongOr(Operator opA, int vA, Operator opB, long vB, int aVal, long bVal) {
        FileSchema schema = intLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        IntLongRow row = new IntLongRow(aVal, false, bVal, false);
        ResolvedPredicate p = or(
                new ResolvedPredicate.IntPredicate(0, opA, vA),
                new ResolvedPredicate.LongPredicate(1, opB, vB));
        boolean expected = applyInt(opA, aVal, vA) || applyLong(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    static Stream<Arguments> intLongCases() {
        List<Arguments> out = new ArrayList<>();
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(Arguments.of(opA, 50, opB, 100L, 50, 100L));
                out.add(Arguments.of(opA, 50, opB, 100L, 49, 101L));
            }
        }
        return out.stream();
    }

    @ParameterizedTest(name = "int+double AND: a {0} {1} AND b {2} {3} on {4},{5}")
    @MethodSource("intDoubleCases")
    void intDoubleAnd(Operator opA, int vA, Operator opB, double vB, int aVal, double bVal) {
        FileSchema schema = intDoubleSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        IntDoubleRow row = new IntDoubleRow(aVal, false, bVal, false);
        ResolvedPredicate p = and(
                new ResolvedPredicate.IntPredicate(0, opA, vA),
                new ResolvedPredicate.DoublePredicate(1, opB, vB));
        boolean expected = applyInt(opA, aVal, vA) && applyDouble(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    @ParameterizedTest(name = "int+double OR: a {0} {1} OR b {2} {3} on {4},{5}")
    @MethodSource("intDoubleCases")
    void intDoubleOr(Operator opA, int vA, Operator opB, double vB, int aVal, double bVal) {
        FileSchema schema = intDoubleSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        IntDoubleRow row = new IntDoubleRow(aVal, false, bVal, false);
        ResolvedPredicate p = or(
                new ResolvedPredicate.IntPredicate(0, opA, vA),
                new ResolvedPredicate.DoublePredicate(1, opB, vB));
        boolean expected = applyInt(opA, aVal, vA) || applyDouble(opB, bVal, vB);
        assertEquivalent(p, row, schema, projection, expected);
    }

    static Stream<Arguments> intDoubleCases() {
        List<Arguments> out = new ArrayList<>();
        for (Operator opA : Operator.values()) {
            for (Operator opB : Operator.values()) {
                out.add(Arguments.of(opA, 50, opB, 1.5, 50, 1.5));
                out.add(Arguments.of(opA, 50, opB, 1.5, 49, 1.4));
            }
        }
        return out.stream();
    }

    // ==================== fusion-disable flag check ====================

    @Test
    void fusionProducesNonGenericMatcher() {
        FileSchema schema = twoLongSchema("a", "b");
        ResolvedPredicate p = and(
                new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                new ResolvedPredicate.LongPredicate(1, Operator.LT, 1000L));
        RowMatcher m = RecordFilterCompiler.compile(p, schema);
        assertThat(m.getClass().getSimpleName()).doesNotContain("And2Matcher");
    }

    // ==================== nested-path fallback ====================

    @Test
    void nestedPathFusionFallsBackToNameKeyed() {
        // Two nested long columns under struct "outer". Indexed fusion must
        // refuse (not top-level) and the name-keyed fusion must still apply.
        FileSchema schema = nestedTwoLongSchema();
        ProjectedSchema projection = projectAll(schema);
        NestedTwoLongRow row = new NestedTwoLongRow(5L, false, 100L, false);
        ResolvedPredicate p = and(
                new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                new ResolvedPredicate.LongPredicate(1, Operator.LT, 1000L));
        boolean expected = true;
        assertEquivalent(p, row, schema, projection, expected);
    }

    // ==================== helpers ====================

    private static ResolvedPredicate and(ResolvedPredicate a, ResolvedPredicate b) {
        return new ResolvedPredicate.And(List.of(a, b));
    }

    private static ResolvedPredicate or(ResolvedPredicate a, ResolvedPredicate b) {
        return new ResolvedPredicate.Or(List.of(a, b));
    }

    private static boolean applyLong(Operator op, long actual, long target) {
        return switch (op) {
            case EQ -> actual == target;
            case NOT_EQ -> actual != target;
            case LT -> actual < target;
            case LT_EQ -> actual <= target;
            case GT -> actual > target;
            case GT_EQ -> actual >= target;
        };
    }

    private static boolean applyInt(Operator op, int actual, int target) {
        return switch (op) {
            case EQ -> actual == target;
            case NOT_EQ -> actual != target;
            case LT -> actual < target;
            case LT_EQ -> actual <= target;
            case GT -> actual > target;
            case GT_EQ -> actual >= target;
        };
    }

    private static boolean applyDouble(Operator op, double actual, double target) {
        int cmp = Double.compare(actual, target);
        return switch (op) {
            case EQ -> cmp == 0;
            case NOT_EQ -> cmp != 0;
            case LT -> cmp < 0;
            case LT_EQ -> cmp <= 0;
            case GT -> cmp > 0;
            case GT_EQ -> cmp >= 0;
        };
    }

    private static boolean applyBoolean(Operator op, boolean actual, boolean target) {
        return switch (op) {
            case EQ -> actual == target;
            case NOT_EQ -> actual != target;
            // RecordFilterEvaluator returns true for any non-null boolean on other ops.
            default -> true;
        };
    }

    private static boolean applyBinary(Operator op, byte[] actual, byte[] target, boolean signed) {
        int cmp = signed
                ? BinaryComparator.compareSigned(actual, target)
                : BinaryComparator.compareUnsigned(actual, target);
        return switch (op) {
            case EQ -> cmp == 0;
            case NOT_EQ -> cmp != 0;
            case LT -> cmp < 0;
            case LT_EQ -> cmp <= 0;
            case GT -> cmp > 0;
            case GT_EQ -> cmp >= 0;
        };
    }

    private static void assertEquivalent(ResolvedPredicate predicate, StructAccessor row,
            FileSchema schema, ProjectedSchema projection, boolean expected) {
        boolean legacy = RecordFilterEvaluator.matchesRow(predicate, row, schema);
        boolean compiledName = RecordFilterCompiler.compile(predicate, schema).test(row);
        boolean compiledIndexed = RecordFilterCompiler.compile(predicate, schema, projection::toProjectedIndex).test(row);
        assertThat(compiledName).as("legacy/compiled-name disagreed for %s", predicate).isEqualTo(legacy);
        assertThat(compiledIndexed).as("legacy/compiled-indexed disagreed for %s", predicate).isEqualTo(legacy);
        assertThat(legacy).as("legacy oracle disagreed with expected for %s", predicate).isEqualTo(expected);
    }

    private static ProjectedSchema projectAll(FileSchema schema) {
        return ProjectedSchema.create(schema, ColumnProjection.all());
    }

    // ==================== schemas ====================

    private static FileSchema twoLongSchema(String n1, String n2) {
        return twoColumnSchema(n1, PhysicalType.INT64, n2, PhysicalType.INT64);
    }

    private static FileSchema twoIntSchema(String n1, String n2) {
        return twoColumnSchema(n1, PhysicalType.INT32, n2, PhysicalType.INT32);
    }

    private static FileSchema twoDoubleSchema(String n1, String n2) {
        return twoColumnSchema(n1, PhysicalType.DOUBLE, n2, PhysicalType.DOUBLE);
    }

    private static FileSchema twoBooleanSchema(String n1, String n2) {
        return twoColumnSchema(n1, PhysicalType.BOOLEAN, n2, PhysicalType.BOOLEAN);
    }

    private static FileSchema twoBinarySchema(String n1, String n2) {
        return twoColumnSchema(n1, PhysicalType.BYTE_ARRAY, n2, PhysicalType.BYTE_ARRAY);
    }

    private static FileSchema longDoubleSchema(String n1, String n2) {
        return twoColumnSchema(n1, PhysicalType.INT64, n2, PhysicalType.DOUBLE);
    }

    private static FileSchema intLongSchema(String n1, String n2) {
        return twoColumnSchema(n1, PhysicalType.INT32, n2, PhysicalType.INT64);
    }

    private static FileSchema intDoubleSchema(String n1, String n2) {
        return twoColumnSchema(n1, PhysicalType.INT32, n2, PhysicalType.DOUBLE);
    }

    private static FileSchema twoColumnSchema(String n1, PhysicalType t1, String n2, PhysicalType t2) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(n1, t1, null, RepetitionType.OPTIONAL, null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(n2, t2, null, RepetitionType.OPTIONAL, null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2));
    }

    private static FileSchema nestedTwoLongSchema() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement outer = new SchemaElement("outer", null, null, RepetitionType.OPTIONAL, 2, null, null, null, null, null);
        SchemaElement a = new SchemaElement("a", PhysicalType.INT64, null, RepetitionType.OPTIONAL, null, null, null, null, null, null);
        SchemaElement b = new SchemaElement("b", PhysicalType.INT64, null, RepetitionType.OPTIONAL, null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, outer, a, b));
    }

    // ==================== row stubs ====================

    private static final class TwoLongRow extends BaseRow {
        private final long v0;
        private final long v1;
        TwoLongRow(long v0, boolean n0, long v1, boolean n1) {
            super(new String[] { "a", "b" }, new boolean[] { n0, n1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public long getLong(String name) { return getLong(indexOf(name)); }
        @Override public long getLong(int idx) { return idx == 0 ? v0 : v1; }
    }

    private static final class TwoIntRow extends BaseRow {
        private final int v0;
        private final int v1;
        TwoIntRow(int v0, boolean n0, int v1, boolean n1) {
            super(new String[] { "a", "b" }, new boolean[] { n0, n1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public int getInt(String name) { return getInt(indexOf(name)); }
        @Override public int getInt(int idx) { return idx == 0 ? v0 : v1; }
    }

    private static final class TwoDoubleRow extends BaseRow {
        private final double v0;
        private final double v1;
        TwoDoubleRow(double v0, boolean n0, double v1, boolean n1) {
            super(new String[] { "a", "b" }, new boolean[] { n0, n1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public double getDouble(String name) { return getDouble(indexOf(name)); }
        @Override public double getDouble(int idx) { return idx == 0 ? v0 : v1; }
    }

    private static final class TwoBooleanRow extends BaseRow {
        private final boolean v0;
        private final boolean v1;
        TwoBooleanRow(boolean v0, boolean n0, boolean v1, boolean n1) {
            super(new String[] { "a", "b" }, new boolean[] { n0, n1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public boolean getBoolean(String name) { return getBoolean(indexOf(name)); }
        @Override public boolean getBoolean(int idx) { return idx == 0 ? v0 : v1; }
    }

    private static final class TwoBinaryRow extends BaseRow {
        private final byte[] v0;
        private final byte[] v1;
        TwoBinaryRow(byte[] v0, boolean n0, byte[] v1, boolean n1) {
            super(new String[] { "a", "b" }, new boolean[] { n0, n1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public byte[] getBinary(String name) { return getBinary(indexOf(name)); }
        @Override public byte[] getBinary(int idx) { return idx == 0 ? v0 : v1; }
    }

    private static final class LongDoubleRow extends BaseRow {
        private final long v0;
        private final double v1;
        LongDoubleRow(long v0, boolean n0, double v1, boolean n1) {
            super(new String[] { "a", "b" }, new boolean[] { n0, n1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public long getLong(String name) { return v0; }
        @Override public long getLong(int idx) { return v0; }
        @Override public double getDouble(String name) { return v1; }
        @Override public double getDouble(int idx) { return v1; }
    }

    private static final class IntLongRow extends BaseRow {
        private final int v0;
        private final long v1;
        IntLongRow(int v0, boolean n0, long v1, boolean n1) {
            super(new String[] { "a", "b" }, new boolean[] { n0, n1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public int getInt(String name) { return v0; }
        @Override public int getInt(int idx) { return v0; }
        @Override public long getLong(String name) { return v1; }
        @Override public long getLong(int idx) { return v1; }
    }

    private static final class IntDoubleRow extends BaseRow {
        private final int v0;
        private final double v1;
        IntDoubleRow(int v0, boolean n0, double v1, boolean n1) {
            super(new String[] { "a", "b" }, new boolean[] { n0, n1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public int getInt(String name) { return v0; }
        @Override public int getInt(int idx) { return v0; }
        @Override public double getDouble(String name) { return v1; }
        @Override public double getDouble(int idx) { return v1; }
    }

    /// Row stub for `outer.a` and `outer.b` nested struct path. The top-level
    /// `outer` field returns this same row in order to keep things simple.
    private static final class NestedTwoLongRow extends BaseRow {
        private final long v0;
        private final long v1;
        private final boolean n0;
        private final boolean n1;
        NestedTwoLongRow(long v0, boolean n0, long v1, boolean n1) {
            super(new String[] { "a", "b" }, new boolean[] { n0, n1 });
            this.v0 = v0;
            this.v1 = v1;
            this.n0 = n0;
            this.n1 = n1;
        }
        @Override public long getLong(String name) { return name.equals("a") ? v0 : v1; }
        @Override public boolean isNull(String name) {
            // For "outer", report not-null and return self via getStruct.
            if (name.equals("outer")) return false;
            return name.equals("a") ? n0 : n1;
        }
        @Override public PqStruct getStruct(String name) {
            // Build a pq-struct view that wraps this row. The simplest path is to
            // delegate by returning a thin wrapper — but RecordFilterEvaluator's
            // resolve() walks via getStruct(name) for intermediate steps and
            // expects a StructAccessor. PqStruct extends StructAccessor in this
            // codebase, so a dedicated subclass works.
            return new NestedStructView(this);
        }
    }

    private static final class NestedStructView implements PqStruct {
        private final NestedTwoLongRow inner;
        NestedStructView(NestedTwoLongRow inner) { this.inner = inner; }
        @Override public boolean isNull(String name) { return inner.isNull(name); }
        @Override public long getLong(String name) { return inner.getLong(name); }
        @Override public int getInt(String name) { throw new UnsupportedOperationException(); }
        @Override public float getFloat(String name) { throw new UnsupportedOperationException(); }
        @Override public double getDouble(String name) { throw new UnsupportedOperationException(); }
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
        @Override public int getFieldCount() { return 2; }
        @Override public String getFieldName(int index) { return index == 0 ? "a" : "b"; }
    }

    private abstract static class BaseRow implements RowReader {
        private final String[] names;
        private final boolean[] nulls;

        BaseRow(String[] names, boolean[] nulls) {
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
        @Override public boolean isNull(int idx) { return nulls[idx]; }

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
