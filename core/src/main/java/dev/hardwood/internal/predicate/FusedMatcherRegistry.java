/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.function.IntUnaryOperator;

import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.schema.FileSchema;

/// Maps a canonicalised arity-2 AND/OR shape to a build-time generated
/// fused matcher class. Returns `null` when the tuple is outside the
/// matrix supported by the annotation processor — callers fall back to
/// the generic [And2Matcher] / [Or2Matcher].
///
/// Both children must reference the same column and the same primitive
/// type. Cross-type, diff-column, binary, boolean, and float compounds
/// are deliberately uncovered here; future runtime-codegen overlays (see
/// `RECORD_FILTER_FUSION_BYTECODE.md`) extend the surface dynamically.
final class FusedMatcherRegistry {

    private FusedMatcherRegistry() {
    }

    static RowMatcher lookup(ResolvedPredicate a, ResolvedPredicate b,
            FileSchema schema, IntUnaryOperator topLevelFieldIndex,
            boolean isAnd) {
        if (a instanceof ResolvedPredicate.IntPredicate ai
                && b instanceof ResolvedPredicate.IntPredicate bi) {
            return lookupInt(ai, bi, schema, topLevelFieldIndex, isAnd);
        }
        if (a instanceof ResolvedPredicate.LongPredicate al
                && b instanceof ResolvedPredicate.LongPredicate bl) {
            return lookupLong(al, bl, schema, topLevelFieldIndex, isAnd);
        }
        if (a instanceof ResolvedPredicate.DoublePredicate ad
                && b instanceof ResolvedPredicate.DoublePredicate bd) {
            return lookupDouble(ad, bd, schema, topLevelFieldIndex, isAnd);
        }
        return null;
    }

    // ==================== Int ====================

    private static RowMatcher lookupInt(ResolvedPredicate.IntPredicate a,
            ResolvedPredicate.IntPredicate b, FileSchema schema,
            IntUnaryOperator topLevelFieldIndex, boolean isAnd) {
        int col = a.columnIndex();
        if (col != b.columnIndex()) {
            return null;
        }
        int idx = RecordFilterCompiler.indexedTopLevel(schema, col, topLevelFieldIndex);
        if (idx >= 0) {
            RowMatcher m = matchIntIndexed(idx, a.op(), a.value(), b.op(), b.value(), isAnd);
            if (m != null) return m;
            return matchIntIndexed(idx, b.op(), b.value(), a.op(), a.value(), isAnd);
        }
        String[] path = RecordFilterCompiler.pathSegments(schema, col);
        String name = RecordFilterCompiler.leafName(schema, col);
        RowMatcher m = matchIntNamed(path, name, a.op(), a.value(), b.op(), b.value(), isAnd);
        if (m != null) return m;
        return matchIntNamed(path, name, b.op(), b.value(), a.op(), a.value(), isAnd);
    }

    private static RowMatcher matchIntIndexed(int idx, Operator opA, int va, Operator opB, int vb, boolean isAnd) {
        if (isAnd) {
            if (opA == Operator.GT_EQ && opB == Operator.LT_EQ) return new FusedIntAndCsCs_GteLte(idx, va, vb);
            if (opA == Operator.GT && opB == Operator.LT) return new FusedIntAndCsCs_GtLt(idx, va, vb);
            if (opA == Operator.GT_EQ && opB == Operator.LT) return new FusedIntAndCsCs_GteLt(idx, va, vb);
            if (opA == Operator.GT && opB == Operator.LT_EQ) return new FusedIntAndCsCs_GtLte(idx, va, vb);
        } else {
            if (opA == Operator.LT && opB == Operator.GT) return new FusedIntOrCsCs_LtGt(idx, va, vb);
            if (opA == Operator.LT_EQ && opB == Operator.GT_EQ) return new FusedIntOrCsCs_LteGte(idx, va, vb);
            if (opA == Operator.EQ && opB == Operator.EQ) return new FusedIntOrCsCs_EqEq(idx, va, vb);
        }
        return null;
    }

    private static RowMatcher matchIntNamed(String[] path, String name,
            Operator opA, int va, Operator opB, int vb, boolean isAnd) {
        if (isAnd) {
            if (opA == Operator.GT_EQ && opB == Operator.LT_EQ) return new FusedIntAndCsCs_GteLte_Named(path, name, va, vb);
            if (opA == Operator.GT && opB == Operator.LT) return new FusedIntAndCsCs_GtLt_Named(path, name, va, vb);
            if (opA == Operator.GT_EQ && opB == Operator.LT) return new FusedIntAndCsCs_GteLt_Named(path, name, va, vb);
            if (opA == Operator.GT && opB == Operator.LT_EQ) return new FusedIntAndCsCs_GtLte_Named(path, name, va, vb);
        } else {
            if (opA == Operator.LT && opB == Operator.GT) return new FusedIntOrCsCs_LtGt_Named(path, name, va, vb);
            if (opA == Operator.LT_EQ && opB == Operator.GT_EQ) return new FusedIntOrCsCs_LteGte_Named(path, name, va, vb);
            if (opA == Operator.EQ && opB == Operator.EQ) return new FusedIntOrCsCs_EqEq_Named(path, name, va, vb);
        }
        return null;
    }

    // ==================== Long ====================

    private static RowMatcher lookupLong(ResolvedPredicate.LongPredicate a,
            ResolvedPredicate.LongPredicate b, FileSchema schema,
            IntUnaryOperator topLevelFieldIndex, boolean isAnd) {
        int col = a.columnIndex();
        if (col != b.columnIndex()) {
            return null;
        }
        int idx = RecordFilterCompiler.indexedTopLevel(schema, col, topLevelFieldIndex);
        if (idx >= 0) {
            RowMatcher m = matchLongIndexed(idx, a.op(), a.value(), b.op(), b.value(), isAnd);
            if (m != null) return m;
            return matchLongIndexed(idx, b.op(), b.value(), a.op(), a.value(), isAnd);
        }
        String[] path = RecordFilterCompiler.pathSegments(schema, col);
        String name = RecordFilterCompiler.leafName(schema, col);
        RowMatcher m = matchLongNamed(path, name, a.op(), a.value(), b.op(), b.value(), isAnd);
        if (m != null) return m;
        return matchLongNamed(path, name, b.op(), b.value(), a.op(), a.value(), isAnd);
    }

    private static RowMatcher matchLongIndexed(int idx, Operator opA, long va, Operator opB, long vb, boolean isAnd) {
        if (isAnd) {
            if (opA == Operator.GT_EQ && opB == Operator.LT_EQ) return new FusedLongAndCsCs_GteLte(idx, va, vb);
            if (opA == Operator.GT && opB == Operator.LT) return new FusedLongAndCsCs_GtLt(idx, va, vb);
            if (opA == Operator.GT_EQ && opB == Operator.LT) return new FusedLongAndCsCs_GteLt(idx, va, vb);
            if (opA == Operator.GT && opB == Operator.LT_EQ) return new FusedLongAndCsCs_GtLte(idx, va, vb);
        } else {
            if (opA == Operator.LT && opB == Operator.GT) return new FusedLongOrCsCs_LtGt(idx, va, vb);
            if (opA == Operator.LT_EQ && opB == Operator.GT_EQ) return new FusedLongOrCsCs_LteGte(idx, va, vb);
            if (opA == Operator.EQ && opB == Operator.EQ) return new FusedLongOrCsCs_EqEq(idx, va, vb);
        }
        return null;
    }

    private static RowMatcher matchLongNamed(String[] path, String name,
            Operator opA, long va, Operator opB, long vb, boolean isAnd) {
        if (isAnd) {
            if (opA == Operator.GT_EQ && opB == Operator.LT_EQ) return new FusedLongAndCsCs_GteLte_Named(path, name, va, vb);
            if (opA == Operator.GT && opB == Operator.LT) return new FusedLongAndCsCs_GtLt_Named(path, name, va, vb);
            if (opA == Operator.GT_EQ && opB == Operator.LT) return new FusedLongAndCsCs_GteLt_Named(path, name, va, vb);
            if (opA == Operator.GT && opB == Operator.LT_EQ) return new FusedLongAndCsCs_GtLte_Named(path, name, va, vb);
        } else {
            if (opA == Operator.LT && opB == Operator.GT) return new FusedLongOrCsCs_LtGt_Named(path, name, va, vb);
            if (opA == Operator.LT_EQ && opB == Operator.GT_EQ) return new FusedLongOrCsCs_LteGte_Named(path, name, va, vb);
            if (opA == Operator.EQ && opB == Operator.EQ) return new FusedLongOrCsCs_EqEq_Named(path, name, va, vb);
        }
        return null;
    }

    // ==================== Double ====================

    private static RowMatcher lookupDouble(ResolvedPredicate.DoublePredicate a,
            ResolvedPredicate.DoublePredicate b, FileSchema schema,
            IntUnaryOperator topLevelFieldIndex, boolean isAnd) {
        int col = a.columnIndex();
        if (col != b.columnIndex()) {
            return null;
        }
        int idx = RecordFilterCompiler.indexedTopLevel(schema, col, topLevelFieldIndex);
        if (idx >= 0) {
            RowMatcher m = matchDoubleIndexed(idx, a.op(), a.value(), b.op(), b.value(), isAnd);
            if (m != null) return m;
            return matchDoubleIndexed(idx, b.op(), b.value(), a.op(), a.value(), isAnd);
        }
        String[] path = RecordFilterCompiler.pathSegments(schema, col);
        String name = RecordFilterCompiler.leafName(schema, col);
        RowMatcher m = matchDoubleNamed(path, name, a.op(), a.value(), b.op(), b.value(), isAnd);
        if (m != null) return m;
        return matchDoubleNamed(path, name, b.op(), b.value(), a.op(), a.value(), isAnd);
    }

    private static RowMatcher matchDoubleIndexed(int idx, Operator opA, double va, Operator opB, double vb, boolean isAnd) {
        if (isAnd) {
            if (opA == Operator.GT_EQ && opB == Operator.LT_EQ) return new FusedDoubleAndCsCs_GteLte(idx, va, vb);
            if (opA == Operator.GT && opB == Operator.LT) return new FusedDoubleAndCsCs_GtLt(idx, va, vb);
            if (opA == Operator.GT_EQ && opB == Operator.LT) return new FusedDoubleAndCsCs_GteLt(idx, va, vb);
            if (opA == Operator.GT && opB == Operator.LT_EQ) return new FusedDoubleAndCsCs_GtLte(idx, va, vb);
        } else {
            if (opA == Operator.LT && opB == Operator.GT) return new FusedDoubleOrCsCs_LtGt(idx, va, vb);
            if (opA == Operator.LT_EQ && opB == Operator.GT_EQ) return new FusedDoubleOrCsCs_LteGte(idx, va, vb);
        }
        return null;
    }

    private static RowMatcher matchDoubleNamed(String[] path, String name,
            Operator opA, double va, Operator opB, double vb, boolean isAnd) {
        if (isAnd) {
            if (opA == Operator.GT_EQ && opB == Operator.LT_EQ) return new FusedDoubleAndCsCs_GteLte_Named(path, name, va, vb);
            if (opA == Operator.GT && opB == Operator.LT) return new FusedDoubleAndCsCs_GtLt_Named(path, name, va, vb);
            if (opA == Operator.GT_EQ && opB == Operator.LT) return new FusedDoubleAndCsCs_GteLt_Named(path, name, va, vb);
            if (opA == Operator.GT && opB == Operator.LT_EQ) return new FusedDoubleAndCsCs_GtLte_Named(path, name, va, vb);
        } else {
            if (opA == Operator.LT && opB == Operator.GT) return new FusedDoubleOrCsCs_LtGt_Named(path, name, va, vb);
            if (opA == Operator.LT_EQ && opB == Operator.GT_EQ) return new FusedDoubleOrCsCs_LteGte_Named(path, name, va, vb);
        }
        return null;
    }
}
