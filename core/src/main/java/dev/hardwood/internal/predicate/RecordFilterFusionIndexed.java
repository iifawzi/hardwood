/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.reader.IndexedAccessor;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Indexed-access variant of [RecordFilterFusion]. Fused matchers use
/// `IndexedAccessor.isNullAt(int)` / `getLongAt(int)` etc. with projected
/// field indices baked in at compile time, bypassing the name → index
/// hash lookup performed by the name-keyed [StructAccessor] interface.
///
/// Used by [RecordFilterCompiler#compile(ResolvedPredicate, FileSchema, ProjectedSchema)]
/// — the entry point that the flat row reader calls — when both children
/// of an arity-2 AND are simple primitive leaves on top-level columns.
/// The cast to [IndexedAccessor] is safe because the indexed compile path
/// is only invoked from readers that guarantee the row implements it
/// (i.e. [dev.hardwood.internal.reader.FlatRowReader]).
final class RecordFilterFusionIndexed {

    private RecordFilterFusionIndexed() {
    }

    static RowMatcher tryFuseAnd2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema,
            ProjectedSchema projection) {
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.LongPredicate lb
                && isTopLevel(schema, la.columnIndex()) && isTopLevel(schema, lb.columnIndex())) {
            int iA = projection.toProjectedIndex(la.columnIndex());
            int iB = projection.toProjectedIndex(lb.columnIndex());
            if (la.columnIndex() == lb.columnIndex()) {
                return longRange(iA, la.op(), la.value(), lb.op(), lb.value());
            }
            return longLongDiff(iA, la.op(), la.value(), iB, lb.op(), lb.value());
        }
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.IntPredicate ib
                && isTopLevel(schema, ia.columnIndex()) && isTopLevel(schema, ib.columnIndex())) {
            int iA = projection.toProjectedIndex(ia.columnIndex());
            int iB = projection.toProjectedIndex(ib.columnIndex());
            if (ia.columnIndex() == ib.columnIndex()) {
                return intRange(iA, ia.op(), ia.value(), ib.op(), ib.value());
            }
            return intIntDiff(iA, ia.op(), ia.value(), iB, ib.op(), ib.value());
        }
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.DoublePredicate db
                && isTopLevel(schema, da.columnIndex()) && isTopLevel(schema, db.columnIndex())) {
            int iA = projection.toProjectedIndex(da.columnIndex());
            int iB = projection.toProjectedIndex(db.columnIndex());
            if (da.columnIndex() == db.columnIndex()) {
                return doubleRange(iA, da.op(), da.value(), db.op(), db.value());
            }
            return doubleDoubleDiff(iA, da.op(), da.value(), iB, db.op(), db.value());
        }
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.DoublePredicate db
                && isTopLevel(schema, la.columnIndex()) && isTopLevel(schema, db.columnIndex())) {
            int iA = projection.toProjectedIndex(la.columnIndex());
            int iB = projection.toProjectedIndex(db.columnIndex());
            return longDoubleDiff(iA, la.op(), la.value(), iB, db.op(), db.value());
        }
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.LongPredicate lb
                && isTopLevel(schema, da.columnIndex()) && isTopLevel(schema, lb.columnIndex())) {
            // canonical-swap (AND-commutative for pure leaves)
            int iA = projection.toProjectedIndex(lb.columnIndex());
            int iB = projection.toProjectedIndex(da.columnIndex());
            return longDoubleDiff(iA, lb.op(), lb.value(), iB, da.op(), da.value());
        }
        return null;
    }

    private static boolean isTopLevel(FileSchema schema, int columnIndex) {
        return schema.getColumn(columnIndex).fieldPath().elements().size() <= 1;
    }

    // ==================== Long + Long, different columns ====================

    private static RowMatcher longLongDiff(int iA, Operator opA, long vA, int iB, Operator opB, long vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) == vA && !a.isNullAt(iB) && a.getLongAt(iB) == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) == vA && !a.isNullAt(iB) && a.getLongAt(iB) != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) == vA && !a.isNullAt(iB) && a.getLongAt(iB) < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) == vA && !a.isNullAt(iB) && a.getLongAt(iB) <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) == vA && !a.isNullAt(iB) && a.getLongAt(iB) > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) == vA && !a.isNullAt(iB) && a.getLongAt(iB) >= vB; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) != vA && !a.isNullAt(iB) && a.getLongAt(iB) == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) != vA && !a.isNullAt(iB) && a.getLongAt(iB) != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) != vA && !a.isNullAt(iB) && a.getLongAt(iB) < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) != vA && !a.isNullAt(iB) && a.getLongAt(iB) <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) != vA && !a.isNullAt(iB) && a.getLongAt(iB) > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) != vA && !a.isNullAt(iB) && a.getLongAt(iB) >= vB; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) < vA && !a.isNullAt(iB) && a.getLongAt(iB) == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) < vA && !a.isNullAt(iB) && a.getLongAt(iB) != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) < vA && !a.isNullAt(iB) && a.getLongAt(iB) < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) < vA && !a.isNullAt(iB) && a.getLongAt(iB) <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) < vA && !a.isNullAt(iB) && a.getLongAt(iB) > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) < vA && !a.isNullAt(iB) && a.getLongAt(iB) >= vB; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) <= vA && !a.isNullAt(iB) && a.getLongAt(iB) == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) <= vA && !a.isNullAt(iB) && a.getLongAt(iB) != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) <= vA && !a.isNullAt(iB) && a.getLongAt(iB) < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) <= vA && !a.isNullAt(iB) && a.getLongAt(iB) <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) <= vA && !a.isNullAt(iB) && a.getLongAt(iB) > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) <= vA && !a.isNullAt(iB) && a.getLongAt(iB) >= vB; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) > vA && !a.isNullAt(iB) && a.getLongAt(iB) == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) > vA && !a.isNullAt(iB) && a.getLongAt(iB) != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) > vA && !a.isNullAt(iB) && a.getLongAt(iB) < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) > vA && !a.isNullAt(iB) && a.getLongAt(iB) <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) > vA && !a.isNullAt(iB) && a.getLongAt(iB) > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) > vA && !a.isNullAt(iB) && a.getLongAt(iB) >= vB; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) >= vA && !a.isNullAt(iB) && a.getLongAt(iB) == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) >= vA && !a.isNullAt(iB) && a.getLongAt(iB) != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) >= vA && !a.isNullAt(iB) && a.getLongAt(iB) < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) >= vA && !a.isNullAt(iB) && a.getLongAt(iB) <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) >= vA && !a.isNullAt(iB) && a.getLongAt(iB) > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) >= vA && !a.isNullAt(iB) && a.getLongAt(iB) >= vB; };
            };
        };
    }

    // ==================== Long, same column (range) ====================

    private static RowMatcher longRange(int idx, Operator opA, long vA, Operator opB, long vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v == vA && v == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v == vA && v != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v == vA && v < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v == vA && v <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v == vA && v > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v == vA && v >= vB; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v != vA && v == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v != vA && v != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v != vA && v < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v != vA && v <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v != vA && v > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v != vA && v >= vB; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v < vA && v == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v < vA && v != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v < vA && v < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v < vA && v <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v < vA && v > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v < vA && v >= vB; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v <= vA && v == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v <= vA && v != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v <= vA && v < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v <= vA && v <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v <= vA && v > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v <= vA && v >= vB; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v > vA && v == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v > vA && v != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v > vA && v < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v > vA && v <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v > vA && v > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v > vA && v >= vB; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v >= vA && v == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v >= vA && v != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v >= vA && v < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v >= vA && v <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v >= vA && v > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; long v = a.getLongAt(idx); return v >= vA && v >= vB; };
            };
        };
    }

    // ==================== Int + Int, different columns ====================

    private static RowMatcher intIntDiff(int iA, Operator opA, int vA, int iB, Operator opB, int vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) == vA && !a.isNullAt(iB) && a.getIntAt(iB) == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) == vA && !a.isNullAt(iB) && a.getIntAt(iB) != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) == vA && !a.isNullAt(iB) && a.getIntAt(iB) < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) == vA && !a.isNullAt(iB) && a.getIntAt(iB) <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) == vA && !a.isNullAt(iB) && a.getIntAt(iB) > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) == vA && !a.isNullAt(iB) && a.getIntAt(iB) >= vB; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) != vA && !a.isNullAt(iB) && a.getIntAt(iB) == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) != vA && !a.isNullAt(iB) && a.getIntAt(iB) != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) != vA && !a.isNullAt(iB) && a.getIntAt(iB) < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) != vA && !a.isNullAt(iB) && a.getIntAt(iB) <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) != vA && !a.isNullAt(iB) && a.getIntAt(iB) > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) != vA && !a.isNullAt(iB) && a.getIntAt(iB) >= vB; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) < vA && !a.isNullAt(iB) && a.getIntAt(iB) == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) < vA && !a.isNullAt(iB) && a.getIntAt(iB) != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) < vA && !a.isNullAt(iB) && a.getIntAt(iB) < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) < vA && !a.isNullAt(iB) && a.getIntAt(iB) <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) < vA && !a.isNullAt(iB) && a.getIntAt(iB) > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) < vA && !a.isNullAt(iB) && a.getIntAt(iB) >= vB; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) <= vA && !a.isNullAt(iB) && a.getIntAt(iB) == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) <= vA && !a.isNullAt(iB) && a.getIntAt(iB) != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) <= vA && !a.isNullAt(iB) && a.getIntAt(iB) < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) <= vA && !a.isNullAt(iB) && a.getIntAt(iB) <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) <= vA && !a.isNullAt(iB) && a.getIntAt(iB) > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) <= vA && !a.isNullAt(iB) && a.getIntAt(iB) >= vB; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) > vA && !a.isNullAt(iB) && a.getIntAt(iB) == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) > vA && !a.isNullAt(iB) && a.getIntAt(iB) != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) > vA && !a.isNullAt(iB) && a.getIntAt(iB) < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) > vA && !a.isNullAt(iB) && a.getIntAt(iB) <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) > vA && !a.isNullAt(iB) && a.getIntAt(iB) > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) > vA && !a.isNullAt(iB) && a.getIntAt(iB) >= vB; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) >= vA && !a.isNullAt(iB) && a.getIntAt(iB) == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) >= vA && !a.isNullAt(iB) && a.getIntAt(iB) != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) >= vA && !a.isNullAt(iB) && a.getIntAt(iB) < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) >= vA && !a.isNullAt(iB) && a.getIntAt(iB) <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) >= vA && !a.isNullAt(iB) && a.getIntAt(iB) > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getIntAt(iA) >= vA && !a.isNullAt(iB) && a.getIntAt(iB) >= vB; };
            };
        };
    }

    // ==================== Int, same column (range) ====================

    private static RowMatcher intRange(int idx, Operator opA, int vA, Operator opB, int vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v == vA && v == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v == vA && v != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v == vA && v < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v == vA && v <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v == vA && v > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v == vA && v >= vB; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v != vA && v == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v != vA && v != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v != vA && v < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v != vA && v <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v != vA && v > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v != vA && v >= vB; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v < vA && v == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v < vA && v != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v < vA && v < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v < vA && v <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v < vA && v > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v < vA && v >= vB; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v <= vA && v == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v <= vA && v != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v <= vA && v < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v <= vA && v <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v <= vA && v > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v <= vA && v >= vB; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v > vA && v == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v > vA && v != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v > vA && v < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v > vA && v <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v > vA && v > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v > vA && v >= vB; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v >= vA && v == vB; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v >= vA && v != vB; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v >= vA && v < vB; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v >= vA && v <= vB; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v >= vA && v > vB; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; int v = a.getIntAt(idx); return v >= vA && v >= vB; };
            };
        };
    }

    // ==================== Double + Double, different columns ====================

    private static RowMatcher doubleDoubleDiff(int iA, Operator opA, double vA, int iB, Operator opB, double vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) == 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) == 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) == 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) == 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) == 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) == 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) >= 0; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) != 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) != 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) != 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) != 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) != 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) != 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) >= 0; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) < 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) < 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) < 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) < 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) < 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) < 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) >= 0; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) <= 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) <= 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) <= 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) <= 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) <= 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) <= 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) >= 0; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) > 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) > 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) > 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) > 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) > 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) > 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) >= 0; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) >= 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) >= 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) >= 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) >= 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) >= 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && Double.compare(a.getDoubleAt(iA), vA) >= 0 && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) >= 0; };
            };
        };
    }

    // ==================== Double, same column (range) ====================

    private static RowMatcher doubleRange(int idx, Operator opA, double vA, Operator opB, double vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) == 0 && Double.compare(v, vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) == 0 && Double.compare(v, vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) == 0 && Double.compare(v, vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) == 0 && Double.compare(v, vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) == 0 && Double.compare(v, vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) == 0 && Double.compare(v, vB) >= 0; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) != 0 && Double.compare(v, vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) != 0 && Double.compare(v, vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) != 0 && Double.compare(v, vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) != 0 && Double.compare(v, vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) != 0 && Double.compare(v, vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) != 0 && Double.compare(v, vB) >= 0; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) < 0 && Double.compare(v, vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) < 0 && Double.compare(v, vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) < 0 && Double.compare(v, vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) < 0 && Double.compare(v, vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) < 0 && Double.compare(v, vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) < 0 && Double.compare(v, vB) >= 0; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) <= 0 && Double.compare(v, vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) <= 0 && Double.compare(v, vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) <= 0 && Double.compare(v, vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) <= 0 && Double.compare(v, vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) <= 0 && Double.compare(v, vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) <= 0 && Double.compare(v, vB) >= 0; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) > 0 && Double.compare(v, vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) > 0 && Double.compare(v, vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) > 0 && Double.compare(v, vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) > 0 && Double.compare(v, vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) > 0 && Double.compare(v, vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) > 0 && Double.compare(v, vB) >= 0; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) >= 0 && Double.compare(v, vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) >= 0 && Double.compare(v, vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) >= 0 && Double.compare(v, vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) >= 0 && Double.compare(v, vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) >= 0 && Double.compare(v, vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; if (a.isNullAt(idx)) return false; double v = a.getDoubleAt(idx); return Double.compare(v, vA) >= 0 && Double.compare(v, vB) >= 0; };
            };
        };
    }

    // ==================== Long + Double, different columns ====================

    private static RowMatcher longDoubleDiff(int iA, Operator opA, long vA, int iB, Operator opB, double vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) == vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) == vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) == vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) == vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) == vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) == vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) >= 0; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) != vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) != vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) != vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) != vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) != vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) != vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) >= 0; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) < vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) < vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) < vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) < vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) < vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) < vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) >= 0; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) <= vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) <= vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) <= vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) <= vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) <= vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) <= vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) >= 0; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) > vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) > vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) > vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) > vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) > vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) > vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) >= 0; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) >= vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) == 0; };
                case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) >= vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) != 0; };
                case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) >= vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) < 0; };
                case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) >= vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) <= 0; };
                case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) >= vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) > 0; };
                case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(iA) && a.getLongAt(iA) >= vA && !a.isNullAt(iB) && Double.compare(a.getDoubleAt(iB), vB) >= 0; };
            };
        };
    }

    /// Single-leaf indexed factories. Used by the indexed compile path for
    /// top-level primitive predicates that fall outside the 2-arity AND
    /// fusion route (any compound shape arity ≠ 2, or non-numeric leaves).

    static RowMatcher intLeaf(int idx, Operator op, int v) {
        return switch (op) {
            case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getIntAt(idx) == v; };
            case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getIntAt(idx) != v; };
            case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getIntAt(idx) < v; };
            case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getIntAt(idx) <= v; };
            case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getIntAt(idx) > v; };
            case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getIntAt(idx) >= v; };
        };
    }

    static RowMatcher longLeaf(int idx, Operator op, long v) {
        return switch (op) {
            case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getLongAt(idx) == v; };
            case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getLongAt(idx) != v; };
            case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getLongAt(idx) < v; };
            case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getLongAt(idx) <= v; };
            case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getLongAt(idx) > v; };
            case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getLongAt(idx) >= v; };
        };
    }

    static RowMatcher floatLeaf(int idx, Operator op, float v) {
        return switch (op) {
            case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Float.compare(a.getFloatAt(idx), v) == 0; };
            case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Float.compare(a.getFloatAt(idx), v) != 0; };
            case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Float.compare(a.getFloatAt(idx), v) < 0; };
            case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Float.compare(a.getFloatAt(idx), v) <= 0; };
            case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Float.compare(a.getFloatAt(idx), v) > 0; };
            case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Float.compare(a.getFloatAt(idx), v) >= 0; };
        };
    }

    static RowMatcher doubleLeaf(int idx, Operator op, double v) {
        return switch (op) {
            case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Double.compare(a.getDoubleAt(idx), v) == 0; };
            case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Double.compare(a.getDoubleAt(idx), v) != 0; };
            case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Double.compare(a.getDoubleAt(idx), v) < 0; };
            case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Double.compare(a.getDoubleAt(idx), v) <= 0; };
            case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Double.compare(a.getDoubleAt(idx), v) > 0; };
            case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Double.compare(a.getDoubleAt(idx), v) >= 0; };
        };
    }

    static RowMatcher booleanLeaf(int idx, Operator op, boolean v) {
        return switch (op) {
            case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getBooleanAt(idx) == v; };
            case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getBooleanAt(idx) != v; };
            default -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx); };
        };
    }

    static RowMatcher isNullLeaf(int idx) {
        return row -> ((IndexedAccessor) row).isNullAt(idx);
    }

    static RowMatcher isNotNullLeaf(int idx) {
        return row -> !((IndexedAccessor) row).isNullAt(idx);
    }
}
