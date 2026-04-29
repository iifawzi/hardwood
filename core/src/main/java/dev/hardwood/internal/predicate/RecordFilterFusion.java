/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.List;

import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;

/// Arity-2 AND fusion: when both children of an [ResolvedPredicate.And] are
/// simple primitive leaves of compatible types, the compiler emits a single
/// fused matcher whose body inlines both comparisons as primitive bytecode
/// operations — no inner virtual call in the row loop.
///
/// Each `(typeA, opA, typeB, opB)` produces a distinct synthetic lambda
/// class, so the fused matchers cannot be polluted by other shapes and
/// stay fast even when the generic [RecordFilterCompiler.And2Matcher]
/// call site goes megamorphic.
///
/// Supported type combinations (the bulk of realistic numeric predicates):
/// - long + long
/// - long + double (also double + long, AND-commutative swap)
/// - int + int
/// - double + double
///
/// Same-column variants (`col >= a AND col < b`, the BETWEEN pattern) for
/// every same-type pair resolve the path and load the value once.
final class RecordFilterFusion {

    private RecordFilterFusion() {
    }

    /// Returns a fused matcher for the two children, or null if the pair
    /// is not eligible for fusion (caller should fall back to the generic
    /// [RecordFilterCompiler.And2Matcher]).
    static RowMatcher tryFuseAnd2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema) {
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.LongPredicate lb) {
            return fuseLongLong(la, lb, schema);
        }
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.IntPredicate ib) {
            return fuseIntInt(ia, ib, schema);
        }
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.DoublePredicate db) {
            return fuseDoubleDouble(da, db, schema);
        }
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.DoublePredicate db) {
            return fuseLongDoubleDiff(la, db, schema);
        }
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.LongPredicate lb) {
            // AND is commutative for pure leaves; canonicalise to (long, double).
            return fuseLongDoubleDiff(lb, da, schema);
        }
        return null;
    }

    // ==================== Long + Long ====================

    private static RowMatcher fuseLongLong(ResolvedPredicate.LongPredicate la, ResolvedPredicate.LongPredicate lb,
            FileSchema schema) {
        String[] pathA = pathSegments(schema, la.columnIndex());
        String nameA = leafName(schema, la.columnIndex());
        if (la.columnIndex() == lb.columnIndex()) {
            return longRange(pathA, nameA, la.op(), la.value(), lb.op(), lb.value());
        }
        String[] pathB = pathSegments(schema, lb.columnIndex());
        String nameB = leafName(schema, lb.columnIndex());
        return longLongDiff(pathA, nameA, la.op(), la.value(), pathB, nameB, lb.op(), lb.value());
    }

    private static RowMatcher longLongDiff(String[] pA, String nA, Operator opA, long vA,
            String[] pB, String nB, Operator opB, long vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) == vB; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) != vB; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) < vB; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) <= vB; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) > vB; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) >= vB; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) == vB; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) != vB; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) < vB; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) <= vB; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) > vB; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) >= vB; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) == vB; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) != vB; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) < vB; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) <= vB; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) > vB; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) >= vB; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) == vB; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) != vB; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) < vB; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) <= vB; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) > vB; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) >= vB; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) == vB; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) != vB; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) < vB; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) <= vB; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) > vB; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) >= vB; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) == vB; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) != vB; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) < vB; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) <= vB; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) > vB; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getLong(nB) >= vB; };
            };
        };
    }

    private static RowMatcher longRange(String[] p, String n, Operator opA, long vA, Operator opB, long vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v == vA && v == vB; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v == vA && v != vB; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v == vA && v < vB; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v == vA && v <= vB; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v == vA && v > vB; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v == vA && v >= vB; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v != vA && v == vB; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v != vA && v != vB; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v != vA && v < vB; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v != vA && v <= vB; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v != vA && v > vB; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v != vA && v >= vB; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v < vA && v == vB; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v < vA && v != vB; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v < vA && v < vB; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v < vA && v <= vB; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v < vA && v > vB; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v < vA && v >= vB; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v <= vA && v == vB; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v <= vA && v != vB; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v <= vA && v < vB; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v <= vA && v <= vB; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v <= vA && v > vB; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v <= vA && v >= vB; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v > vA && v == vB; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v > vA && v != vB; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v > vA && v < vB; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v > vA && v <= vB; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v > vA && v > vB; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v > vA && v >= vB; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v >= vA && v == vB; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v >= vA && v != vB; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v >= vA && v < vB; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v >= vA && v <= vB; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v >= vA && v > vB; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; long v = s.getLong(n); return v >= vA && v >= vB; };
            };
        };
    }

    // ==================== Int + Int ====================

    private static RowMatcher fuseIntInt(ResolvedPredicate.IntPredicate ia, ResolvedPredicate.IntPredicate ib,
            FileSchema schema) {
        String[] pathA = pathSegments(schema, ia.columnIndex());
        String nameA = leafName(schema, ia.columnIndex());
        if (ia.columnIndex() == ib.columnIndex()) {
            return intRange(pathA, nameA, ia.op(), ia.value(), ib.op(), ib.value());
        }
        String[] pathB = pathSegments(schema, ib.columnIndex());
        String nameB = leafName(schema, ib.columnIndex());
        return intIntDiff(pathA, nameA, ia.op(), ia.value(), pathB, nameB, ib.op(), ib.value());
    }

    private static RowMatcher intIntDiff(String[] pA, String nA, Operator opA, int vA,
            String[] pB, String nB, Operator opB, int vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) == vB; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) != vB; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) < vB; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) <= vB; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) > vB; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) >= vB; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) == vB; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) != vB; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) < vB; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) <= vB; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) > vB; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) >= vB; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) == vB; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) != vB; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) < vB; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) <= vB; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) > vB; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) >= vB; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) == vB; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) != vB; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) < vB; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) <= vB; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) > vB; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) >= vB; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) == vB; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) != vB; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) < vB; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) <= vB; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) > vB; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) >= vB; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) == vB; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) != vB; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) < vB; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) <= vB; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) > vB; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getInt(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && sB.getInt(nB) >= vB; };
            };
        };
    }

    private static RowMatcher intRange(String[] p, String n, Operator opA, int vA, Operator opB, int vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v == vA && v == vB; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v == vA && v != vB; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v == vA && v < vB; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v == vA && v <= vB; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v == vA && v > vB; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v == vA && v >= vB; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v != vA && v == vB; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v != vA && v != vB; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v != vA && v < vB; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v != vA && v <= vB; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v != vA && v > vB; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v != vA && v >= vB; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v < vA && v == vB; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v < vA && v != vB; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v < vA && v < vB; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v < vA && v <= vB; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v < vA && v > vB; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v < vA && v >= vB; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v <= vA && v == vB; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v <= vA && v != vB; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v <= vA && v < vB; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v <= vA && v <= vB; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v <= vA && v > vB; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v <= vA && v >= vB; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v > vA && v == vB; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v > vA && v != vB; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v > vA && v < vB; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v > vA && v <= vB; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v > vA && v > vB; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v > vA && v >= vB; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v >= vA && v == vB; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v >= vA && v != vB; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v >= vA && v < vB; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v >= vA && v <= vB; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v >= vA && v > vB; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; int v = s.getInt(n); return v >= vA && v >= vB; };
            };
        };
    }

    // ==================== Double + Double ====================
    //
    // Use Double.compare to mirror legacy NaN-aware semantics from the leaf factories.

    private static RowMatcher fuseDoubleDouble(ResolvedPredicate.DoublePredicate da,
            ResolvedPredicate.DoublePredicate db, FileSchema schema) {
        String[] pathA = pathSegments(schema, da.columnIndex());
        String nameA = leafName(schema, da.columnIndex());
        if (da.columnIndex() == db.columnIndex()) {
            return doubleRange(pathA, nameA, da.op(), da.value(), db.op(), db.value());
        }
        String[] pathB = pathSegments(schema, db.columnIndex());
        String nameB = leafName(schema, db.columnIndex());
        return doubleDoubleDiff(pathA, nameA, da.op(), da.value(), pathB, nameB, db.op(), db.value());
    }

    private static RowMatcher doubleDoubleDiff(String[] pA, String nA, Operator opA, double vA,
            String[] pB, String nB, Operator opB, double vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) != 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) != 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) != 0; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) != 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) < 0; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) != 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) <= 0; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) != 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) > 0; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) != 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) >= 0; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) == 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) == 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) != 0; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) == 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) < 0; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) == 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) <= 0; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) == 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) > 0; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) == 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) >= 0; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) >= 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) >= 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) != 0; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) >= 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) < 0; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) >= 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) <= 0; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) >= 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) > 0; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) >= 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) >= 0; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) > 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) > 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) != 0; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) > 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) < 0; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) > 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) <= 0; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) > 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) > 0; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) > 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) >= 0; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) <= 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) <= 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) != 0; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) <= 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) < 0; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) <= 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) <= 0; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) <= 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) > 0; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) <= 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) >= 0; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) < 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) < 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) != 0; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) < 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) < 0; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) < 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) <= 0; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) < 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) > 0; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || Double.compare(sA.getDouble(nA), vA) < 0) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) >= 0; };
            };
        };
    }

    private static RowMatcher doubleRange(String[] p, String n, Operator opA, double vA, Operator opB, double vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) == 0 && Double.compare(v, vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) == 0 && Double.compare(v, vB) != 0; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) == 0 && Double.compare(v, vB) < 0; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) == 0 && Double.compare(v, vB) <= 0; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) == 0 && Double.compare(v, vB) > 0; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) == 0 && Double.compare(v, vB) >= 0; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) != 0 && Double.compare(v, vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) != 0 && Double.compare(v, vB) != 0; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) != 0 && Double.compare(v, vB) < 0; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) != 0 && Double.compare(v, vB) <= 0; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) != 0 && Double.compare(v, vB) > 0; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) != 0 && Double.compare(v, vB) >= 0; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) < 0 && Double.compare(v, vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) < 0 && Double.compare(v, vB) != 0; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) < 0 && Double.compare(v, vB) < 0; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) < 0 && Double.compare(v, vB) <= 0; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) < 0 && Double.compare(v, vB) > 0; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) < 0 && Double.compare(v, vB) >= 0; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) <= 0 && Double.compare(v, vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) <= 0 && Double.compare(v, vB) != 0; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) <= 0 && Double.compare(v, vB) < 0; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) <= 0 && Double.compare(v, vB) <= 0; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) <= 0 && Double.compare(v, vB) > 0; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) <= 0 && Double.compare(v, vB) >= 0; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) > 0 && Double.compare(v, vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) > 0 && Double.compare(v, vB) != 0; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) > 0 && Double.compare(v, vB) < 0; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) > 0 && Double.compare(v, vB) <= 0; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) > 0 && Double.compare(v, vB) > 0; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) > 0 && Double.compare(v, vB) >= 0; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) >= 0 && Double.compare(v, vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) >= 0 && Double.compare(v, vB) != 0; };
                case LT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) >= 0 && Double.compare(v, vB) < 0; };
                case LT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) >= 0 && Double.compare(v, vB) <= 0; };
                case GT -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) >= 0 && Double.compare(v, vB) > 0; };
                case GT_EQ -> row -> { StructAccessor s = resolve(row, p); if (s == null || s.isNull(n)) return false; double v = s.getDouble(n); return Double.compare(v, vA) >= 0 && Double.compare(v, vB) >= 0; };
            };
        };
    }

    // ==================== Long + Double (cross-column only; types differ) ====================

    private static RowMatcher fuseLongDoubleDiff(ResolvedPredicate.LongPredicate la,
            ResolvedPredicate.DoublePredicate db, FileSchema schema) {
        String[] pathA = pathSegments(schema, la.columnIndex());
        String nameA = leafName(schema, la.columnIndex());
        String[] pathB = pathSegments(schema, db.columnIndex());
        String nameB = leafName(schema, db.columnIndex());
        return longDoubleDiff(pathA, nameA, la.op(), la.value(), pathB, nameB, db.op(), db.value());
    }

    private static RowMatcher longDoubleDiff(String[] pA, String nA, Operator opA, long vA,
            String[] pB, String nB, Operator opB, double vB) {
        return switch (opA) {
            case EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) != 0; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) < 0; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) <= 0; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) > 0; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) >= 0; };
            };
            case NOT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) != 0; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) < 0; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) <= 0; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) > 0; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) == vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) >= 0; };
            };
            case LT -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) != 0; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) < 0; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) <= 0; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) > 0; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) >= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) >= 0; };
            };
            case LT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) != 0; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) < 0; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) <= 0; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) > 0; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) > vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) >= 0; };
            };
            case GT -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) != 0; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) < 0; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) <= 0; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) > 0; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) <= vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) >= 0; };
            };
            case GT_EQ -> switch (opB) {
                case EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) == 0; };
                case NOT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) != 0; };
                case LT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) < 0; };
                case LT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) <= 0; };
                case GT -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) > 0; };
                case GT_EQ -> row -> { StructAccessor sA = resolve(row, pA); if (sA == null || sA.isNull(nA) || sA.getLong(nA) < vA) return false; StructAccessor sB = resolve(row, pB); return sB != null && !sB.isNull(nB) && Double.compare(sB.getDouble(nB), vB) >= 0; };
            };
        };
    }

    // ==================== Path resolution helpers ====================

    private static StructAccessor resolve(StructAccessor row, String[] path) {
        StructAccessor current = row;
        for (int i = 0; i < path.length; i++) {
            String segment = path[i];
            if (current.isNull(segment)) {
                return null;
            }
            current = current.getStruct(segment);
        }
        return current;
    }

    private static String[] pathSegments(FileSchema schema, int columnIndex) {
        List<String> elements = schema.getColumn(columnIndex).fieldPath().elements();
        if (elements.size() <= 1) {
            return EMPTY_PATH;
        }
        String[] out = new String[elements.size() - 1];
        for (int i = 0; i < out.length; i++) {
            out[i] = elements.get(i);
        }
        return out;
    }

    private static String leafName(FileSchema schema, int columnIndex) {
        return schema.getColumn(columnIndex).fieldPath().leafName();
    }

    private static final String[] EMPTY_PATH = new String[0];
}
