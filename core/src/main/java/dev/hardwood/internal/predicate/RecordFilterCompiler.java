/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.Arrays;
import java.util.List;

import dev.hardwood.internal.reader.IndexedAccessor;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Compiles a [ResolvedPredicate] into a [RowMatcher] tree once per reader.
///
/// All field-name lookups, struct-path resolutions, and operator decisions
/// are performed at compile time. The returned matcher only reads values
/// and runs comparisons per row, eliminating the type and operator
/// switches that [RecordFilterEvaluator] performs for every row.
public final class RecordFilterCompiler {

    private static final String[] EMPTY_PATH = new String[0];
    private static final int IN_LIST_BINARY_SEARCH_THRESHOLD = 16;

    private RecordFilterCompiler() {
    }

    public static RowMatcher compile(ResolvedPredicate predicate, FileSchema schema) {
        return compile(predicate, schema, null);
    }

    /// Indexed-access overload: when the row reader guarantees the row will
    /// implement [IndexedAccessor] (i.e. it is a
    /// [dev.hardwood.internal.reader.FlatRowReader]), pass its projection
    /// here. The compiler then translates each top-level leaf's column
    /// index to the projected field index at compile time and emits
    /// indexed leaves that bypass the name → index hash lookup. Nested
    /// paths (path length > 1) still use the name-keyed leaves regardless,
    /// since indexed access is only valid for flat top-level columns.
    public static RowMatcher compile(ResolvedPredicate predicate, FileSchema schema, ProjectedSchema projection) {
        return switch (predicate) {
            case ResolvedPredicate.IntPredicate p -> projection != null && isTopLevel(schema, p.columnIndex())
                    ? indexedIntLeaf(projection.toProjectedIndex(p.columnIndex()), p.op(), p.value())
                    : intLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            case ResolvedPredicate.LongPredicate p -> projection != null && isTopLevel(schema, p.columnIndex())
                    ? indexedLongLeaf(projection.toProjectedIndex(p.columnIndex()), p.op(), p.value())
                    : longLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            case ResolvedPredicate.FloatPredicate p -> projection != null && isTopLevel(schema, p.columnIndex())
                    ? indexedFloatLeaf(projection.toProjectedIndex(p.columnIndex()), p.op(), p.value())
                    : floatLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            case ResolvedPredicate.DoublePredicate p -> projection != null && isTopLevel(schema, p.columnIndex())
                    ? indexedDoubleLeaf(projection.toProjectedIndex(p.columnIndex()), p.op(), p.value())
                    : doubleLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            case ResolvedPredicate.BooleanPredicate p -> projection != null && isTopLevel(schema, p.columnIndex())
                    ? indexedBooleanLeaf(projection.toProjectedIndex(p.columnIndex()), p.op(), p.value())
                    : booleanLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            case ResolvedPredicate.BinaryPredicate p ->
                    binaryLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()),
                            p.op(), p.value(), p.signed());
            case ResolvedPredicate.IntInPredicate p ->
                    intInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values());
            case ResolvedPredicate.LongInPredicate p ->
                    longInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values());
            case ResolvedPredicate.BinaryInPredicate p ->
                    binaryInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values());
            case ResolvedPredicate.IsNullPredicate p -> projection != null && isTopLevel(schema, p.columnIndex())
                    ? indexedIsNullLeaf(projection.toProjectedIndex(p.columnIndex()))
                    : isNullLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()));
            case ResolvedPredicate.IsNotNullPredicate p -> projection != null && isTopLevel(schema, p.columnIndex())
                    ? indexedIsNotNullLeaf(projection.toProjectedIndex(p.columnIndex()))
                    : isNotNullLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()));
            case ResolvedPredicate.And and -> compileAnd(and.children(), schema, projection);
            case ResolvedPredicate.Or or -> compileOr(or.children(), schema, projection);
        };
    }

    private static boolean isTopLevel(FileSchema schema, int columnIndex) {
        return schema.getColumn(columnIndex).fieldPath().elements().size() <= 1;
    }

    // ==================== Compounds ====================

    private static RowMatcher compileAnd(List<ResolvedPredicate> children, FileSchema schema,
            ProjectedSchema projection) {
        RowMatcher[] compiled = compileAll(children, schema, projection);
        if (compiled.length == 1) {
            return compiled[0];
        }
        return new AndNMatcher(compiled);
    }

    private static RowMatcher compileOr(List<ResolvedPredicate> children, FileSchema schema,
            ProjectedSchema projection) {
        RowMatcher[] compiled = compileAll(children, schema, projection);
        if (compiled.length == 1) {
            return compiled[0];
        }
        return new OrNMatcher(compiled);
    }

    private static final class AndNMatcher implements RowMatcher {
        private final RowMatcher[] children;
        AndNMatcher(RowMatcher[] children) {
            this.children = children;
        }
        @Override
        public boolean test(StructAccessor row) {
            for (int i = 0; i < children.length; i++) {
                if (!children[i].test(row)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class OrNMatcher implements RowMatcher {
        private final RowMatcher[] children;
        OrNMatcher(RowMatcher[] children) {
            this.children = children;
        }
        @Override
        public boolean test(StructAccessor row) {
            for (int i = 0; i < children.length; i++) {
                if (children[i].test(row)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static RowMatcher[] compileAll(List<ResolvedPredicate> children, FileSchema schema,
            ProjectedSchema projection) {
        RowMatcher[] out = new RowMatcher[children.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = compile(children.get(i), schema, projection);
        }
        return out;
    }

    // ==================== Name-keyed leaf factories ====================
    //
    // Each factory returns a different lambda per operator. The switch on
    // op happens once at compile time; the returned lambda has the operator
    // baked in as a literal comparison — no per-row dispatch.
    //
    // `path` is the array of intermediate struct names (empty for top-level).
    // `name` is the leaf field name.

    private static RowMatcher intLeaf(String[] path, String name, Operator op, int v) {
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) == v; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) != v; };
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) < v; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) <= v; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) > v; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) >= v; };
        };
    }

    private static RowMatcher longLeaf(String[] path, String name, Operator op, long v) {
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) == v; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) != v; };
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) < v; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) <= v; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) > v; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) >= v; };
        };
    }

    // Float and Double use Float.compare / Double.compare to match the legacy
    // RecordFilterEvaluator semantics for NaN ordering.

    private static RowMatcher floatLeaf(String[] path, String name, Operator op, float v) {
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) == 0; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) != 0; };
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) < 0; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) <= 0; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) > 0; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) >= 0; };
        };
    }

    private static RowMatcher doubleLeaf(String[] path, String name, Operator op, double v) {
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) == 0; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) != 0; };
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) < 0; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) <= 0; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) > 0; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) >= 0; };
        };
    }

    private static RowMatcher booleanLeaf(String[] path, String name, Operator op, boolean v) {
        // BooleanPredicate honours only EQ and NOT_EQ; matchesRow returns true for any other op
        // when the value is non-null (equivalent to a non-null check).
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getBoolean(name) == v; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getBoolean(name) != v; };
            default -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name); };
        };
    }

    private static RowMatcher binaryLeaf(String[] path, String name, Operator op, byte[] v, boolean signed) {
        return switch (op) {
            case EQ -> row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return compareBinary(a.getBinary(name), v, signed) == 0;
            };
            case NOT_EQ -> row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return compareBinary(a.getBinary(name), v, signed) != 0;
            };
            case LT -> row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return compareBinary(a.getBinary(name), v, signed) < 0;
            };
            case LT_EQ -> row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return compareBinary(a.getBinary(name), v, signed) <= 0;
            };
            case GT -> row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return compareBinary(a.getBinary(name), v, signed) > 0;
            };
            case GT_EQ -> row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return compareBinary(a.getBinary(name), v, signed) >= 0;
            };
        };
    }

    private static int compareBinary(byte[] left, byte[] right, boolean signed) {
        return signed
                ? BinaryComparator.compareSigned(left, right)
                : BinaryComparator.compareUnsigned(left, right);
    }

    private static RowMatcher intInLeaf(String[] path, String name, int[] values) {
        int[] sorted = values.clone();
        Arrays.sort(sorted);
        if (sorted.length >= IN_LIST_BINARY_SEARCH_THRESHOLD) {
            return row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return Arrays.binarySearch(sorted, a.getInt(name)) >= 0;
            };
        }
        return row -> {
            StructAccessor a = resolve(row, path);
            if (a == null || a.isNull(name)) return false;
            int val = a.getInt(name);
            for (int i = 0; i < sorted.length; i++) {
                if (sorted[i] == val) return true;
            }
            return false;
        };
    }

    private static RowMatcher longInLeaf(String[] path, String name, long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        if (sorted.length >= IN_LIST_BINARY_SEARCH_THRESHOLD) {
            return row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return Arrays.binarySearch(sorted, a.getLong(name)) >= 0;
            };
        }
        return row -> {
            StructAccessor a = resolve(row, path);
            if (a == null || a.isNull(name)) return false;
            long val = a.getLong(name);
            for (int i = 0; i < sorted.length; i++) {
                if (sorted[i] == val) return true;
            }
            return false;
        };
    }

    private static RowMatcher binaryInLeaf(String[] path, String name, byte[][] values) {
        byte[][] copy = values.clone();
        return row -> {
            StructAccessor a = resolve(row, path);
            if (a == null || a.isNull(name)) return false;
            byte[] val = a.getBinary(name);
            for (int i = 0; i < copy.length; i++) {
                if (Arrays.equals(val, copy[i])) return true;
            }
            return false;
        };
    }

    private static RowMatcher isNullLeaf(String[] path, String name) {
        return row -> {
            StructAccessor a = resolve(row, path);
            return a == null || a.isNull(name);
        };
    }

    private static RowMatcher isNotNullLeaf(String[] path, String name) {
        return row -> {
            StructAccessor a = resolve(row, path);
            return a != null && !a.isNull(name);
        };
    }

    // ==================== Indexed leaf factories ====================
    //
    // Used when the row reader implements [IndexedAccessor] (i.e. it is a
    // [dev.hardwood.internal.reader.FlatRowReader]) and the leaf operates
    // on a top-level column. The cast is safe by construction — only the
    // projection-aware compile entry point routes here, and that entry
    // point is invoked solely from readers that guarantee the row implements
    // [IndexedAccessor].

    private static RowMatcher indexedIntLeaf(int idx, Operator op, int v) {
        return switch (op) {
            case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getIntAt(idx) == v; };
            case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getIntAt(idx) != v; };
            case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getIntAt(idx) < v; };
            case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getIntAt(idx) <= v; };
            case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getIntAt(idx) > v; };
            case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getIntAt(idx) >= v; };
        };
    }

    private static RowMatcher indexedLongLeaf(int idx, Operator op, long v) {
        return switch (op) {
            case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getLongAt(idx) == v; };
            case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getLongAt(idx) != v; };
            case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getLongAt(idx) < v; };
            case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getLongAt(idx) <= v; };
            case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getLongAt(idx) > v; };
            case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getLongAt(idx) >= v; };
        };
    }

    private static RowMatcher indexedFloatLeaf(int idx, Operator op, float v) {
        return switch (op) {
            case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Float.compare(a.getFloatAt(idx), v) == 0; };
            case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Float.compare(a.getFloatAt(idx), v) != 0; };
            case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Float.compare(a.getFloatAt(idx), v) < 0; };
            case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Float.compare(a.getFloatAt(idx), v) <= 0; };
            case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Float.compare(a.getFloatAt(idx), v) > 0; };
            case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Float.compare(a.getFloatAt(idx), v) >= 0; };
        };
    }

    private static RowMatcher indexedDoubleLeaf(int idx, Operator op, double v) {
        return switch (op) {
            case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Double.compare(a.getDoubleAt(idx), v) == 0; };
            case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Double.compare(a.getDoubleAt(idx), v) != 0; };
            case LT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Double.compare(a.getDoubleAt(idx), v) < 0; };
            case LT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Double.compare(a.getDoubleAt(idx), v) <= 0; };
            case GT -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Double.compare(a.getDoubleAt(idx), v) > 0; };
            case GT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && Double.compare(a.getDoubleAt(idx), v) >= 0; };
        };
    }

    private static RowMatcher indexedBooleanLeaf(int idx, Operator op, boolean v) {
        return switch (op) {
            case EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getBooleanAt(idx) == v; };
            case NOT_EQ -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx) && a.getBooleanAt(idx) != v; };
            default -> row -> { IndexedAccessor a = (IndexedAccessor) row; return !a.isNullAt(idx); };
        };
    }

    private static RowMatcher indexedIsNullLeaf(int idx) {
        return row -> ((IndexedAccessor) row).isNullAt(idx);
    }

    private static RowMatcher indexedIsNotNullLeaf(int idx) {
        return row -> !((IndexedAccessor) row).isNullAt(idx);
    }

    // ==================== Path resolution ====================

    /// Walks the row through the captured intermediate struct path.
    /// Returns null if any intermediate struct is null. For top-level
    /// columns `path` is empty and the row itself is returned.
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
}
