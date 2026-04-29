/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Internal-only fast-path accessor that takes a *projected field index*
/// (the index of a column within the row reader's projection) instead of a
/// field name. Bypasses the name → index hash lookup that
/// [dev.hardwood.row.StructAccessor] performs on every access.
///
/// Implemented by [FlatRowReader]; consulted only by compiled
/// [dev.hardwood.internal.predicate.RowMatcher]s when the schema column
/// resolves to a flat (top-level, non-nested) path *and* the row reader
/// guarantees the row will be an `IndexedAccessor`. The compiler emits
/// these leaves only via the
/// [dev.hardwood.internal.predicate.RecordFilterCompiler#compile(dev.hardwood.internal.predicate.ResolvedPredicate, dev.hardwood.schema.FileSchema, dev.hardwood.schema.ProjectedSchema)]
/// overload, which the flat row reader calls.
///
/// All methods take the *projected* field index (i.e. the index returned by
/// [dev.hardwood.schema.ProjectedSchema#toProjectedIndex(int)] for a given
/// file column index). The compiler performs the file → projected
/// translation once at compile time and bakes the result into the matcher.
public interface IndexedAccessor {

    boolean isNullAt(int projectedIndex);

    int getIntAt(int projectedIndex);

    long getLongAt(int projectedIndex);

    float getFloatAt(int projectedIndex);

    double getDoubleAt(int projectedIndex);

    boolean getBooleanAt(int projectedIndex);

    byte[] getBinaryAt(int projectedIndex);
}
