/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.function.IntUnaryOperator;

import dev.hardwood.schema.FileSchema;

/// Single entry point used by [RecordFilterCompiler] to fuse arity-2
/// AND/OR compounds into a per-tuple [RowMatcher]. The base implementation
/// delegates to [FusedMatcherRegistry], which resolves the most common
/// build-time-generated tuples; future overlays may swap this class via
/// multi-release JARs to add runtime codegen on newer JDKs.
final class FusionDispatcher {

    private FusionDispatcher() {
    }

    static RowMatcher fuse(ResolvedPredicate a, ResolvedPredicate b,
            FileSchema schema, IntUnaryOperator topLevelFieldIndex,
            boolean isAnd) {
        return FusedMatcherRegistry.lookup(a, b, schema, topLevelFieldIndex, isAnd);
    }
}
