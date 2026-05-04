/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.predicate.codegen.GenerateFusedMatchers;

/// Single trigger element for the [GenerateFusedMatchers] annotation
/// processor. The processor reacts to this annotation by emitting fused
/// arity-2 [RowMatcher] classes into `dev.hardwood.internal.predicate.fused`.
@GenerateFusedMatchers
final class FusedMatchers {
    private FusedMatchers() {
    }
}
