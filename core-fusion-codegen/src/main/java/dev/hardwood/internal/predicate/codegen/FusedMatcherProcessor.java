/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.codegen;

import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;

/// Annotation processor that emits fused arity-2 [RowMatcher] classes for
/// the most common compound-predicate shapes. Triggered once per build via
/// the single [GenerateFusedMatchers]-annotated marker class in
/// `hardwood-core`.
///
/// The 20-tuple matrix and the per-tuple emission live in [MatrixEmitter];
/// this class only handles annotation discovery and round-management.
@SupportedAnnotationTypes("dev.hardwood.internal.predicate.codegen.GenerateFusedMatchers")
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public final class FusedMatcherProcessor extends AbstractProcessor {

    private boolean emitted;

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (emitted || roundEnv.processingOver()) {
            return false;
        }
        if (roundEnv.getElementsAnnotatedWith(GenerateFusedMatchers.class).isEmpty()) {
            return false;
        }
        new MatrixEmitter(processingEnv).emitAll();
        emitted = true;
        return true;
    }
}
