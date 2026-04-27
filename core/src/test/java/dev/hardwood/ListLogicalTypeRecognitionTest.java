/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

// Testing the modern List variant
class ListLogicalTypeRecognitionTest {

    static Stream<Arguments> allVariants() {
        return Stream.of(
                Arguments.of(Named.of("both annotations",
                        Paths.get("src/test/resources/list_annotation_both_test.parquet"))),
                Arguments.of(Named.of("modern-only (logicalType only)",
                        Paths.get("src/test/resources/list_annotation_modern_only_test.parquet"))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allVariants")
    void testOuterGroupIsRecognisedAsList(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            SchemaNode tagsNode = reader.getFileSchema().getRootNode().children().get(1);
            assertThat(tagsNode).isInstanceOf(SchemaNode.GroupNode.class);
            SchemaNode.GroupNode tagsGroup = (SchemaNode.GroupNode) tagsNode;
            assertThat(tagsGroup.isList()).isTrue();
            assertThat(tagsGroup.isStruct()).isFalse();
            assertThat(tagsGroup.isMap()).isFalse();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allVariants")
    void testLogicalTypeIsListType(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            SchemaNode.GroupNode tagsGroup =
                    (SchemaNode.GroupNode) reader.getFileSchema().getRootNode().children().get(1);
            assertThat(tagsGroup.logicalType()).isInstanceOf(LogicalType.ListType.class);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allVariants")
    void testListElementReachableOnceIsListIsCorrect(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            SchemaNode.GroupNode tagsGroup =
                    (SchemaNode.GroupNode) reader.getFileSchema().getRootNode().children().get(1);
            SchemaNode element = tagsGroup.getListElement();
            assertThat(element).isNotNull();
            assertThat(element.name()).isEqualTo("element");
        }
    }
}
