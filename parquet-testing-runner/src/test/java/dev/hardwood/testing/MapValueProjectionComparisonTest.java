/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Documents a projection divergence between Hardwood and parquet-java for a
/// sub-field of a `map<string, struct>` value (see GitHub issue tracking the
/// core defect).
///
/// The fixture is `people map<string, struct{name, age}>`. The projection keeps
/// only the value sub-field `age` (`people.key_value.value.age`).
///
/// A Parquet `MAP` requires its `key` column — a valid requested-projection
/// schema cannot drop it. parquet-java therefore reads the keys alongside the
/// projected `age` and narrows the value struct (dropping `name`), returning a
/// well-formed map.
///
/// Hardwood's `ColumnProjection` resolves by leaf name and collects only the
/// `value.age` leaf, leaving the mandatory `key` column unprojected. The row
/// reader then cannot assemble the map and throws when the column is accessed.
/// When the core projection resolver is fixed to force-include a map's key,
/// the Hardwood case below should be updated to assert a successful read.
class MapValueProjectionComparisonTest {

    @TempDir
    static Path tmpDir;

    private static Path parquetFile;

    private static final String FULL_SCHEMA = """
            message schema {
              required int32 id;
              optional group people (MAP) {
                repeated group key_value {
                  required binary key (STRING);
                  optional group value {
                    optional binary name (STRING);
                    optional int32 age;
                  }
                }
              }
            }
            """;

    /// Same schema with `name` removed from the map value — the key column is
    /// retained, as a valid Parquet map projection requires.
    private static final String VALUE_AGE_PROJECTION = """
            message schema {
              optional group people (MAP) {
                repeated group key_value {
                  required binary key (STRING);
                  optional group value {
                    optional int32 age;
                  }
                }
              }
            }
            """;

    @BeforeAll
    static void writeFile() throws IOException {
        parquetFile = tmpDir.resolve("map_value_projection.parquet");
        MessageType schema = MessageTypeParser.parseMessageType(FULL_SCHEMA);
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.toUri());
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
                .withConf(conf)
                .withType(schema)
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);

            Group row1 = factory.newGroup();
            row1.append("id", 1);
            Group people1 = row1.addGroup("people");
            appendEntry(people1, "alice", "Alice", 30);
            appendEntry(people1, "bob", "Bob", 40);
            writer.write(row1);

            Group row2 = factory.newGroup();
            row2.append("id", 2);
            Group people2 = row2.addGroup("people");
            appendEntry(people2, "carol", "Carol", 50);
            writer.write(row2);
        }
    }

    private static void appendEntry(Group map, String key, String name, int age) {
        Group entry = map.addGroup("key_value");
        entry.append("key", key);
        Group value = entry.addGroup("value");
        value.append("name", name);
        value.append("age", age);
    }

    @Test
    void parquetJavaReadsKeysAndNarrowsValue() throws IOException {
        Configuration conf = new Configuration();
        conf.set(ReadSupport.PARQUET_READ_SCHEMA, VALUE_AGE_PROJECTION);
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.toUri());

        List<String> keys = new ArrayList<>();
        List<Integer> ages = new ArrayList<>();
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), hadoopPath)
                .withConf(conf)
                .build()) {
            Group row;
            while ((row = reader.read()) != null) {
                Group people = row.getGroup("people", 0);
                int entries = people.getFieldRepetitionCount("key_value");
                for (int i = 0; i < entries; i++) {
                    Group entry = people.getGroup("key_value", i);
                    keys.add(entry.getString("key", 0));
                    Group value = entry.getGroup("value", 0);
                    ages.add(value.getInteger("age", 0));
                    // The projected value struct carries only `age`; `name` is gone.
                    assertThat(value.getType().containsField("name")).isFalse();
                }
            }
        }

        // parquet-java kept the mandatory key column and read the projected value.
        assertThat(keys).containsExactly("alice", "bob", "carol");
        assertThat(ages).containsExactly(30, 40, 50);
    }

    @Test
    void hardwoodFailsToProjectMapValueSubField() throws Exception {
        // Hardwood resolves `people.key_value.value.age` to the single value leaf
        // and drops the map's required key column, so the map cannot be assembled.
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rowReader = fileReader.buildRowReader()
                     .projection(ColumnProjection.columns("people.key_value.value.age"))
                     .build()) {
            rowReader.next();
            // Current (defective) behavior: accessing the under-projected map throws.
            assertThatThrownBy(() -> rowReader.isNull("people"))
                    .isInstanceOf(IndexOutOfBoundsException.class);
        }
    }
}
