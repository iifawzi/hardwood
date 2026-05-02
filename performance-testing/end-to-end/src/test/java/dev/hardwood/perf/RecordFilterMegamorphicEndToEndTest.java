/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.perf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

/// End-to-end record-filter benchmark under deliberate inline-cache
/// pollution. Twelve distinct query shapes are run sequentially in the
/// same JVM through `ParquetFileReader.buildRowReader().filter(...).build()`,
/// driving the outer call site at `FilteredRowReader.hasNext()` to ≥12
/// receiver classes — every shape contributes one bytecode-fused matcher
/// class with fusion on, or shares a small set of fixed-arity matchers
/// (And2Matcher / Or2Matcher) when fusion is off.
///
/// Run twice to populate the two-way comparison documented in
/// `_designs/RECORD_FILTER_FUSION_BYTECODE.md`. Separate JVMs so the
/// static `FUSION_ENABLED` flag stays clean:
///
/// ```shell
/// # BC fusion enabled (default)
/// ./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
///   -Dtest=RecordFilterMegamorphicEndToEndTest -Dperf.runs=5
///
/// # No fusion (Stage 1–3 fixed-arity matchers)
/// ./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
///   -Dtest=RecordFilterMegamorphicEndToEndTest -Dperf.runs=5 \
///   -Dhardwood.recordfilter.fusion=false
/// ```
class RecordFilterMegamorphicEndToEndTest {

    private static final Path BENCHMARK_FILE = Path.of("target/record_filter_megamorphic.parquet");
    private static final int TOTAL_ROWS = 10_000_000;
    private static final int DEFAULT_RUNS = 5;

    private record NamedFilter(String name, FilterPredicate predicate) {}

    @Test
    void compareMegamorphicOverhead() throws Exception {
        ensureBenchmarkFileExists();

        int runs = Integer.parseInt(System.getProperty("perf.runs", String.valueOf(DEFAULT_RUNS)));
        boolean fusionEnabled = !"false".equalsIgnoreCase(System.getProperty("hardwood.recordfilter.fusion"));

        System.out.println("\n=== Record Filter Megamorphic Benchmark ===");
        System.out.println("File: " + BENCHMARK_FILE + " (" + Files.size(BENCHMARK_FILE) / (1024 * 1024) + " MB)");
        System.out.println("Total rows: " + String.format("%,d", TOTAL_ROWS));
        System.out.println("Runs per shape: " + runs);
        System.out.println("Fusion enabled: " + fusionEnabled);

        List<NamedFilter> shapes = buildShapes();
        System.out.println("Shape count: " + shapes.size());

        // Warmup once across all shapes — primes the JIT for every fused class.
        System.out.println("\nWarmup...");
        for (NamedFilter f : shapes) {
            scan(f.predicate());
        }

        long[][] timings = new long[shapes.size()][runs];
        long[] matched = new long[shapes.size()];

        for (int run = 0; run < runs; run++) {
            for (int s = 0; s < shapes.size(); s++) {
                long start = System.nanoTime();
                long count = scan(shapes.get(s).predicate());
                timings[s][run] = System.nanoTime() - start;
                if (run == 0) {
                    matched[s] = count;
                }
            }
        }

        System.out.println("\nResults (avg ms per scan, " + runs + " runs):");
        System.out.printf("  %-50s %10s %15s %12s%n", "shape", "avg ms", "matched rows", "rows/sec");
        long totalNanos = 0;
        for (int s = 0; s < shapes.size(); s++) {
            double avgMs = avg(timings[s]) / 1_000_000.0;
            totalNanos += sum(timings[s]);
            System.out.printf("  %-50s %10.1f %,15d %,12.0f%n",
                    shapes.get(s).name(), avgMs, matched[s],
                    matched[s] / (avgMs / 1000.0));
        }
        System.out.printf("%n  Total wall time over %d shapes × %d runs: %.1f ms%n",
                shapes.size(), runs, totalNanos / 1_000_000.0);
    }

    private long scan(FilterPredicate predicate) throws Exception {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(BENCHMARK_FILE));
             RowReader rows = reader.buildRowReader().filter(predicate).build()) {
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
        }
        return count;
    }

    private static List<NamedFilter> buildShapes() {
        return List.of(
                new NamedFilter("id BETWEEN 1M and 4M (long+long AND)",
                        FilterPredicate.and(FilterPredicate.gtEq("id", 1_000_000L),
                                FilterPredicate.lt("id", 4_000_000L))),
                new NamedFilter("id < 500K OR id > 9.5M (long+long OR)",
                        FilterPredicate.or(FilterPredicate.lt("id", 500_000L),
                                FilterPredicate.gt("id", 9_500_000L))),
                new NamedFilter("tag BETWEEN 0 and 50 (int+int AND)",
                        FilterPredicate.and(FilterPredicate.gtEq("tag", 0),
                                FilterPredicate.lt("tag", 50))),
                new NamedFilter("tag = 5 OR tag = 47 (int+int OR)",
                        FilterPredicate.or(FilterPredicate.eq("tag", 5),
                                FilterPredicate.eq("tag", 47))),
                new NamedFilter("value BETWEEN 0 and 500 (double+double AND)",
                        FilterPredicate.and(FilterPredicate.gtEq("value", 0.0),
                                FilterPredicate.lt("value", 500.0))),
                new NamedFilter("id < 5M AND value < 500 (long+double AND)",
                        FilterPredicate.and(FilterPredicate.lt("id", 5_000_000L),
                                FilterPredicate.lt("value", 500.0))),
                new NamedFilter("id < 5M OR value > 500 (long+double OR)",
                        FilterPredicate.or(FilterPredicate.lt("id", 5_000_000L),
                                FilterPredicate.gt("value", 500.0))),
                new NamedFilter("tag < 50 AND id > 5M (int+long AND)",
                        FilterPredicate.and(FilterPredicate.lt("tag", 50),
                                FilterPredicate.gt("id", 5_000_000L))),
                new NamedFilter("tag < 50 AND value < 500 (int+double AND)",
                        FilterPredicate.and(FilterPredicate.lt("tag", 50),
                                FilterPredicate.lt("value", 500.0))),
                new NamedFilter("flag = true AND flag != false (bool AND)",
                        FilterPredicate.and(FilterPredicate.eq("flag", true),
                                FilterPredicate.notEq("flag", false))),
                new NamedFilter("value > 0 AND id < 9999 (canonical-swap)",
                        FilterPredicate.and(FilterPredicate.gt("value", 0.0),
                                FilterPredicate.lt("id", 9999L))),
                new NamedFilter("bin BETWEEN k200 and k800 (binary+binary AND)",
                        FilterPredicate.and(
                                FilterPredicate.gtEq("bin", "k200"),
                                FilterPredicate.lt("bin", "k800"))));
    }

    private void ensureBenchmarkFileExists() throws IOException {
        if (Files.exists(BENCHMARK_FILE) && Files.size(BENCHMARK_FILE) > 0) {
            return;
        }

        System.out.println("Generating benchmark file (" + TOTAL_ROWS / 1_000_000 + "M rows)...");

        Schema schema = SchemaBuilder.record("benchmark")
                .fields()
                .requiredLong("id")
                .requiredDouble("value")
                .requiredInt("tag")
                .requiredBoolean("flag")
                .requiredString("bin")
                .endRecord();

        Configuration conf = new Configuration();
        conf.set("parquet.writer.version", "v2");

        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(BENCHMARK_FILE.toAbsolutePath().toString());

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize((long) TOTAL_ROWS * 16)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withPageWriteChecksumEnabled(false)
                .build()) {

            Random rng = new Random(42);
            for (int i = 0; i < TOTAL_ROWS; i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", (long) i);
                record.put("value", rng.nextDouble() * 1000.0);
                record.put("tag", rng.nextInt(100));
                record.put("flag", rng.nextBoolean());
                record.put("bin", String.format("k%03d", rng.nextInt(1000)));
                writer.write(record);
            }
        }

        System.out.println("Generated " + BENCHMARK_FILE + " (" + Files.size(BENCHMARK_FILE) / (1024 * 1024) + " MB)");
    }

    private static long sum(long[] xs) {
        long t = 0;
        for (long x : xs) t += x;
        return t;
    }

    private static double avg(long[] xs) {
        return (double) sum(xs) / xs.length;
    }
}
