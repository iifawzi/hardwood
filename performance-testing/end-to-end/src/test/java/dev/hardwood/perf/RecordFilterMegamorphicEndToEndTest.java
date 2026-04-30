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
import java.util.ArrayList;
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

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end megamorphic-shape benchmark for record-level fusion.
///
/// Lazy-generates a 10M-row Parquet file with five columns spanning every
/// fused type (id long, value double, tag int, flag boolean, bin binary)
/// and runs ~12 distinct arity-2 compound predicates through `ParquetFileReader`
/// in the same JVM. Every shape contributes a unique fused matcher class to
/// the `FilteredRowReader.hasNext` outer call site, so by the time the last
/// shape runs the call site is megamorphic across all of them.
///
/// Two run modes (controlled by the system property
/// `hardwood.recordfilter.fusion`):
/// - `true` (default): each compiled matcher is a unique fused class with no
///   inner virtual call.
/// - `false`: matchers route through the generic `And2Matcher` / `Or2Matcher`
///   whose inner `a.test()` / `b.test()` sites also go megamorphic.
///
/// The harness times each shape and prints a side-by-side comparison plus
/// totals. Run separately under each mode for a clean A/B; sharing one JVM
/// would mean the static FUSION_ENABLED flag is fixed at first compile.
///
/// Run:
/// ```shell
/// ./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
///     -Dtest=RecordFilterMegamorphicEndToEndTest -Dperf.runs=5 \
///     -Dhardwood.recordfilter.fusion=true
/// ```
class RecordFilterMegamorphicEndToEndTest {

    private static final Path BENCHMARK_FILE = Path.of("target/record_filter_megamorphic.parquet");
    private static final int TOTAL_ROWS = 10_000_000;
    private static final int DEFAULT_RUNS = 5;

    @Test
    void compareMegamorphicFusion() throws Exception {
        ensureBenchmarkFileExists();

        int runs = Integer.parseInt(System.getProperty("perf.runs", String.valueOf(DEFAULT_RUNS)));
        String fusionMode = System.getProperty("hardwood.recordfilter.fusion", "true");

        List<NamedFilter> shapes = buildShapes();

        System.out.println("\n=== Megamorphic Record-Filter Fusion Benchmark ===");
        System.out.printf("File: %s (%d MB)%n", BENCHMARK_FILE,
                Files.size(BENCHMARK_FILE) / (1024 * 1024));
        System.out.printf("Total rows: %,d%n", TOTAL_ROWS);
        System.out.printf("Shapes: %d%n", shapes.size());
        System.out.printf("Runs per shape: %d%n", runs);
        System.out.printf("Fusion mode: %s%n", fusionMode);

        // Warmup pass — run every shape once to JIT-compile the hot paths.
        System.out.println("\nWarmup...");
        for (NamedFilter shape : shapes) {
            runQuery(shape.filter);
        }

        // Measurement pass — for each shape, capture per-run wall time.
        long[][] times = new long[shapes.size()][runs];
        long[][] counts = new long[shapes.size()][runs];
        for (int s = 0; s < shapes.size(); s++) {
            for (int r = 0; r < runs; r++) {
                long start = System.nanoTime();
                counts[s][r] = runQuery(shapes.get(s).filter);
                times[s][r] = System.nanoTime() - start;
            }
        }

        // Report.
        System.out.println("\nResults:");
        System.out.printf("  %-50s %12s %14s %14s%n", "Shape", "Avg ms", "Rows matched", "Records/sec");
        System.out.println("  " + "-".repeat(94));
        long totalNanos = 0L;
        for (int s = 0; s < shapes.size(); s++) {
            double avgMs = mean(times[s]) / 1_000_000.0;
            long rows = counts[s][0];
            System.out.printf("  %-50s %12.1f %,14d %,14.0f%n",
                    shapes.get(s).name, avgMs, rows, rows / (avgMs / 1000.0));
            totalNanos += sum(times[s]);
        }
        double totalMs = totalNanos / 1_000_000.0;
        System.out.printf("%n  Total wall time across %d shape × run combinations: %.1f ms%n",
                shapes.size() * runs, totalMs);
        System.out.printf("  Mean per (shape × run): %.2f ms%n", totalMs / (shapes.size() * runs));

        // Sanity — every shape should produce a stable, sane match count.
        for (int s = 0; s < shapes.size(); s++) {
            long firstRunCount = counts[s][0];
            assertThat(firstRunCount).as("non-zero count expected for %s", shapes.get(s).name)
                    .isGreaterThanOrEqualTo(0L)
                    .isLessThanOrEqualTo(TOTAL_ROWS);
            for (int r = 1; r < runs; r++) {
                assertThat(counts[s][r])
                        .as("count must be deterministic across runs for %s", shapes.get(s).name)
                        .isEqualTo(firstRunCount);
            }
        }
    }

    private long runQuery(FilterPredicate filter) throws Exception {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(BENCHMARK_FILE));
                RowReader rows = reader.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
        }
        return count;
    }

    private static List<NamedFilter> buildShapes() {
        List<NamedFilter> out = new ArrayList<>();

        // Column ranges: id 0..10M, value uniform [0, 1000), tag 0..99, bin "k0".."k999".

        // 0: long+long same-column AND (BETWEEN). Selectivity ~30%.
        out.add(new NamedFilter("id BETWEEN 1M and 4M (long+long AND)",
                FilterPredicate.and(
                        FilterPredicate.gtEq("id", 1_000_000L),
                        FilterPredicate.lt("id", 4_000_000L))));

        // 1: long+long same-column OR. Selectivity ~10%.
        out.add(new NamedFilter("id < 500K OR id > 9.5M (long+long OR)",
                FilterPredicate.or(
                        FilterPredicate.lt("id", 500_000L),
                        FilterPredicate.gt("id", 9_500_000L))));

        // 2: int+int same-column AND. Selectivity ~50% (tag in [0, 50)).
        out.add(new NamedFilter("tag BETWEEN 0 and 50 (int+int AND)",
                FilterPredicate.and(
                        FilterPredicate.gtEq("tag", 0),
                        FilterPredicate.lt("tag", 50))));

        // 3: int+int same-column OR. Selectivity ~2%.
        out.add(new NamedFilter("tag = 5 OR tag = 47 (int+int OR)",
                FilterPredicate.or(
                        FilterPredicate.eq("tag", 5),
                        FilterPredicate.eq("tag", 47))));

        // 4: double+double same-column AND. Selectivity ~50% (value < 500).
        out.add(new NamedFilter("value BETWEEN 0 and 500 (double+double AND)",
                FilterPredicate.and(
                        FilterPredicate.gtEq("value", 0.0),
                        FilterPredicate.lt("value", 500.0))));

        // 5: long+double diff-column AND. Selectivity ~25%.
        out.add(new NamedFilter("id < 5M AND value < 500 (long+double AND)",
                FilterPredicate.and(
                        FilterPredicate.lt("id", 5_000_000L),
                        FilterPredicate.lt("value", 500.0))));

        // 6: long+double diff-column OR. Selectivity ~75%.
        out.add(new NamedFilter("id < 5M OR value > 500 (long+double OR)",
                FilterPredicate.or(
                        FilterPredicate.lt("id", 5_000_000L),
                        FilterPredicate.gt("value", 500.0))));

        // 7: int+long diff-column AND. Selectivity ~25%.
        out.add(new NamedFilter("tag < 50 AND id > 5M (int+long AND)",
                FilterPredicate.and(
                        FilterPredicate.lt("tag", 50),
                        FilterPredicate.gt("id", 5_000_000L))));

        // 8: int+double diff-column AND. Selectivity ~25%.
        out.add(new NamedFilter("tag < 50 AND value < 500 (int+double AND)",
                FilterPredicate.and(
                        FilterPredicate.lt("tag", 50),
                        FilterPredicate.lt("value", 500.0))));

        // 9: boolean+boolean diff-column AND. flag is always evaluated against itself
        // for two different operands — ~50% match.
        out.add(new NamedFilter("flag = true AND flag != false (boolean+boolean AND)",
                FilterPredicate.and(
                        FilterPredicate.eq("flag", true),
                        FilterPredicate.notEq("flag", false))));

        // 10: double+long canonical-swap (canonicalises to long+double internally).
        out.add(new NamedFilter("value > 0 AND id < 9999 (double+long AND, canon-swap)",
                FilterPredicate.and(
                        FilterPredicate.gt("value", 0.0),
                        FilterPredicate.lt("id", 9_999L))));

        // 11: binary+binary same-column AND (range over UTF-8 string keys).
        // The `bin` column is UTF8-annotated binary; the String predicate resolves
        // to a BinaryPredicate at the executor layer, so this exercises the fused
        // binary+binary path.
        out.add(new NamedFilter("bin BETWEEN k200 and k800 (binary+binary AND)",
                FilterPredicate.and(
                        FilterPredicate.gtEq("bin", "k200"),
                        FilterPredicate.lt("bin", "k800"))));

        return out;
    }

    private static double mean(long[] values) {
        long total = 0L;
        for (long v : values) {
            total += v;
        }
        return (double) total / values.length;
    }

    private static long sum(long[] values) {
        long total = 0L;
        for (long v : values) {
            total += v;
        }
        return total;
    }

    private record NamedFilter(String name, FilterPredicate filter) {
    }

    private static void ensureBenchmarkFileExists() throws IOException {
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
                String key = String.format("k%03d", rng.nextInt(1000));
                record.put("bin", key);
                writer.write(record);
            }
        }

        System.out.println("Generated " + BENCHMARK_FILE
                + " (" + Files.size(BENCHMARK_FILE) / (1024 * 1024) + " MB)");
    }
}
