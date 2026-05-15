/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.reader.Validity;

/// Measures the consumer-side loop shapes a `ColumnReader` caller can adopt
/// against a `Validity` value with `long[]` backing (#440):
///
/// - `isNullPerRow` — per-row `validity.isNotNull(i)` (idiomatic; what
///   existing callers already use).
/// - `nextNullCursor` — outer loop iterates null positions via
///   `validity.nextNull(from, count)`; the inner loop is a tight
///   no-validity-touch sum over each run of consecutive present positions.
///   Wins on low null density (long runs).
/// - `wordsOuterTzcnt` — word-wise outer loop with `Long.numberOfTrailingZeros`
///   + clear-lowest-bit, visiting only the present positions. Wins on high
///   null density (short runs of presents, mostly clear words to skip).
///
/// `nullPct` scans the loop shapes against varying null densities. The two
/// new shapes have complementary sweet spots; the per-row form is the
/// generic fallback that performs roughly the same regardless.
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar ColumnReaderScanBenchmark -rf json -rff column-reader-scan-jmh.json
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms512m", "-Xmx512m" })
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@OperationsPerInvocation(ColumnReaderScanBenchmark.BATCH_SIZE)
public class ColumnReaderScanBenchmark {

    static final int BATCH_SIZE = 4096;

    /// Null density in percent. `0` is the no-nulls fast path; `1` is sparse
    /// (typical analytical workload); `50` defeats branch prediction; `90` is
    /// null-dense and exercises the `tzcnt` skip path.
    @Param({ "0", "1", "10", "50", "90" })
    public int nullPct;

    private Validity validity;
    private int[] values;

    @Setup
    public void setup() {
        Random rng = new Random(42);
        long[] words = nullPct == 0 ? null : new long[(BATCH_SIZE + 63) >>> 6];
        values = new int[BATCH_SIZE];
        for (int i = 0; i < BATCH_SIZE; i++) {
            values[i] = i;
            if (words != null && rng.nextInt(100) >= nullPct) {
                words[i >>> 6] |= 1L << i;
            }
        }
        validity = Validity.of(words);
    }

    @Benchmark
    public long isNullPerRow() {
        long sum = 0L;
        Validity v = validity;
        int[] arr = values;
        if (!v.hasNulls()) {
            for (int i = 0; i < BATCH_SIZE; i++) {
                sum += arr[i];
            }
        }
        else {
            for (int i = 0; i < BATCH_SIZE; i++) {
                if (v.isNotNull(i)) {
                    sum += arr[i];
                }
            }
        }
        return sum;
    }

    @Benchmark
    public long nextNullCursor() {
        long sum = 0L;
        Validity v = validity;
        int[] arr = values;
        int n = BATCH_SIZE;
        if (!v.hasNulls()) {
            for (int i = 0; i < n; i++) {
                sum += arr[i];
            }
            return sum;
        }
        int i = 0;
        while (i < n) {
            int nullIdx = v.nextNull(i, n);
            int end = nullIdx < 0 ? n : nullIdx;
            for (int j = i; j < end; j++) {
                sum += arr[j];
            }
            i = end + 1;   // skip the null position itself
        }
        return sum;
    }

    @Benchmark
    public long wordsOuterTzcnt() {
        long sum = 0L;
        long[] w = validity.words();
        int[] arr = values;
        if (w == null) {
            for (int i = 0; i < BATCH_SIZE; i++) {
                sum += arr[i];
            }
            return sum;
        }
        int wordCount = (BATCH_SIZE + 63) >>> 6;
        for (int wi = 0; wi < wordCount; wi++) {
            long present = w[wi];
            int base = wi << 6;
            while (present != 0L) {
                int bit = Long.numberOfTrailingZeros(present);
                sum += arr[base + bit];
                present &= present - 1L;
            }
        }
        return sum;
    }
}
