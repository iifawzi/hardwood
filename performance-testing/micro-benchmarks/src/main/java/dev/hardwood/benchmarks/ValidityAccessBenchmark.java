/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.util.BitSet;
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

/// Isolates the per-row null-check pattern that changed in #440:
/// `BitSet.get(i)` versus the inlined `(words[i >>> 6] & (1L << i)) != 0L`.
///
/// The two benchmarks read the same underlying bitmap (one wrapped in
/// `BitSet`, one as raw `long[]`) and sum a parallel `int[]` of values at the
/// positions where the bit is set — mirroring the shape of `FlatRowReader`'s
/// `if (validity.get(row)) … values[row]` per-row check + payload load.
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar ValidityAccessBenchmark -rf json -rff validity-access-jmh.json
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms512m", "-Xmx512m" })
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@OperationsPerInvocation(ValidityAccessBenchmark.BATCH_SIZE)
public class ValidityAccessBenchmark {

    static final int BATCH_SIZE = 4096;

    /// Null density in percent. `0` is the no-nulls hot path (the JIT can keep
    /// the load predicted-taken); `10` is a typical column; `50` defeats branch
    /// prediction on the per-row check.
    @Param({ "0", "10", "50" })
    public int nullPct;

    private BitSet validityBitSet;
    private long[] validityWords;
    private int[] values;

    @Setup
    public void setup() {
        Random rng = new Random(42);
        validityBitSet = new BitSet(BATCH_SIZE);
        validityWords = new long[(BATCH_SIZE + 63) >>> 6];
        values = new int[BATCH_SIZE];
        for (int i = 0; i < BATCH_SIZE; i++) {
            values[i] = i;
            if (rng.nextInt(100) >= nullPct) {
                validityBitSet.set(i);
                validityWords[i >>> 6] |= 1L << i;
            }
        }
    }

    @Benchmark
    public long bitSetGet() {
        long sum = 0L;
        BitSet v = validityBitSet;
        int[] arr = values;
        for (int i = 0; i < BATCH_SIZE; i++) {
            if (v.get(i)) {
                sum += arr[i];
            }
        }
        return sum;
    }

    @Benchmark
    public long wordsInlined() {
        long sum = 0L;
        long[] w = validityWords;
        int[] arr = values;
        for (int i = 0; i < BATCH_SIZE; i++) {
            if ((w[i >>> 6] & (1L << i)) != 0L) {
                sum += arr[i];
            }
        }
        return sum;
    }
}
