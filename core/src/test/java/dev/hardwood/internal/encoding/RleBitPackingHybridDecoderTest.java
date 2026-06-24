/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/// Correctness coverage for [RleBitPackingHybridDecoder], exercising the bulk
/// bit-width fast paths (notably 9-16) and the fused no-null dictionary gather
/// against an independent reference encoder.
class RleBitPackingHybridDecoderTest {

    static IntStream widths() {
        return IntStream.rangeClosed(1, 31);
    }

    @ParameterizedTest
    @MethodSource("widths")
    void decodesBitPackedRunForEveryWidth(int width) {
        Random rnd = new Random(width * 1000L + 7);
        int count = 8 * 200; // multiple of 8
        int[] expected = randomValues(rnd, count, width);

        byte[] encoded = bitPacked(expected, width);
        int[] actual = new int[count];
        new RleBitPackingHybridDecoder(encoded, width).readInts(actual, 0, count);

        assertThat(actual).containsExactly(expected);
    }

    @ParameterizedTest
    @MethodSource("widths")
    void decodesPartialAndMixedRuns(int width) {
        Random rnd = new Random(width * 31L + 1);
        int rleValue = rnd.nextInt(1 << Math.min(width, 30));
        int rleCount = 13;
        int[] packed = randomValues(rnd, 8 * 150, width);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeAll(out, rle(rleValue, rleCount, width));
        writeAll(out, bitPacked(packed, width));

        int total = rleCount + packed.length;
        int[] expected = new int[total];
        java.util.Arrays.fill(expected, 0, rleCount, rleValue & mask(width));
        System.arraycopy(packed, 0, expected, rleCount, packed.length);

        int[] actual = new int[total];
        new RleBitPackingHybridDecoder(out.toByteArray(), width).readInts(actual, 0, total);

        assertThat(actual).containsExactly(expected);
    }

    @Test
    void fusedDictionaryGatherMatchesReference() {
        int width = 12; // exercises the 9-16 fast path
        Random rnd = new Random(99);
        int dictSize = 1 << width;
        double[] dict = new double[dictSize];
        for (int i = 0; i < dictSize; i++) {
            dict[i] = rnd.nextDouble() * 1000.0;
        }
        int[] indices = randomValues(rnd, 8 * 300, width);
        byte[] encoded = bitPacked(indices, width);

        double[] output = new double[indices.length];
        new RleBitPackingHybridDecoder(encoded, width)
                .readDictionaryDoubles(output, dict, null, 0);

        double[] expected = new double[indices.length];
        for (int i = 0; i < indices.length; i++) {
            expected[i] = dict[indices[i]];
        }
        assertThat(output).containsExactly(expected);
    }

    @Test
    void fusedDictionaryGatherHandlesRleRun() {
        int width = 10;
        double[] dict = {1.5, 2.5, 3.5, 4.5};
        int idx = 3;
        int count = 5000;
        byte[] encoded = rle(idx, count, width);

        double[] output = new double[count];
        new RleBitPackingHybridDecoder(encoded, width)
                .readDictionaryDoubles(output, dict, null, 0);

        double[] expected = new double[count];
        java.util.Arrays.fill(expected, dict[idx]);
        assertThat(output).containsExactly(expected);
    }

    @Test
    void nullAwareDictionaryStillWorks() {
        int width = 11;
        double[] dict = {10.0, 20.0, 30.0, 40.0, 50.0};
        // non-null at positions 0 and 2; nulls at 1 and 3
        int[] defLevels = {1, 0, 1, 0};
        int maxDef = 1;
        int[] indices = {4, 1}; // two non-null values
        byte[] encoded = bitPacked(pad8(indices), width);

        double[] output = new double[4];
        new RleBitPackingHybridDecoder(encoded, width)
                .readDictionaryDoubles(output, dict, defLevels, maxDef);

        assertThat(output).containsExactly(dict[4], 0.0, dict[1], 0.0);
    }

    // --- reference encoder (LSB-first, matching the Parquet RLE/bit-packing hybrid) ---

    private static int mask(int width) {
        return width >= 31 ? Integer.MAX_VALUE : (1 << width) - 1;
    }

    private static int[] randomValues(Random rnd, int count, int width) {
        int bound = width >= 31 ? Integer.MAX_VALUE : (1 << width);
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = rnd.nextInt(bound);
        }
        return values;
    }

    private static int[] pad8(int[] values) {
        int[] padded = new int[(values.length + 7) / 8 * 8];
        System.arraycopy(values, 0, padded, 0, values.length);
        return padded;
    }

    private static byte[] bitPacked(int[] values, int width) {
        if (values.length % 8 != 0) {
            throw new IllegalArgumentException("bit-packed run must be a multiple of 8");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeVarInt(out, ((long) (values.length / 8) << 1) | 1L);
        long acc = 0;
        int bits = 0;
        long m = mask(width) & 0xFFFFFFFFL;
        for (int v : values) {
            acc |= (v & m) << bits;
            bits += width;
            while (bits >= 8) {
                out.write((int) (acc & 0xFF));
                acc >>>= 8;
                bits -= 8;
            }
        }
        return out.toByteArray();
    }

    private static byte[] rle(int value, int count, int width) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeVarInt(out, (long) count << 1);
        int bytes = (width + 7) / 8;
        for (int i = 0; i < bytes; i++) {
            out.write((value >>> (8 * i)) & 0xFF);
        }
        return out.toByteArray();
    }

    private static void writeVarInt(ByteArrayOutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) (value & 0x7F));
    }

    private static void writeAll(ByteArrayOutputStream out, byte[] bytes) {
        out.write(bytes, 0, bytes.length);
    }
}
