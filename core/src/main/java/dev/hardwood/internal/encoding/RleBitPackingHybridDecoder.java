/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

import dev.hardwood.internal.encoding.simd.SimdOperations;
import dev.hardwood.internal.encoding.simd.VectorSupport;

/// Decoder for RLE/Bit-Packing Hybrid encoding.
/// Used primarily for definition/repetition levels and dictionary indices.
public class RleBitPackingHybridDecoder {

    private static final SimdOperations SIMD_OPS = VectorSupport.operations();

    // Little-endian 64-bit reads straight off the backing array, avoiding the
    // ByteBuffer indirection and bounds checks. Parquet bit-packing is LSB-first.
    private static final VarHandle LONG_LE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    // Thread-local reusable buffer for temporary index arrays in dictionary decoding.
    // Safe because the executor is a fixed platform thread pool, so buffers persist
    // across page decodes on the same thread with zero synchronization overhead.
    private static final ThreadLocal<int[]> TEMP_INDICES = new ThreadLocal<>();

    // Block size for fused dictionary decode: bit-packed indices are unpacked into a
    // small cache-resident buffer and gathered directly into typed output, avoiding a
    // full-length intermediate index array. Must be a multiple of 8.
    private static final int GATHER_BLOCK = 1024;
    private static final ThreadLocal<int[]> GATHER_TMP =
            ThreadLocal.withInitial(() -> new int[GATHER_BLOCK]);

    private final byte[] data;
    private final int dataEnd;
    private final int bitWidth;
    private final int bitMask;
    private int pos;

    // Run state
    private int currentValue;
    private int remainingInRun;
    private boolean isRleRun;

    // Bit buffer for packed values
    private long bitBuffer;
    private int bitsInBuffer;

    public RleBitPackingHybridDecoder(byte[] data, int bitWidth) {
        this(data, 0, data.length, bitWidth);
    }

    public RleBitPackingHybridDecoder(byte[] data, int offset, int length, int bitWidth) {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new IllegalArgumentException("Invalid RLE bit width: " + bitWidth
                    + ". Must be between 0 and 32");
        }
        this.data = data;
        this.dataEnd = offset + length;
        this.pos = offset;
        this.bitWidth = bitWidth;
        this.bitMask = (bitWidth == 0) ? 0 : (1 << bitWidth) - 1;
    }

    public void readInts(int[] buffer, int offset, int count) {
        if (bitWidth == 0 || pos >= dataEnd) {
            return;
        }

        int outPos = offset;
        int remaining = count;

        while (remaining > 0) {
            if (remainingInRun == 0) {
                readNextRun();
                if (remainingInRun == 0) {
                    break;
                }
            }

            int toRead = Math.min(remaining, remainingInRun);

            if (isRleRun) {
                Arrays.fill(buffer, outPos, outPos + toRead, currentValue);
            }
            else {
                decodeBitPacked(buffer, outPos, toRead);
            }

            outPos += toRead;
            remainingInRun -= toRead;
            remaining -= toRead;
        }

        if (remaining > 0) {
            throw new IllegalStateException("Insufficient RLE/Bit-Packing data: decoded "
                    + (count - remaining) + " of " + count + " requested values");
        }
    }

    // Type-specific dictionary lookups to avoid boxing

    public void readDictionaryLongs(long[] output, long[] dictionary, int[] defLevels, int maxDef) {
        if (defLevels == null) {
            gatherDictionaryLongs(output, dictionary);
        }
        else {
            gatherDictionaryLongs(output, dictionary, defLevels, maxDef);
        }
    }

    public void readDictionaryDoubles(double[] output, double[] dictionary, int[] defLevels, int maxDef) {
        if (defLevels == null) {
            gatherDictionaryDoubles(output, dictionary);
        }
        else {
            gatherDictionaryDoubles(output, dictionary, defLevels, maxDef);
        }
    }

    public void readDictionaryInts(int[] output, int[] dictionary, int[] defLevels, int maxDef) {
        if (defLevels == null) {
            gatherDictionaryInts(output, dictionary);
        }
        else {
            gatherDictionaryInts(output, dictionary, defLevels, maxDef);
        }
    }

    public void readDictionaryFloats(float[] output, float[] dictionary, int[] defLevels, int maxDef) {
        if (defLevels == null) {
            gatherDictionaryFloats(output, dictionary);
        }
        else {
            gatherDictionaryFloats(output, dictionary, defLevels, maxDef);
        }
    }

    public void readDictionaryByteArrays(byte[][] output, byte[][] dictionary, int[] defLevels, int maxDef) {
        int[] indices = decodeIndices(output.length, defLevels, maxDef);
        applyDictionary(output, dictionary, indices, defLevels, maxDef);
    }

    public void readBooleans(boolean[] output, int[] defLevels, int maxDef) {
        int[] indices = decodeIndices(output.length, defLevels, maxDef);
        if (defLevels == null) {
            for (int i = 0; i < output.length; i++) {
                output[i] = indices[i] != 0;
            }
        }
        else {
            int idx = 0;
            for (int i = 0; i < output.length; i++) {
                if (defLevels[i] == maxDef) {
                    output[i] = indices[idx++] != 0;
                }
            }
        }
    }

    // Fused no-null dictionary decode: skips the intermediate index array. An RLE run
    // becomes a single constant typed fill; a bit-packed run is unpacked in
    // cache-resident blocks and gathered straight into the output. Per-type
    // specialization avoids boxing the dictionary values.

    private void gatherDictionaryLongs(long[] output, long[] dict) {
        if (bitWidth == 0) {
            Arrays.fill(output, dict[0]);
            return;
        }
        int[] tmp = GATHER_TMP.get();
        int outPos = 0;
        int remaining = output.length;
        while (remaining > 0) {
            if (remainingInRun == 0) {
                readNextRun();
                if (remainingInRun == 0) {
                    break;
                }
            }
            int toRead = Math.min(remaining, remainingInRun);
            if (isRleRun) {
                Arrays.fill(output, outPos, outPos + toRead, dict[currentValue]);
                outPos += toRead;
            }
            else {
                int left = toRead;
                while (left > 0) {
                    int blk = Math.min(left, GATHER_BLOCK);
                    decodeBitPacked(tmp, 0, blk);
                    for (int i = 0; i < blk; i++) {
                        output[outPos + i] = dict[tmp[i]];
                    }
                    outPos += blk;
                    left -= blk;
                }
            }
            remainingInRun -= toRead;
            remaining -= toRead;
        }
        if (remaining > 0) {
            throw insufficientData(output.length, remaining);
        }
    }

    private void gatherDictionaryDoubles(double[] output, double[] dict) {
        if (bitWidth == 0) {
            Arrays.fill(output, dict[0]);
            return;
        }
        int[] tmp = GATHER_TMP.get();
        int outPos = 0;
        int remaining = output.length;
        while (remaining > 0) {
            if (remainingInRun == 0) {
                readNextRun();
                if (remainingInRun == 0) {
                    break;
                }
            }
            int toRead = Math.min(remaining, remainingInRun);
            if (isRleRun) {
                Arrays.fill(output, outPos, outPos + toRead, dict[currentValue]);
                outPos += toRead;
            }
            else {
                int left = toRead;
                while (left > 0) {
                    int blk = Math.min(left, GATHER_BLOCK);
                    decodeBitPacked(tmp, 0, blk);
                    for (int i = 0; i < blk; i++) {
                        output[outPos + i] = dict[tmp[i]];
                    }
                    outPos += blk;
                    left -= blk;
                }
            }
            remainingInRun -= toRead;
            remaining -= toRead;
        }
        if (remaining > 0) {
            throw insufficientData(output.length, remaining);
        }
    }

    private void gatherDictionaryInts(int[] output, int[] dict) {
        if (bitWidth == 0) {
            Arrays.fill(output, dict[0]);
            return;
        }
        int[] tmp = GATHER_TMP.get();
        int outPos = 0;
        int remaining = output.length;
        while (remaining > 0) {
            if (remainingInRun == 0) {
                readNextRun();
                if (remainingInRun == 0) {
                    break;
                }
            }
            int toRead = Math.min(remaining, remainingInRun);
            if (isRleRun) {
                Arrays.fill(output, outPos, outPos + toRead, dict[currentValue]);
                outPos += toRead;
            }
            else {
                int left = toRead;
                while (left > 0) {
                    int blk = Math.min(left, GATHER_BLOCK);
                    decodeBitPacked(tmp, 0, blk);
                    for (int i = 0; i < blk; i++) {
                        output[outPos + i] = dict[tmp[i]];
                    }
                    outPos += blk;
                    left -= blk;
                }
            }
            remainingInRun -= toRead;
            remaining -= toRead;
        }
        if (remaining > 0) {
            throw insufficientData(output.length, remaining);
        }
    }

    private void gatherDictionaryFloats(float[] output, float[] dict) {
        if (bitWidth == 0) {
            Arrays.fill(output, dict[0]);
            return;
        }
        int[] tmp = GATHER_TMP.get();
        int outPos = 0;
        int remaining = output.length;
        while (remaining > 0) {
            if (remainingInRun == 0) {
                readNextRun();
                if (remainingInRun == 0) {
                    break;
                }
            }
            int toRead = Math.min(remaining, remainingInRun);
            if (isRleRun) {
                Arrays.fill(output, outPos, outPos + toRead, dict[currentValue]);
                outPos += toRead;
            }
            else {
                int left = toRead;
                while (left > 0) {
                    int blk = Math.min(left, GATHER_BLOCK);
                    decodeBitPacked(tmp, 0, blk);
                    for (int i = 0; i < blk; i++) {
                        output[outPos + i] = dict[tmp[i]];
                    }
                    outPos += blk;
                    left -= blk;
                }
            }
            remainingInRun -= toRead;
            remaining -= toRead;
        }
        if (remaining > 0) {
            throw insufficientData(output.length, remaining);
        }
    }

    private static IllegalStateException insufficientData(int total, int remaining) {
        return new IllegalStateException("Insufficient RLE/Bit-Packing data: decoded "
                + (total - remaining) + " of " + total + " requested values");
    }

    // Fused null-aware dictionary decode: streams the bit-packed indices in
    // cache-resident blocks and scatters each into the next present output slot,
    // skipping null positions. Replaces the decode-into-full-index-array followed
    // by a separate apply pass; null slots keep their zero-initialised default.
    // Per-type specialization avoids boxing the dictionary values.

    private void gatherDictionaryLongs(long[] output, long[] dict, int[] defLevels, int maxDef) {
        int count = countNonNulls(defLevels, maxDef);
        if (count == 0) {
            return;
        }
        if (bitWidth == 0) {
            long v = dict[0];
            for (int i = 0; i < output.length; i++) {
                if (defLevels[i] == maxDef) {
                    output[i] = v;
                }
            }
            return;
        }
        int[] tmp = GATHER_TMP.get();
        int outPos = 0;
        int produced = 0;
        while (produced < count) {
            int blk = Math.min(count - produced, GATHER_BLOCK);
            readInts(tmp, 0, blk);
            for (int k = 0; k < blk; k++) {
                while (defLevels[outPos] != maxDef) {
                    outPos++;
                }
                output[outPos++] = dict[tmp[k]];
            }
            produced += blk;
        }
    }

    private void gatherDictionaryDoubles(double[] output, double[] dict, int[] defLevels, int maxDef) {
        int count = countNonNulls(defLevels, maxDef);
        if (count == 0) {
            return;
        }
        if (bitWidth == 0) {
            double v = dict[0];
            for (int i = 0; i < output.length; i++) {
                if (defLevels[i] == maxDef) {
                    output[i] = v;
                }
            }
            return;
        }
        int[] tmp = GATHER_TMP.get();
        int outPos = 0;
        int produced = 0;
        while (produced < count) {
            int blk = Math.min(count - produced, GATHER_BLOCK);
            readInts(tmp, 0, blk);
            for (int k = 0; k < blk; k++) {
                while (defLevels[outPos] != maxDef) {
                    outPos++;
                }
                output[outPos++] = dict[tmp[k]];
            }
            produced += blk;
        }
    }

    private void gatherDictionaryInts(int[] output, int[] dict, int[] defLevels, int maxDef) {
        int count = countNonNulls(defLevels, maxDef);
        if (count == 0) {
            return;
        }
        if (bitWidth == 0) {
            int v = dict[0];
            for (int i = 0; i < output.length; i++) {
                if (defLevels[i] == maxDef) {
                    output[i] = v;
                }
            }
            return;
        }
        int[] tmp = GATHER_TMP.get();
        int outPos = 0;
        int produced = 0;
        while (produced < count) {
            int blk = Math.min(count - produced, GATHER_BLOCK);
            readInts(tmp, 0, blk);
            for (int k = 0; k < blk; k++) {
                while (defLevels[outPos] != maxDef) {
                    outPos++;
                }
                output[outPos++] = dict[tmp[k]];
            }
            produced += blk;
        }
    }

    private void gatherDictionaryFloats(float[] output, float[] dict, int[] defLevels, int maxDef) {
        int count = countNonNulls(defLevels, maxDef);
        if (count == 0) {
            return;
        }
        if (bitWidth == 0) {
            float v = dict[0];
            for (int i = 0; i < output.length; i++) {
                if (defLevels[i] == maxDef) {
                    output[i] = v;
                }
            }
            return;
        }
        int[] tmp = GATHER_TMP.get();
        int outPos = 0;
        int produced = 0;
        while (produced < count) {
            int blk = Math.min(count - produced, GATHER_BLOCK);
            readInts(tmp, 0, blk);
            for (int k = 0; k < blk; k++) {
                while (defLevels[outPos] != maxDef) {
                    outPos++;
                }
                output[outPos++] = dict[tmp[k]];
            }
            produced += blk;
        }
    }

    private int[] decodeIndices(int len, int[] defLevels, int maxDef) {
        int count = defLevels == null ? len : countNonNulls(defLevels, maxDef);
        int[] indices = borrowTemp(count);
        if (bitWidth == 0) {
            Arrays.fill(indices, 0, count, 0);
        }
        else {
            readInts(indices, 0, count);
        }
        return indices;
    }

    private static int[] borrowTemp(int minSize) {
        int[] buf = TEMP_INDICES.get();
        if (buf == null || buf.length < minSize) {
            buf = new int[minSize];
            TEMP_INDICES.set(buf);
        }
        return buf;
    }

    private void applyDictionary(byte[][] output, byte[][] dict, int[] indices, int[] defLevels, int maxDef) {
        if (defLevels == null) {
            for (int i = 0; i < output.length; i++) {
                output[i] = dict[indices[i]];
            }
        }
        else {
            int idx = 0;
            for (int i = 0; i < output.length; i++) {
                if (defLevels[i] == maxDef) {
                    output[i] = dict[indices[idx++]];
                }
            }
        }
    }

    private static int countNonNulls(int[] defLevels, int maxDef) {
        return SIMD_OPS.countNonNulls(defLevels, maxDef);
    }

    private void readNextRun() {
        if (pos >= dataEnd) {
            remainingInRun = 0;
            return;
        }

        long header = readUnsignedVarInt();

        if ((header & 1) == 1) {
            // Bit-packed: header >> 1 = number of 8-value groups
            remainingInRun = (int) (header >> 1) * 8;
            isRleRun = false;
        }
        else {
            // RLE: header >> 1 = repeat count
            remainingInRun = (int) (header >> 1);
            currentValue = readRleValue();
            isRleRun = true;
        }
    }

    private int readRleValue() {
        int bytesNeeded = (bitWidth + 7) / 8;
        int value = 0;
        for (int i = 0; i < bytesNeeded && pos < dataEnd; i++) {
            value |= (data[pos++] & 0xFF) << (i * 8);
        }
        return value & bitMask;
    }

    /// Batch decode bit-packed values. Optimized paths for common bit widths.
    private void decodeBitPacked(int[] output, int outPos, int count) {
        final int width = bitWidth;
        final int mask = bitMask;

        // Drain leftover bits first
        while (bitsInBuffer >= width && count > 0) {
            output[outPos++] = (int) (bitBuffer & mask);
            bitBuffer >>>= width;
            bitsInBuffer -= width;
            count--;
        }

        // Fast path for bit width 1 (common for definition levels)
        if (width == 1) {
            while (count >= 8 && pos < dataEnd) {
                int b = data[pos++] & 0xFF;
                output[outPos]     = b & 1;
                output[outPos + 1] = (b >> 1) & 1;
                output[outPos + 2] = (b >> 2) & 1;
                output[outPos + 3] = (b >> 3) & 1;
                output[outPos + 4] = (b >> 4) & 1;
                output[outPos + 5] = (b >> 5) & 1;
                output[outPos + 6] = (b >> 6) & 1;
                output[outPos + 7] = (b >> 7) & 1;
                outPos += 8;
                count -= 8;
            }
        }
        // For widths 2-8: read 8 bytes at once when possible, extract 8 values
        else if (width <= 8) {
            // Process 8 values at a time using bulk long reads when we have enough data
            while (count >= 8 && pos + 8 <= dataEnd) {
                long bits = (long) LONG_LE.get(data, pos);
                pos += width; // Only consume 'width' bytes for 8 values

                output[outPos]     = (int) (bits & mask); bits >>>= width;
                output[outPos + 1] = (int) (bits & mask); bits >>>= width;
                output[outPos + 2] = (int) (bits & mask); bits >>>= width;
                output[outPos + 3] = (int) (bits & mask); bits >>>= width;
                output[outPos + 4] = (int) (bits & mask); bits >>>= width;
                output[outPos + 5] = (int) (bits & mask); bits >>>= width;
                output[outPos + 6] = (int) (bits & mask); bits >>>= width;
                output[outPos + 7] = (int) (bits & mask);
                outPos += 8;
                count -= 8;
            }
            // Fallback when near end of buffer
            while (count >= 8 && pos + width <= dataEnd) {
                long bits = 0;
                for (int i = 0; i < width; i++) {
                    bits |= ((long) (data[pos++] & 0xFF)) << (i * 8);
                }
                output[outPos]     = (int) (bits & mask); bits >>>= width;
                output[outPos + 1] = (int) (bits & mask); bits >>>= width;
                output[outPos + 2] = (int) (bits & mask); bits >>>= width;
                output[outPos + 3] = (int) (bits & mask); bits >>>= width;
                output[outPos + 4] = (int) (bits & mask); bits >>>= width;
                output[outPos + 5] = (int) (bits & mask); bits >>>= width;
                output[outPos + 6] = (int) (bits & mask); bits >>>= width;
                output[outPos + 7] = (int) (bits & mask);
                outPos += 8;
                count -= 8;
            }
        }
        // For widths 9-16: one 8-value group spans at most 128 bits, so two
        // little-endian 64-bit loads cover it. Wider widths (rare: >65535 distinct
        // dictionary entries) fall through to the scalar bit-buffer tail below.
        else if (width <= 16) {
            while (count >= 8 && pos + 16 <= dataEnd) {
                long lo = (long) LONG_LE.get(data, pos);
                long hi = (long) LONG_LE.get(data, pos + 8);
                pos += width; // Only consume 'width' bytes for 8 values

                for (int i = 0; i < 8; i++) {
                    int b = i * width;
                    long v;
                    if (b >= 64) {
                        v = hi >>> (b - 64);
                    }
                    else if (b + width <= 64) {
                        v = lo >>> b;
                    }
                    else {
                        v = (lo >>> b) | (hi << (64 - b));
                    }
                    output[outPos + i] = (int) (v & mask);
                }
                outPos += 8;
                count -= 8;
            }
        }

        // Handle remaining values
        while (count > 0) {
            while (bitsInBuffer < width && pos < dataEnd) {
                bitBuffer |= ((long) (data[pos++] & 0xFF)) << bitsInBuffer;
                bitsInBuffer += 8;
            }
            if (bitsInBuffer < width) {
                break;
            }
            output[outPos++] = (int) (bitBuffer & mask);
            bitBuffer >>>= width;
            bitsInBuffer -= width;
            count--;
        }
    }

    private long readUnsignedVarInt() {
        long result = 0;
        int shift = 0;
        while (pos < dataEnd) {
            int b = data[pos++] & 0xFF;
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
        }
        return result;
    }
}
