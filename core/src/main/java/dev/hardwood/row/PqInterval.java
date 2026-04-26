/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

/// Value type for the Parquet `INTERVAL` logical type.
///
/// The three components are stored independently — they are **not** normalized
/// (e.g. 90 days is not converted to ~3 months) because the calendar semantics
/// of months and days vary. The on-disk encoding is a 12-byte
/// FIXED_LEN_BYTE_ARRAY: three little-endian **unsigned** 32-bit integers in
/// the order `months`, `days`, `milliseconds`.
///
/// **Unsigned semantics:** Java has no unsigned `int` type, so values greater
/// than `Integer.MAX_VALUE` will appear negative when read directly. To recover
/// the unsigned value, use `Integer.toUnsignedLong(interval.months())` (and the
/// same for `days()` / `milliseconds()`).
///
/// @param months       number of months (unsigned 32-bit)
/// @param days         number of days (unsigned 32-bit)
/// @param milliseconds number of milliseconds (unsigned 32-bit)
public record PqInterval(int months, int days, int milliseconds) {}
