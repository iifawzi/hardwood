#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""Generate RecordFilterFusion.java and RecordFilterFusionIndexed.java.

Each fused (typeA, opA, typeB, opB, connective, accessMode, sameOrDiff)
tuple must produce a distinct synthetic-class lambda. This generator
emits the entire matrix from the eligibility rules in
_designs/RECORD_FILTER_FUSION.md so the source stays consistent.

Run from repo root: python3 tools/gen_fusion.py
"""

import os
from pathlib import Path

OPS = ["EQ", "NOT_EQ", "LT", "LT_EQ", "GT", "GT_EQ"]

# Map op -> Java relational symbol for primitive compare.
OP_SYMBOL = {
    "EQ": "==", "NOT_EQ": "!=", "LT": "<", "LT_EQ": "<=", "GT": ">", "GT_EQ": ">=",
}

# Map op -> the rejection condition used in AND first-leaf short-circuit.
# i.e. "if first-leaf <REJECT_A> the operand, return false" — REJECT_A is the
# logical inverse of the op's positive form.
REJECT_OP = {
    "EQ": "!=", "NOT_EQ": "==", "LT": ">=", "LT_EQ": ">", "GT": "<=", "GT_EQ": "<",
}

# ====================================================================
# Name-keyed (StructAccessor) lambda emitters.
# ====================================================================


def name_diff_and(getter, type_name):
    """Returns the body of fooFooAndDiff for a primitive type using direct compare."""
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_b = OP_SYMBOL[opB]
            rej_a = REJECT_OP[opA]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor sA = resolve(row, pA); "
                f"if (sA == null || sA.isNull(nA) || sA.{getter}(nA) {rej_a} vA) return false; "
                "StructAccessor sB = resolve(row, pB); "
                f"return sB != null && !sB.isNull(nB) && sB.{getter}(nB) {sym_b} vB; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def name_diff_or(getter, type_name):
    """Body for fooFooOrDiff using direct compare (positive form on both sides)."""
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor sA = resolve(row, pA); "
                f"if (sA != null && !sA.isNull(nA) && sA.{getter}(nA) {sym_a} vA) return true; "
                "StructAccessor sB = resolve(row, pB); "
                f"return sB != null && !sB.isNull(nB) && sB.{getter}(nB) {sym_b} vB; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def name_range_and(getter, type_name):
    """Same-column AND: load value once, apply both ops as positive form joined with &&."""
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor s = resolve(row, p); "
                "if (s == null || s.isNull(n)) return false; "
                f"{type_name} v = s.{getter}(n); "
                f"return v {sym_a} vA && v {sym_b} vB; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def name_range_or(getter, type_name):
    """Same-column OR: load value once, apply both ops as positive form joined with ||."""
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor s = resolve(row, p); "
                "if (s == null || s.isNull(n)) return false; "
                f"{type_name} v = s.{getter}(n); "
                f"return v {sym_a} vA || v {sym_b} vB; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


# ====================================================================
# Double / Float emitters using Double.compare / Float.compare.
# ====================================================================


def name_double_diff_and(getter, type_name, cmp_class):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_b = OP_SYMBOL[opB]
            rej_a = REJECT_OP[opA]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor sA = resolve(row, pA); "
                "if (sA == null || sA.isNull(nA)) return false; "
                f"if ({cmp_class}.compare(sA.{getter}(nA), vA) {rej_a} 0) return false; "
                "StructAccessor sB = resolve(row, pB); "
                "if (sB == null || sB.isNull(nB)) return false; "
                f"return {cmp_class}.compare(sB.{getter}(nB), vB) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def name_double_diff_or(getter, type_name, cmp_class):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor sA = resolve(row, pA); "
                f"if (sA != null && !sA.isNull(nA) && {cmp_class}.compare(sA.{getter}(nA), vA) {sym_a} 0) return true; "
                "StructAccessor sB = resolve(row, pB); "
                f"return sB != null && !sB.isNull(nB) && {cmp_class}.compare(sB.{getter}(nB), vB) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def name_double_range_and(getter, type_name, cmp_class):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor s = resolve(row, p); "
                "if (s == null || s.isNull(n)) return false; "
                f"{type_name} v = s.{getter}(n); "
                f"return {cmp_class}.compare(v, vA) {sym_a} 0 && {cmp_class}.compare(v, vB) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def name_double_range_or(getter, type_name, cmp_class):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor s = resolve(row, p); "
                "if (s == null || s.isNull(n)) return false; "
                f"{type_name} v = s.{getter}(n); "
                f"return {cmp_class}.compare(v, vA) {sym_a} 0 || {cmp_class}.compare(v, vB) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


# ====================================================================
# Cross-type diff emitters.
# ====================================================================


def name_cross_diff_and(getterA, typeA, getterB, typeB, b_uses_compare=False, b_cmp_class=None):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            rej_a = REJECT_OP[opA]
            sym_b = OP_SYMBOL[opB]
            if b_uses_compare:
                inner_b = f"return sB != null && !sB.isNull(nB) && {b_cmp_class}.compare(sB.{getterB}(nB), vB) {sym_b} 0; "
            else:
                inner_b = f"return sB != null && !sB.isNull(nB) && sB.{getterB}(nB) {sym_b} vB; "
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor sA = resolve(row, pA); "
                f"if (sA == null || sA.isNull(nA) || sA.{getterA}(nA) {rej_a} vA) return false; "
                "StructAccessor sB = resolve(row, pB); "
                + inner_b
                + "};"
            )
        out.append("            };")
    return "\n".join(out)


def name_cross_diff_or(getterA, typeA, getterB, typeB, b_uses_compare=False, b_cmp_class=None):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            if b_uses_compare:
                inner_b = f"return sB != null && !sB.isNull(nB) && {b_cmp_class}.compare(sB.{getterB}(nB), vB) {sym_b} 0; "
            else:
                inner_b = f"return sB != null && !sB.isNull(nB) && sB.{getterB}(nB) {sym_b} vB; "
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor sA = resolve(row, pA); "
                f"if (sA != null && !sA.isNull(nA) && sA.{getterA}(nA) {sym_a} vA) return true; "
                "StructAccessor sB = resolve(row, pB); "
                + inner_b
                + "};"
            )
        out.append("            };")
    return "\n".join(out)


# Variant of name_cross_diff for opA using Double.compare (long+double canonical = (long, double),
# but if we have double+long the canonicalization at dispatch swaps so (long, double) is canonical;
# but for double-side first via canon swap, we still keep opA referring to the long side).
def name_cross_diff_and_a_compare(getterA, typeA, cmpA, getterB, typeB, b_uses_compare=False, b_cmp_class=None):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            rej_a = REJECT_OP[opA]
            sym_b = OP_SYMBOL[opB]
            if b_uses_compare:
                inner_b = f"return sB != null && !sB.isNull(nB) && {b_cmp_class}.compare(sB.{getterB}(nB), vB) {sym_b} 0; "
            else:
                inner_b = f"return sB != null && !sB.isNull(nB) && sB.{getterB}(nB) {sym_b} vB; "
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor sA = resolve(row, pA); "
                "if (sA == null || sA.isNull(nA)) return false; "
                f"if ({cmpA}.compare(sA.{getterA}(nA), vA) {rej_a} 0) return false; "
                "StructAccessor sB = resolve(row, pB); "
                + inner_b
                + "};"
            )
        out.append("            };")
    return "\n".join(out)


def name_cross_diff_or_a_compare(getterA, typeA, cmpA, getterB, typeB, b_uses_compare=False, b_cmp_class=None):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            if b_uses_compare:
                inner_b = f"return sB != null && !sB.isNull(nB) && {b_cmp_class}.compare(sB.{getterB}(nB), vB) {sym_b} 0; "
            else:
                inner_b = f"return sB != null && !sB.isNull(nB) && sB.{getterB}(nB) {sym_b} vB; "
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor sA = resolve(row, pA); "
                f"if (sA != null && !sA.isNull(nA) && {cmpA}.compare(sA.{getterA}(nA), vA) {sym_a} 0) return true; "
                "StructAccessor sB = resolve(row, pB); "
                + inner_b
                + "};"
            )
        out.append("            };")
    return "\n".join(out)


# ====================================================================
# Boolean diff emitters (only EQ/NOT_EQ honored; other ops fall back to non-null check).
# ====================================================================


def name_boolean_diff_and():
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            # First leaf: if opA is EQ/NOT_EQ, reject when value mismatches; else just require non-null.
            if opA == "EQ":
                a_check = "sA == null || sA.isNull(nA) || sA.getBoolean(nA) != vA"
            elif opA == "NOT_EQ":
                a_check = "sA == null || sA.isNull(nA) || sA.getBoolean(nA) == vA"
            else:
                a_check = "sA == null || sA.isNull(nA)"
            # Second leaf: if opB is EQ/NOT_EQ, compare; else just require non-null.
            if opB == "EQ":
                b_ret = "return sB != null && !sB.isNull(nB) && sB.getBoolean(nB) == vB; "
            elif opB == "NOT_EQ":
                b_ret = "return sB != null && !sB.isNull(nB) && sB.getBoolean(nB) != vB; "
            else:
                b_ret = "return sB != null && !sB.isNull(nB); "
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor sA = resolve(row, pA); "
                f"if ({a_check}) return false; "
                "StructAccessor sB = resolve(row, pB); "
                + b_ret
                + "};"
            )
        out.append("            };")
    return "\n".join(out)


def name_boolean_diff_or():
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            if opA == "EQ":
                a_accept = "sA != null && !sA.isNull(nA) && sA.getBoolean(nA) == vA"
            elif opA == "NOT_EQ":
                a_accept = "sA != null && !sA.isNull(nA) && sA.getBoolean(nA) != vA"
            else:
                a_accept = "sA != null && !sA.isNull(nA)"
            if opB == "EQ":
                b_ret = "return sB != null && !sB.isNull(nB) && sB.getBoolean(nB) == vB; "
            elif opB == "NOT_EQ":
                b_ret = "return sB != null && !sB.isNull(nB) && sB.getBoolean(nB) != vB; "
            else:
                b_ret = "return sB != null && !sB.isNull(nB); "
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor sA = resolve(row, pA); "
                f"if ({a_accept}) return true; "
                "StructAccessor sB = resolve(row, pB); "
                + b_ret
                + "};"
            )
        out.append("            };")
    return "\n".join(out)


# ====================================================================
# Binary diff/range emitters.
# Comparison goes through compareBinary(actual, target, signed) returning -1/0/+1.
# Then ops compare the cmp value to 0.
# ====================================================================


def name_binary_diff_and():
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            rej_a = REJECT_OP[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor sA = resolve(row, pA); "
                "if (sA == null || sA.isNull(nA)) return false; "
                f"if (compareBinary(sA.getBinary(nA), vA, signedA) {rej_a} 0) return false; "
                "StructAccessor sB = resolve(row, pB); "
                "if (sB == null || sB.isNull(nB)) return false; "
                f"return compareBinary(sB.getBinary(nB), vB, signedB) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def name_binary_diff_or():
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor sA = resolve(row, pA); "
                f"if (sA != null && !sA.isNull(nA) && compareBinary(sA.getBinary(nA), vA, signedA) {sym_a} 0) return true; "
                "StructAccessor sB = resolve(row, pB); "
                f"return sB != null && !sB.isNull(nB) && compareBinary(sB.getBinary(nB), vB, signedB) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def name_binary_range_and():
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor s = resolve(row, p); "
                "if (s == null || s.isNull(n)) return false; "
                "byte[] v = s.getBinary(n); "
                f"return compareBinary(v, vA, signed) {sym_a} 0 && compareBinary(v, vB, signed) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def name_binary_range_or():
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "StructAccessor s = resolve(row, p); "
                "if (s == null || s.isNull(n)) return false; "
                "byte[] v = s.getBinary(n); "
                f"return compareBinary(v, vA, signed) {sym_a} 0 || compareBinary(v, vB, signed) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


# ====================================================================
# Indexed (RowReader) emitters — same patterns but with idxA/idxB and getX(int).
# ====================================================================


def idx_diff_and(getter, type_name):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            rej_a = REJECT_OP[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                f"if (a.isNull(iA) || a.{getter}(iA) {rej_a} vA) return false; "
                f"return !a.isNull(iB) && a.{getter}(iB) {sym_b} vB; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_diff_or(getter, type_name):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                f"if (!a.isNull(iA) && a.{getter}(iA) {sym_a} vA) return true; "
                f"return !a.isNull(iB) && a.{getter}(iB) {sym_b} vB; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_range_and(getter, type_name):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                "if (a.isNull(i)) return false; "
                f"{type_name} v = a.{getter}(i); "
                f"return v {sym_a} vA && v {sym_b} vB; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_range_or(getter, type_name):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                "if (a.isNull(i)) return false; "
                f"{type_name} v = a.{getter}(i); "
                f"return v {sym_a} vA || v {sym_b} vB; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_double_diff_and(getter, type_name, cmp_class):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            rej_a = REJECT_OP[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                "if (a.isNull(iA)) return false; "
                f"if ({cmp_class}.compare(a.{getter}(iA), vA) {rej_a} 0) return false; "
                "if (a.isNull(iB)) return false; "
                f"return {cmp_class}.compare(a.{getter}(iB), vB) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_double_diff_or(getter, type_name, cmp_class):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                f"if (!a.isNull(iA) && {cmp_class}.compare(a.{getter}(iA), vA) {sym_a} 0) return true; "
                f"return !a.isNull(iB) && {cmp_class}.compare(a.{getter}(iB), vB) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_double_range_and(getter, type_name, cmp_class):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                "if (a.isNull(i)) return false; "
                f"{type_name} v = a.{getter}(i); "
                f"return {cmp_class}.compare(v, vA) {sym_a} 0 && {cmp_class}.compare(v, vB) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_double_range_or(getter, type_name, cmp_class):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                "if (a.isNull(i)) return false; "
                f"{type_name} v = a.{getter}(i); "
                f"return {cmp_class}.compare(v, vA) {sym_a} 0 || {cmp_class}.compare(v, vB) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_cross_diff_and(getterA, getterB, b_uses_compare=False, b_cmp_class=None):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            rej_a = REJECT_OP[opA]
            sym_b = OP_SYMBOL[opB]
            if b_uses_compare:
                inner_b = f"return !a.isNull(iB) && {b_cmp_class}.compare(a.{getterB}(iB), vB) {sym_b} 0; "
            else:
                inner_b = f"return !a.isNull(iB) && a.{getterB}(iB) {sym_b} vB; "
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                f"if (a.isNull(iA) || a.{getterA}(iA) {rej_a} vA) return false; "
                + inner_b
                + "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_cross_diff_or(getterA, getterB, b_uses_compare=False, b_cmp_class=None):
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            if b_uses_compare:
                inner_b = f"return !a.isNull(iB) && {b_cmp_class}.compare(a.{getterB}(iB), vB) {sym_b} 0; "
            else:
                inner_b = f"return !a.isNull(iB) && a.{getterB}(iB) {sym_b} vB; "
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                f"if (!a.isNull(iA) && a.{getterA}(iA) {sym_a} vA) return true; "
                + inner_b
                + "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_boolean_diff_and():
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            if opA == "EQ":
                a_check = "a.isNull(iA) || a.getBoolean(iA) != vA"
            elif opA == "NOT_EQ":
                a_check = "a.isNull(iA) || a.getBoolean(iA) == vA"
            else:
                a_check = "a.isNull(iA)"
            if opB == "EQ":
                b_ret = "return !a.isNull(iB) && a.getBoolean(iB) == vB; "
            elif opB == "NOT_EQ":
                b_ret = "return !a.isNull(iB) && a.getBoolean(iB) != vB; "
            else:
                b_ret = "return !a.isNull(iB); "
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                f"if ({a_check}) return false; "
                + b_ret
                + "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_boolean_diff_or():
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            if opA == "EQ":
                a_accept = "!a.isNull(iA) && a.getBoolean(iA) == vA"
            elif opA == "NOT_EQ":
                a_accept = "!a.isNull(iA) && a.getBoolean(iA) != vA"
            else:
                a_accept = "!a.isNull(iA)"
            if opB == "EQ":
                b_ret = "return !a.isNull(iB) && a.getBoolean(iB) == vB; "
            elif opB == "NOT_EQ":
                b_ret = "return !a.isNull(iB) && a.getBoolean(iB) != vB; "
            else:
                b_ret = "return !a.isNull(iB); "
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                f"if ({a_accept}) return true; "
                + b_ret
                + "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_binary_diff_and():
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            rej_a = REJECT_OP[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                "if (a.isNull(iA)) return false; "
                f"if (compareBinary(a.getBinary(iA), vA, signedA) {rej_a} 0) return false; "
                "if (a.isNull(iB)) return false; "
                f"return compareBinary(a.getBinary(iB), vB, signedB) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_binary_diff_or():
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                f"if (!a.isNull(iA) && compareBinary(a.getBinary(iA), vA, signedA) {sym_a} 0) return true; "
                f"return !a.isNull(iB) && compareBinary(a.getBinary(iB), vB, signedB) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_binary_range_and():
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                "if (a.isNull(i)) return false; "
                "byte[] v = a.getBinary(i); "
                f"return compareBinary(v, vA, signed) {sym_a} 0 && compareBinary(v, vB, signed) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


def idx_binary_range_or():
    out = []
    for opA in OPS:
        out.append(f"            case {opA} -> switch (opB) {{")
        for opB in OPS:
            sym_a = OP_SYMBOL[opA]
            sym_b = OP_SYMBOL[opB]
            out.append(
                f"                case {opB} -> row -> "
                "{ "
                "RowReader a = (RowReader) row; "
                "if (a.isNull(i)) return false; "
                "byte[] v = a.getBinary(i); "
                f"return compareBinary(v, vA, signed) {sym_a} 0 || compareBinary(v, vB, signed) {sym_b} 0; "
                "};"
            )
        out.append("            };")
    return "\n".join(out)


# ====================================================================
# File assembly.
# ====================================================================

LICENSE = """/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

"""


def emit_named_method(name, sig, body):
    return f"    private static RowMatcher {name}({sig}) {{\n        return switch (opA) {{\n{body}\n        }};\n    }}\n"


def build_named_file():
    parts = [LICENSE]
    parts.append("""import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.internal.predicate.RecordFilterCompiler.compareBinary;
import static dev.hardwood.internal.predicate.RecordFilterCompiler.leafName;
import static dev.hardwood.internal.predicate.RecordFilterCompiler.pathSegments;
import static dev.hardwood.internal.predicate.RecordFilterCompiler.resolve;

/// Arity-2 AND/OR fusion: when both children of a compound are simple
/// primitive leaves of compatible types, the compiler emits a single
/// fused matcher whose body inlines both comparisons as primitive
/// bytecode operations — no inner virtual call in the row loop.
///
/// Each `(typeA, opA, typeB, opB, connective)` produces a distinct
/// synthetic lambda class, so the fused matcher's body cannot be
/// polluted by other shapes and stays fast even when the generic
/// [RecordFilterCompiler] `And2Matcher` / `Or2Matcher` call site goes
/// megamorphic.
///
/// This file is generated by `tools/gen_fusion.py` — do not hand-edit.
final class RecordFilterFusion {

    private RecordFilterFusion() {
    }

    static RowMatcher tryFuseAnd2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema) {
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.LongPredicate lb) return fuseLongLongAnd(la, lb, schema);
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.IntPredicate ib) return fuseIntIntAnd(ia, ib, schema);
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.DoublePredicate db) return fuseDoubleDoubleAnd(da, db, schema);
        if (a instanceof ResolvedPredicate.BooleanPredicate ba && b instanceof ResolvedPredicate.BooleanPredicate bb) return fuseBooleanBooleanAnd(ba, bb, schema);
        if (a instanceof ResolvedPredicate.BinaryPredicate ba && b instanceof ResolvedPredicate.BinaryPredicate bb) return fuseBinaryBinaryAnd(ba, bb, schema);
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.DoublePredicate db) return fuseLongDoubleAndDiff(la, db, schema);
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.LongPredicate lb) return fuseLongDoubleAndDiff(lb, da, schema);
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.LongPredicate lb) return fuseIntLongAndDiff(ia, lb, schema);
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.IntPredicate ib) return fuseIntLongAndDiff(ib, la, schema);
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.DoublePredicate db) return fuseIntDoubleAndDiff(ia, db, schema);
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.IntPredicate ib) return fuseIntDoubleAndDiff(ib, da, schema);
        return null;
    }

    static RowMatcher tryFuseOr2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema) {
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.LongPredicate lb) return fuseLongLongOr(la, lb, schema);
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.IntPredicate ib) return fuseIntIntOr(ia, ib, schema);
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.DoublePredicate db) return fuseDoubleDoubleOr(da, db, schema);
        if (a instanceof ResolvedPredicate.BooleanPredicate ba && b instanceof ResolvedPredicate.BooleanPredicate bb) return fuseBooleanBooleanOr(ba, bb, schema);
        if (a instanceof ResolvedPredicate.BinaryPredicate ba && b instanceof ResolvedPredicate.BinaryPredicate bb) return fuseBinaryBinaryOr(ba, bb, schema);
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.DoublePredicate db) return fuseLongDoubleOrDiff(la, db, schema);
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.LongPredicate lb) return fuseLongDoubleOrDiff(lb, da, schema);
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.LongPredicate lb) return fuseIntLongOrDiff(ia, lb, schema);
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.IntPredicate ib) return fuseIntLongOrDiff(ib, la, schema);
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.DoublePredicate db) return fuseIntDoubleOrDiff(ia, db, schema);
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.IntPredicate ib) return fuseIntDoubleOrDiff(ib, da, schema);
        return null;
    }

    // ==================== same-type entry points ====================

    private static RowMatcher fuseLongLongAnd(ResolvedPredicate.LongPredicate la, ResolvedPredicate.LongPredicate lb, FileSchema schema) {
        String[] pA = pathSegments(schema, la.columnIndex()); String nA = leafName(schema, la.columnIndex());
        if (la.columnIndex() == lb.columnIndex()) return longRangeAnd(pA, nA, la.op(), la.value(), lb.op(), lb.value());
        String[] pB = pathSegments(schema, lb.columnIndex()); String nB = leafName(schema, lb.columnIndex());
        return longLongAndDiff(pA, nA, la.op(), la.value(), pB, nB, lb.op(), lb.value());
    }

    private static RowMatcher fuseLongLongOr(ResolvedPredicate.LongPredicate la, ResolvedPredicate.LongPredicate lb, FileSchema schema) {
        String[] pA = pathSegments(schema, la.columnIndex()); String nA = leafName(schema, la.columnIndex());
        if (la.columnIndex() == lb.columnIndex()) return longRangeOr(pA, nA, la.op(), la.value(), lb.op(), lb.value());
        String[] pB = pathSegments(schema, lb.columnIndex()); String nB = leafName(schema, lb.columnIndex());
        return longLongOrDiff(pA, nA, la.op(), la.value(), pB, nB, lb.op(), lb.value());
    }

    private static RowMatcher fuseIntIntAnd(ResolvedPredicate.IntPredicate ia, ResolvedPredicate.IntPredicate ib, FileSchema schema) {
        String[] pA = pathSegments(schema, ia.columnIndex()); String nA = leafName(schema, ia.columnIndex());
        if (ia.columnIndex() == ib.columnIndex()) return intRangeAnd(pA, nA, ia.op(), ia.value(), ib.op(), ib.value());
        String[] pB = pathSegments(schema, ib.columnIndex()); String nB = leafName(schema, ib.columnIndex());
        return intIntAndDiff(pA, nA, ia.op(), ia.value(), pB, nB, ib.op(), ib.value());
    }

    private static RowMatcher fuseIntIntOr(ResolvedPredicate.IntPredicate ia, ResolvedPredicate.IntPredicate ib, FileSchema schema) {
        String[] pA = pathSegments(schema, ia.columnIndex()); String nA = leafName(schema, ia.columnIndex());
        if (ia.columnIndex() == ib.columnIndex()) return intRangeOr(pA, nA, ia.op(), ia.value(), ib.op(), ib.value());
        String[] pB = pathSegments(schema, ib.columnIndex()); String nB = leafName(schema, ib.columnIndex());
        return intIntOrDiff(pA, nA, ia.op(), ia.value(), pB, nB, ib.op(), ib.value());
    }

    private static RowMatcher fuseDoubleDoubleAnd(ResolvedPredicate.DoublePredicate da, ResolvedPredicate.DoublePredicate db, FileSchema schema) {
        String[] pA = pathSegments(schema, da.columnIndex()); String nA = leafName(schema, da.columnIndex());
        if (da.columnIndex() == db.columnIndex()) return doubleRangeAnd(pA, nA, da.op(), da.value(), db.op(), db.value());
        String[] pB = pathSegments(schema, db.columnIndex()); String nB = leafName(schema, db.columnIndex());
        return doubleDoubleAndDiff(pA, nA, da.op(), da.value(), pB, nB, db.op(), db.value());
    }

    private static RowMatcher fuseDoubleDoubleOr(ResolvedPredicate.DoublePredicate da, ResolvedPredicate.DoublePredicate db, FileSchema schema) {
        String[] pA = pathSegments(schema, da.columnIndex()); String nA = leafName(schema, da.columnIndex());
        if (da.columnIndex() == db.columnIndex()) return doubleRangeOr(pA, nA, da.op(), da.value(), db.op(), db.value());
        String[] pB = pathSegments(schema, db.columnIndex()); String nB = leafName(schema, db.columnIndex());
        return doubleDoubleOrDiff(pA, nA, da.op(), da.value(), pB, nB, db.op(), db.value());
    }

    private static RowMatcher fuseBooleanBooleanAnd(ResolvedPredicate.BooleanPredicate ba, ResolvedPredicate.BooleanPredicate bb, FileSchema schema) {
        String[] pA = pathSegments(schema, ba.columnIndex()); String nA = leafName(schema, ba.columnIndex());
        String[] pB = pathSegments(schema, bb.columnIndex()); String nB = leafName(schema, bb.columnIndex());
        return booleanBooleanAndDiff(pA, nA, ba.op(), ba.value(), pB, nB, bb.op(), bb.value());
    }

    private static RowMatcher fuseBooleanBooleanOr(ResolvedPredicate.BooleanPredicate ba, ResolvedPredicate.BooleanPredicate bb, FileSchema schema) {
        String[] pA = pathSegments(schema, ba.columnIndex()); String nA = leafName(schema, ba.columnIndex());
        String[] pB = pathSegments(schema, bb.columnIndex()); String nB = leafName(schema, bb.columnIndex());
        return booleanBooleanOrDiff(pA, nA, ba.op(), ba.value(), pB, nB, bb.op(), bb.value());
    }

    private static RowMatcher fuseBinaryBinaryAnd(ResolvedPredicate.BinaryPredicate ba, ResolvedPredicate.BinaryPredicate bb, FileSchema schema) {
        String[] pA = pathSegments(schema, ba.columnIndex()); String nA = leafName(schema, ba.columnIndex());
        if (ba.columnIndex() == bb.columnIndex()) return binaryRangeAnd(pA, nA, ba.op(), ba.value(), bb.op(), bb.value(), ba.signed());
        String[] pB = pathSegments(schema, bb.columnIndex()); String nB = leafName(schema, bb.columnIndex());
        return binaryBinaryAndDiff(pA, nA, ba.op(), ba.value(), ba.signed(), pB, nB, bb.op(), bb.value(), bb.signed());
    }

    private static RowMatcher fuseBinaryBinaryOr(ResolvedPredicate.BinaryPredicate ba, ResolvedPredicate.BinaryPredicate bb, FileSchema schema) {
        String[] pA = pathSegments(schema, ba.columnIndex()); String nA = leafName(schema, ba.columnIndex());
        if (ba.columnIndex() == bb.columnIndex()) return binaryRangeOr(pA, nA, ba.op(), ba.value(), bb.op(), bb.value(), ba.signed());
        String[] pB = pathSegments(schema, bb.columnIndex()); String nB = leafName(schema, bb.columnIndex());
        return binaryBinaryOrDiff(pA, nA, ba.op(), ba.value(), ba.signed(), pB, nB, bb.op(), bb.value(), bb.signed());
    }

    // ==================== cross-type entry points ====================

    private static RowMatcher fuseLongDoubleAndDiff(ResolvedPredicate.LongPredicate la, ResolvedPredicate.DoublePredicate db, FileSchema schema) {
        String[] pA = pathSegments(schema, la.columnIndex()); String nA = leafName(schema, la.columnIndex());
        String[] pB = pathSegments(schema, db.columnIndex()); String nB = leafName(schema, db.columnIndex());
        return longDoubleAndDiff(pA, nA, la.op(), la.value(), pB, nB, db.op(), db.value());
    }

    private static RowMatcher fuseLongDoubleOrDiff(ResolvedPredicate.LongPredicate la, ResolvedPredicate.DoublePredicate db, FileSchema schema) {
        String[] pA = pathSegments(schema, la.columnIndex()); String nA = leafName(schema, la.columnIndex());
        String[] pB = pathSegments(schema, db.columnIndex()); String nB = leafName(schema, db.columnIndex());
        return longDoubleOrDiff(pA, nA, la.op(), la.value(), pB, nB, db.op(), db.value());
    }

    private static RowMatcher fuseIntLongAndDiff(ResolvedPredicate.IntPredicate ia, ResolvedPredicate.LongPredicate lb, FileSchema schema) {
        String[] pA = pathSegments(schema, ia.columnIndex()); String nA = leafName(schema, ia.columnIndex());
        String[] pB = pathSegments(schema, lb.columnIndex()); String nB = leafName(schema, lb.columnIndex());
        return intLongAndDiff(pA, nA, ia.op(), ia.value(), pB, nB, lb.op(), lb.value());
    }

    private static RowMatcher fuseIntLongOrDiff(ResolvedPredicate.IntPredicate ia, ResolvedPredicate.LongPredicate lb, FileSchema schema) {
        String[] pA = pathSegments(schema, ia.columnIndex()); String nA = leafName(schema, ia.columnIndex());
        String[] pB = pathSegments(schema, lb.columnIndex()); String nB = leafName(schema, lb.columnIndex());
        return intLongOrDiff(pA, nA, ia.op(), ia.value(), pB, nB, lb.op(), lb.value());
    }

    private static RowMatcher fuseIntDoubleAndDiff(ResolvedPredicate.IntPredicate ia, ResolvedPredicate.DoublePredicate db, FileSchema schema) {
        String[] pA = pathSegments(schema, ia.columnIndex()); String nA = leafName(schema, ia.columnIndex());
        String[] pB = pathSegments(schema, db.columnIndex()); String nB = leafName(schema, db.columnIndex());
        return intDoubleAndDiff(pA, nA, ia.op(), ia.value(), pB, nB, db.op(), db.value());
    }

    private static RowMatcher fuseIntDoubleOrDiff(ResolvedPredicate.IntPredicate ia, ResolvedPredicate.DoublePredicate db, FileSchema schema) {
        String[] pA = pathSegments(schema, ia.columnIndex()); String nA = leafName(schema, ia.columnIndex());
        String[] pB = pathSegments(schema, db.columnIndex()); String nB = leafName(schema, db.columnIndex());
        return intDoubleOrDiff(pA, nA, ia.op(), ia.value(), pB, nB, db.op(), db.value());
    }

    // ==================== generated lambda bodies ====================

""")

    # Generate body methods for each combo.
    methods = []

    methods.append(("longLongAndDiff",
        "String[] pA, String nA, Operator opA, long vA, String[] pB, String nB, Operator opB, long vB",
        name_diff_and("getLong", "long")))
    methods.append(("longLongOrDiff",
        "String[] pA, String nA, Operator opA, long vA, String[] pB, String nB, Operator opB, long vB",
        name_diff_or("getLong", "long")))
    methods.append(("longRangeAnd",
        "String[] p, String n, Operator opA, long vA, Operator opB, long vB",
        name_range_and("getLong", "long")))
    methods.append(("longRangeOr",
        "String[] p, String n, Operator opA, long vA, Operator opB, long vB",
        name_range_or("getLong", "long")))

    methods.append(("intIntAndDiff",
        "String[] pA, String nA, Operator opA, int vA, String[] pB, String nB, Operator opB, int vB",
        name_diff_and("getInt", "int")))
    methods.append(("intIntOrDiff",
        "String[] pA, String nA, Operator opA, int vA, String[] pB, String nB, Operator opB, int vB",
        name_diff_or("getInt", "int")))
    methods.append(("intRangeAnd",
        "String[] p, String n, Operator opA, int vA, Operator opB, int vB",
        name_range_and("getInt", "int")))
    methods.append(("intRangeOr",
        "String[] p, String n, Operator opA, int vA, Operator opB, int vB",
        name_range_or("getInt", "int")))

    methods.append(("doubleDoubleAndDiff",
        "String[] pA, String nA, Operator opA, double vA, String[] pB, String nB, Operator opB, double vB",
        name_double_diff_and("getDouble", "double", "Double")))
    methods.append(("doubleDoubleOrDiff",
        "String[] pA, String nA, Operator opA, double vA, String[] pB, String nB, Operator opB, double vB",
        name_double_diff_or("getDouble", "double", "Double")))
    methods.append(("doubleRangeAnd",
        "String[] p, String n, Operator opA, double vA, Operator opB, double vB",
        name_double_range_and("getDouble", "double", "Double")))
    methods.append(("doubleRangeOr",
        "String[] p, String n, Operator opA, double vA, Operator opB, double vB",
        name_double_range_or("getDouble", "double", "Double")))

    methods.append(("booleanBooleanAndDiff",
        "String[] pA, String nA, Operator opA, boolean vA, String[] pB, String nB, Operator opB, boolean vB",
        name_boolean_diff_and()))
    methods.append(("booleanBooleanOrDiff",
        "String[] pA, String nA, Operator opA, boolean vA, String[] pB, String nB, Operator opB, boolean vB",
        name_boolean_diff_or()))

    methods.append(("binaryBinaryAndDiff",
        "String[] pA, String nA, Operator opA, byte[] vA, boolean signedA, String[] pB, String nB, Operator opB, byte[] vB, boolean signedB",
        name_binary_diff_and()))
    methods.append(("binaryBinaryOrDiff",
        "String[] pA, String nA, Operator opA, byte[] vA, boolean signedA, String[] pB, String nB, Operator opB, byte[] vB, boolean signedB",
        name_binary_diff_or()))
    methods.append(("binaryRangeAnd",
        "String[] p, String n, Operator opA, byte[] vA, Operator opB, byte[] vB, boolean signed",
        name_binary_range_and()))
    methods.append(("binaryRangeOr",
        "String[] p, String n, Operator opA, byte[] vA, Operator opB, byte[] vB, boolean signed",
        name_binary_range_or()))

    # Cross-type. long+double: A=long (direct), B=double (Double.compare).
    methods.append(("longDoubleAndDiff",
        "String[] pA, String nA, Operator opA, long vA, String[] pB, String nB, Operator opB, double vB",
        name_cross_diff_and("getLong", "long", "getDouble", "double", b_uses_compare=True, b_cmp_class="Double")))
    methods.append(("longDoubleOrDiff",
        "String[] pA, String nA, Operator opA, long vA, String[] pB, String nB, Operator opB, double vB",
        name_cross_diff_or("getLong", "long", "getDouble", "double", b_uses_compare=True, b_cmp_class="Double")))

    # int+long: A=int (direct), B=long (direct).
    methods.append(("intLongAndDiff",
        "String[] pA, String nA, Operator opA, int vA, String[] pB, String nB, Operator opB, long vB",
        name_cross_diff_and("getInt", "int", "getLong", "long")))
    methods.append(("intLongOrDiff",
        "String[] pA, String nA, Operator opA, int vA, String[] pB, String nB, Operator opB, long vB",
        name_cross_diff_or("getInt", "int", "getLong", "long")))

    # int+double: A=int (direct), B=double (Double.compare).
    methods.append(("intDoubleAndDiff",
        "String[] pA, String nA, Operator opA, int vA, String[] pB, String nB, Operator opB, double vB",
        name_cross_diff_and("getInt", "int", "getDouble", "double", b_uses_compare=True, b_cmp_class="Double")))
    methods.append(("intDoubleOrDiff",
        "String[] pA, String nA, Operator opA, int vA, String[] pB, String nB, Operator opB, double vB",
        name_cross_diff_or("getInt", "int", "getDouble", "double", b_uses_compare=True, b_cmp_class="Double")))

    for name, sig, body in methods:
        parts.append(emit_named_method(name, sig, body))
        parts.append("\n")

    parts.append("}\n")
    return "".join(parts)


def build_indexed_file():
    parts = [LICENSE]
    parts.append("""import java.util.function.IntUnaryOperator;

import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.internal.predicate.RecordFilterCompiler.compareBinary;
import static dev.hardwood.internal.predicate.RecordFilterCompiler.indexedTopLevel;

/// Indexed-access counterpart to [RecordFilterFusion]. Used when the
/// reader is a [RowReader] (so the cast in the fused lambda is safe) and
/// both leaves operate on directly-addressable top-level columns. The
/// `topLevelFieldIndex` mapping returns the reader's field index for a
/// given file leaf-column index, or `-1` when the column isn't directly
/// addressable.
///
/// This file is generated by `tools/gen_fusion.py` — do not hand-edit.
final class RecordFilterFusionIndexed {

    private RecordFilterFusionIndexed() {
    }

    static RowMatcher tryFuseAnd2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema, IntUnaryOperator topLevelFieldIndex) {
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.LongPredicate lb) {
            int iA = indexedTopLevel(schema, la.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, lb.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            if (la.columnIndex() == lb.columnIndex()) return idxLongRangeAnd(iA, la.op(), la.value(), lb.op(), lb.value());
            return idxLongLongAndDiff(iA, la.op(), la.value(), iB, lb.op(), lb.value());
        }
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.IntPredicate ib) {
            int iA = indexedTopLevel(schema, ia.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, ib.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            if (ia.columnIndex() == ib.columnIndex()) return idxIntRangeAnd(iA, ia.op(), ia.value(), ib.op(), ib.value());
            return idxIntIntAndDiff(iA, ia.op(), ia.value(), iB, ib.op(), ib.value());
        }
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.DoublePredicate db) {
            int iA = indexedTopLevel(schema, da.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, db.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            if (da.columnIndex() == db.columnIndex()) return idxDoubleRangeAnd(iA, da.op(), da.value(), db.op(), db.value());
            return idxDoubleDoubleAndDiff(iA, da.op(), da.value(), iB, db.op(), db.value());
        }
        if (a instanceof ResolvedPredicate.BooleanPredicate ba && b instanceof ResolvedPredicate.BooleanPredicate bb) {
            int iA = indexedTopLevel(schema, ba.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, bb.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxBooleanBooleanAndDiff(iA, ba.op(), ba.value(), iB, bb.op(), bb.value());
        }
        if (a instanceof ResolvedPredicate.BinaryPredicate ba && b instanceof ResolvedPredicate.BinaryPredicate bb) {
            int iA = indexedTopLevel(schema, ba.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, bb.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            if (ba.columnIndex() == bb.columnIndex()) return idxBinaryRangeAnd(iA, ba.op(), ba.value(), bb.op(), bb.value(), ba.signed());
            return idxBinaryBinaryAndDiff(iA, ba.op(), ba.value(), ba.signed(), iB, bb.op(), bb.value(), bb.signed());
        }
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.DoublePredicate db) {
            int iA = indexedTopLevel(schema, la.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, db.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxLongDoubleAndDiff(iA, la.op(), la.value(), iB, db.op(), db.value());
        }
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.LongPredicate lb) {
            int iA = indexedTopLevel(schema, lb.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, da.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxLongDoubleAndDiff(iA, lb.op(), lb.value(), iB, da.op(), da.value());
        }
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.LongPredicate lb) {
            int iA = indexedTopLevel(schema, ia.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, lb.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxIntLongAndDiff(iA, ia.op(), ia.value(), iB, lb.op(), lb.value());
        }
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.IntPredicate ib) {
            int iA = indexedTopLevel(schema, ib.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, la.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxIntLongAndDiff(iA, ib.op(), ib.value(), iB, la.op(), la.value());
        }
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.DoublePredicate db) {
            int iA = indexedTopLevel(schema, ia.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, db.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxIntDoubleAndDiff(iA, ia.op(), ia.value(), iB, db.op(), db.value());
        }
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.IntPredicate ib) {
            int iA = indexedTopLevel(schema, ib.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, da.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxIntDoubleAndDiff(iA, ib.op(), ib.value(), iB, da.op(), da.value());
        }
        return null;
    }

    static RowMatcher tryFuseOr2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema, IntUnaryOperator topLevelFieldIndex) {
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.LongPredicate lb) {
            int iA = indexedTopLevel(schema, la.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, lb.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            if (la.columnIndex() == lb.columnIndex()) return idxLongRangeOr(iA, la.op(), la.value(), lb.op(), lb.value());
            return idxLongLongOrDiff(iA, la.op(), la.value(), iB, lb.op(), lb.value());
        }
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.IntPredicate ib) {
            int iA = indexedTopLevel(schema, ia.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, ib.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            if (ia.columnIndex() == ib.columnIndex()) return idxIntRangeOr(iA, ia.op(), ia.value(), ib.op(), ib.value());
            return idxIntIntOrDiff(iA, ia.op(), ia.value(), iB, ib.op(), ib.value());
        }
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.DoublePredicate db) {
            int iA = indexedTopLevel(schema, da.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, db.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            if (da.columnIndex() == db.columnIndex()) return idxDoubleRangeOr(iA, da.op(), da.value(), db.op(), db.value());
            return idxDoubleDoubleOrDiff(iA, da.op(), da.value(), iB, db.op(), db.value());
        }
        if (a instanceof ResolvedPredicate.BooleanPredicate ba && b instanceof ResolvedPredicate.BooleanPredicate bb) {
            int iA = indexedTopLevel(schema, ba.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, bb.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxBooleanBooleanOrDiff(iA, ba.op(), ba.value(), iB, bb.op(), bb.value());
        }
        if (a instanceof ResolvedPredicate.BinaryPredicate ba && b instanceof ResolvedPredicate.BinaryPredicate bb) {
            int iA = indexedTopLevel(schema, ba.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, bb.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            if (ba.columnIndex() == bb.columnIndex()) return idxBinaryRangeOr(iA, ba.op(), ba.value(), bb.op(), bb.value(), ba.signed());
            return idxBinaryBinaryOrDiff(iA, ba.op(), ba.value(), ba.signed(), iB, bb.op(), bb.value(), bb.signed());
        }
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.DoublePredicate db) {
            int iA = indexedTopLevel(schema, la.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, db.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxLongDoubleOrDiff(iA, la.op(), la.value(), iB, db.op(), db.value());
        }
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.LongPredicate lb) {
            int iA = indexedTopLevel(schema, lb.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, da.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxLongDoubleOrDiff(iA, lb.op(), lb.value(), iB, da.op(), da.value());
        }
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.LongPredicate lb) {
            int iA = indexedTopLevel(schema, ia.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, lb.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxIntLongOrDiff(iA, ia.op(), ia.value(), iB, lb.op(), lb.value());
        }
        if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.IntPredicate ib) {
            int iA = indexedTopLevel(schema, ib.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, la.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxIntLongOrDiff(iA, ib.op(), ib.value(), iB, la.op(), la.value());
        }
        if (a instanceof ResolvedPredicate.IntPredicate ia && b instanceof ResolvedPredicate.DoublePredicate db) {
            int iA = indexedTopLevel(schema, ia.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, db.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxIntDoubleOrDiff(iA, ia.op(), ia.value(), iB, db.op(), db.value());
        }
        if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.IntPredicate ib) {
            int iA = indexedTopLevel(schema, ib.columnIndex(), topLevelFieldIndex);
            int iB = indexedTopLevel(schema, da.columnIndex(), topLevelFieldIndex);
            if (iA < 0 || iB < 0) return null;
            return idxIntDoubleOrDiff(iA, ib.op(), ib.value(), iB, da.op(), da.value());
        }
        return null;
    }

    // ==================== generated lambda bodies ====================

""")

    methods = []
    methods.append(("idxLongLongAndDiff",
        "int iA, Operator opA, long vA, int iB, Operator opB, long vB",
        idx_diff_and("getLong", "long")))
    methods.append(("idxLongLongOrDiff",
        "int iA, Operator opA, long vA, int iB, Operator opB, long vB",
        idx_diff_or("getLong", "long")))
    methods.append(("idxLongRangeAnd",
        "int i, Operator opA, long vA, Operator opB, long vB",
        idx_range_and("getLong", "long")))
    methods.append(("idxLongRangeOr",
        "int i, Operator opA, long vA, Operator opB, long vB",
        idx_range_or("getLong", "long")))

    methods.append(("idxIntIntAndDiff",
        "int iA, Operator opA, int vA, int iB, Operator opB, int vB",
        idx_diff_and("getInt", "int")))
    methods.append(("idxIntIntOrDiff",
        "int iA, Operator opA, int vA, int iB, Operator opB, int vB",
        idx_diff_or("getInt", "int")))
    methods.append(("idxIntRangeAnd",
        "int i, Operator opA, int vA, Operator opB, int vB",
        idx_range_and("getInt", "int")))
    methods.append(("idxIntRangeOr",
        "int i, Operator opA, int vA, Operator opB, int vB",
        idx_range_or("getInt", "int")))

    methods.append(("idxDoubleDoubleAndDiff",
        "int iA, Operator opA, double vA, int iB, Operator opB, double vB",
        idx_double_diff_and("getDouble", "double", "Double")))
    methods.append(("idxDoubleDoubleOrDiff",
        "int iA, Operator opA, double vA, int iB, Operator opB, double vB",
        idx_double_diff_or("getDouble", "double", "Double")))
    methods.append(("idxDoubleRangeAnd",
        "int i, Operator opA, double vA, Operator opB, double vB",
        idx_double_range_and("getDouble", "double", "Double")))
    methods.append(("idxDoubleRangeOr",
        "int i, Operator opA, double vA, Operator opB, double vB",
        idx_double_range_or("getDouble", "double", "Double")))

    methods.append(("idxBooleanBooleanAndDiff",
        "int iA, Operator opA, boolean vA, int iB, Operator opB, boolean vB",
        idx_boolean_diff_and()))
    methods.append(("idxBooleanBooleanOrDiff",
        "int iA, Operator opA, boolean vA, int iB, Operator opB, boolean vB",
        idx_boolean_diff_or()))

    methods.append(("idxBinaryBinaryAndDiff",
        "int iA, Operator opA, byte[] vA, boolean signedA, int iB, Operator opB, byte[] vB, boolean signedB",
        idx_binary_diff_and()))
    methods.append(("idxBinaryBinaryOrDiff",
        "int iA, Operator opA, byte[] vA, boolean signedA, int iB, Operator opB, byte[] vB, boolean signedB",
        idx_binary_diff_or()))
    methods.append(("idxBinaryRangeAnd",
        "int i, Operator opA, byte[] vA, Operator opB, byte[] vB, boolean signed",
        idx_binary_range_and()))
    methods.append(("idxBinaryRangeOr",
        "int i, Operator opA, byte[] vA, Operator opB, byte[] vB, boolean signed",
        idx_binary_range_or()))

    methods.append(("idxLongDoubleAndDiff",
        "int iA, Operator opA, long vA, int iB, Operator opB, double vB",
        idx_cross_diff_and("getLong", "getDouble", b_uses_compare=True, b_cmp_class="Double")))
    methods.append(("idxLongDoubleOrDiff",
        "int iA, Operator opA, long vA, int iB, Operator opB, double vB",
        idx_cross_diff_or("getLong", "getDouble", b_uses_compare=True, b_cmp_class="Double")))

    methods.append(("idxIntLongAndDiff",
        "int iA, Operator opA, int vA, int iB, Operator opB, long vB",
        idx_cross_diff_and("getInt", "getLong")))
    methods.append(("idxIntLongOrDiff",
        "int iA, Operator opA, int vA, int iB, Operator opB, long vB",
        idx_cross_diff_or("getInt", "getLong")))

    methods.append(("idxIntDoubleAndDiff",
        "int iA, Operator opA, int vA, int iB, Operator opB, double vB",
        idx_cross_diff_and("getInt", "getDouble", b_uses_compare=True, b_cmp_class="Double")))
    methods.append(("idxIntDoubleOrDiff",
        "int iA, Operator opA, int vA, int iB, Operator opB, double vB",
        idx_cross_diff_or("getInt", "getDouble", b_uses_compare=True, b_cmp_class="Double")))

    for name, sig, body in methods:
        parts.append(f"    private static RowMatcher {name}({sig}) {{\n        return switch (opA) {{\n{body}\n        }};\n    }}\n\n")

    parts.append("}\n")
    return "".join(parts)


def main():
    repo = Path(__file__).resolve().parent.parent
    name_path = repo / "core/src/main/java/dev/hardwood/internal/predicate/RecordFilterFusion.java"
    idx_path = repo / "core/src/main/java/dev/hardwood/internal/predicate/RecordFilterFusionIndexed.java"
    name_path.write_text(build_named_file())
    idx_path.write_text(build_indexed_file())
    print(f"wrote {name_path}")
    print(f"wrote {idx_path}")


if __name__ == "__main__":
    main()
