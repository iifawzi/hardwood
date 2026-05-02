/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntUnaryOperator;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;

/// Bytecode-generation strategy for arity-2 AND/OR fusion.
///
/// Per query, ASM emits a hidden class implementing [RowMatcher] whose
/// `test(StructAccessor)` body is the fused comparison as raw bytecode —
/// no inner virtual call, every operator and operand baked into the
/// constant pool. The class is defined via
/// [MethodHandles.Lookup#defineHiddenClass(byte[], boolean)] with `true`
/// so it is GC-eligible when the resulting [RowMatcher] becomes
/// unreachable.
///
/// Two access modes are supported:
///
/// - **Indexed** ([tryFuseAnd2Indexed], [tryFuseOr2Indexed]) — the body
///   casts the row to [RowReader] and uses the `getX(int)` accessors.
///   Gated on top-level columns where the reader supplies a non-negative
///   field index.
/// - **Name-keyed** ([tryFuseAnd2], [tryFuseOr2]) — the body calls
///   [RecordFilterCompiler#resolve(StructAccessor, String[])] to walk
///   the intermediate struct path, then `getX(name)` on the resolved
///   accessor.
///
/// This class only handles the *bytecode emission* shape. The dispatch
/// table that decides which builder to invoke for a given
/// `(typeA, typeB, sameColumn)` triple is in this file as well, but the
/// per-builder operator switching is delegated to a small set of jump
/// emitters.
final class RecordFilterFusionBC {

    private static final String STRUCT_ACCESSOR = Type.getInternalName(StructAccessor.class);
    private static final String ROW_READER = Type.getInternalName(RowReader.class);
    private static final String ROW_MATCHER = Type.getInternalName(RowMatcher.class);
    private static final String COMPILER = Type.getInternalName(RecordFilterCompiler.class);
    private static final String PACKAGE = "dev/hardwood/internal/predicate/";
    private static final AtomicLong CLASS_COUNTER = new AtomicLong();

    private RecordFilterFusionBC() {
    }

    // ==================== Public entry points ====================

    static RowMatcher tryFuseAnd2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema) {
        return dispatchNameKeyed(a, b, schema, true);
    }

    static RowMatcher tryFuseOr2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema) {
        return dispatchNameKeyed(a, b, schema, false);
    }

    static RowMatcher tryFuseAnd2Indexed(ResolvedPredicate a, ResolvedPredicate b,
            FileSchema schema, IntUnaryOperator topLevelFieldIndex) {
        return dispatchIndexed(a, b, schema, topLevelFieldIndex, true);
    }

    static RowMatcher tryFuseOr2Indexed(ResolvedPredicate a, ResolvedPredicate b,
            FileSchema schema, IntUnaryOperator topLevelFieldIndex) {
        return dispatchIndexed(a, b, schema, topLevelFieldIndex, false);
    }

    // ==================== Indexed dispatch ====================

    private static RowMatcher dispatchIndexed(ResolvedPredicate a, ResolvedPredicate b,
            FileSchema schema, IntUnaryOperator topLevelFieldIndex, boolean isAnd) {
        if (a instanceof ResolvedPredicate.LongPredicate la
                && b instanceof ResolvedPredicate.LongPredicate lb
                && la.columnIndex() == lb.columnIndex()) {
            return buildLongSameColIndexed(idxOrNeg(schema, la.columnIndex(), topLevelFieldIndex),
                    la.op(), la.value(), lb.op(), lb.value(), isAnd);
        }
        if (a instanceof ResolvedPredicate.IntPredicate ia
                && b instanceof ResolvedPredicate.IntPredicate ib
                && ia.columnIndex() == ib.columnIndex()) {
            return buildIntSameColIndexed(idxOrNeg(schema, ia.columnIndex(), topLevelFieldIndex),
                    ia.op(), ia.value(), ib.op(), ib.value(), isAnd);
        }
        if (a instanceof ResolvedPredicate.FloatPredicate fa
                && b instanceof ResolvedPredicate.FloatPredicate fb
                && fa.columnIndex() == fb.columnIndex()) {
            return buildFloatSameColIndexed(idxOrNeg(schema, fa.columnIndex(), topLevelFieldIndex),
                    fa.op(), fa.value(), fb.op(), fb.value(), isAnd);
        }
        if (a instanceof ResolvedPredicate.DoublePredicate da
                && b instanceof ResolvedPredicate.DoublePredicate db
                && da.columnIndex() == db.columnIndex()) {
            return buildDoubleSameColIndexed(idxOrNeg(schema, da.columnIndex(), topLevelFieldIndex),
                    da.op(), da.value(), db.op(), db.value(), isAnd);
        }
        if (a instanceof ResolvedPredicate.BinaryPredicate ba
                && b instanceof ResolvedPredicate.BinaryPredicate bb
                && ba.columnIndex() == bb.columnIndex()
                && ba.signed() == bb.signed()) {
            return buildBinarySameColIndexed(idxOrNeg(schema, ba.columnIndex(), topLevelFieldIndex),
                    ba.op(), ba.value(), bb.op(), bb.value(), ba.signed(), isAnd);
        }
        if (a instanceof ResolvedPredicate.BooleanPredicate bla
                && b instanceof ResolvedPredicate.BooleanPredicate blb
                && bla.columnIndex() != blb.columnIndex()) {
            int idxA = idxOrNeg(schema, bla.columnIndex(), topLevelFieldIndex);
            int idxB = idxOrNeg(schema, blb.columnIndex(), topLevelFieldIndex);
            if (idxA < 0 || idxB < 0) return null;
            return buildBooleanDiffColIndexed(idxA, bla.op(), bla.value(),
                    idxB, blb.op(), blb.value(), isAnd);
        }
        // Cross-type diff-column. Canonicalise to (int, long), (int, double),
        // (long, double) so swapped orderings dispatch to one builder.
        // AND/OR are commutative for these leaves (both null-as-false, both
        // commutative connectives) — see _designs/RECORD_FILTER_FUSION.md.
        ResolvedPredicate.IntPredicate ip = pickInt(a, b);
        ResolvedPredicate.LongPredicate lp = pickLong(a, b);
        ResolvedPredicate.DoublePredicate dp = pickDouble(a, b);
        if (ip != null && lp != null && ip.columnIndex() != lp.columnIndex()) {
            int idxI = idxOrNeg(schema, ip.columnIndex(), topLevelFieldIndex);
            int idxL = idxOrNeg(schema, lp.columnIndex(), topLevelFieldIndex);
            if (idxI < 0 || idxL < 0) return null;
            return buildIntLongDiffColIndexed(idxI, ip.op(), ip.value(),
                    idxL, lp.op(), lp.value(), isAnd);
        }
        if (ip != null && dp != null && ip.columnIndex() != dp.columnIndex()) {
            int idxI = idxOrNeg(schema, ip.columnIndex(), topLevelFieldIndex);
            int idxD = idxOrNeg(schema, dp.columnIndex(), topLevelFieldIndex);
            if (idxI < 0 || idxD < 0) return null;
            return buildIntDoubleDiffColIndexed(idxI, ip.op(), ip.value(),
                    idxD, dp.op(), dp.value(), isAnd);
        }
        if (lp != null && dp != null && lp.columnIndex() != dp.columnIndex()) {
            int idxL = idxOrNeg(schema, lp.columnIndex(), topLevelFieldIndex);
            int idxD = idxOrNeg(schema, dp.columnIndex(), topLevelFieldIndex);
            if (idxL < 0 || idxD < 0) return null;
            return buildLongDoubleDiffColIndexed(idxL, lp.op(), lp.value(),
                    idxD, dp.op(), dp.value(), isAnd);
        }
        return null;
    }

    private static ResolvedPredicate.IntPredicate pickInt(ResolvedPredicate a, ResolvedPredicate b) {
        if (a instanceof ResolvedPredicate.IntPredicate ia) return ia;
        if (b instanceof ResolvedPredicate.IntPredicate ib) return ib;
        return null;
    }

    private static ResolvedPredicate.LongPredicate pickLong(ResolvedPredicate a, ResolvedPredicate b) {
        if (a instanceof ResolvedPredicate.LongPredicate la) return la;
        if (b instanceof ResolvedPredicate.LongPredicate lb) return lb;
        return null;
    }

    private static ResolvedPredicate.DoublePredicate pickDouble(ResolvedPredicate a, ResolvedPredicate b) {
        if (a instanceof ResolvedPredicate.DoublePredicate da) return da;
        if (b instanceof ResolvedPredicate.DoublePredicate db) return db;
        return null;
    }

    private static RowMatcher dispatchNameKeyed(ResolvedPredicate a, ResolvedPredicate b,
            FileSchema schema, boolean isAnd) {
        // Stub: name-keyed builders land in subsequent commits.
        return null;
    }

    /// Returns the reader field index for a top-level column, or `-1` if
    /// the leaf cannot use indexed access (no callback, non-top-level path,
    /// callback declines).
    private static int idxOrNeg(FileSchema schema, int columnIndex,
            IntUnaryOperator topLevelFieldIndex) {
        if (topLevelFieldIndex == null) return -1;
        if (schema.getColumn(columnIndex).fieldPath().elements().size() > 1) return -1;
        return topLevelFieldIndex.applyAsInt(columnIndex);
    }

    // ==================== Long+Long same-column (indexed) ====================

    /// Emits:
    /// ```java
    /// final class FusedLLSameIdx_N implements RowMatcher {
    ///   public boolean test(StructAccessor row) {
    ///     RowReader r = (RowReader) row;
    ///     if (r.isNull(<idx>)) return false;
    ///     long v = r.getLong(<idx>);
    ///     return v <opA> vA <connective> v <opB> vB;
    ///   }
    /// }
    /// ```
    private static RowMatcher buildLongSameColIndexed(int idx, Operator opA, long vA,
            Operator opB, long vB, boolean isAnd) {
        if (idx < 0) return null;
        String simpleName = (isAnd ? "FusedLLAndIdx_" : "FusedLLOrIdx_") + CLASS_COUNTER.incrementAndGet();
        String owner = PACKAGE + simpleName;
        ClassWriter cw = startClass(owner);
        emitNoArgCtor(cw);

        MethodVisitor mv = startTestMethod(cw);
        Label trueRet = new Label();
        Label falseRet = new Label();

        // RowReader r = (RowReader) row; (slot 2)
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, ROW_READER);
        mv.visitVarInsn(Opcodes.ASTORE, 2);

        // if (r.isNull(idx)) return false
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "isNull", "(I)Z", true);
        mv.visitJumpInsn(Opcodes.IFNE, falseRet);

        // long v = r.getLong(idx); (slot 3-4)
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getLong", "(I)J", true);
        mv.visitVarInsn(Opcodes.LSTORE, 3);

        emitLongPairBody(mv, 3, vA, opA, vB, opB, isAnd, trueRet, falseRet);

        endTestWithBranches(mv, trueRet, falseRet);
        return defineNoArg(cw);
    }

    /// Emits the AND/OR body for a pair of comparisons against the long in
    /// local slot `vSlot`. Uses `IFEQ`/`IFNE` jumps; for AND the first
    /// failing leaf falls to `falseRet`; for OR the first succeeding leaf
    /// jumps to `trueRet`.
    private static void emitLongPairBody(MethodVisitor mv, int vSlot,
            long vA, Operator opA, long vB, Operator opB,
            boolean isAnd, Label trueRet, Label falseRet) {
        if (isAnd) {
            // Leaf A: if !(v opA vA) goto falseRet
            mv.visitVarInsn(Opcodes.LLOAD, vSlot);
            mv.visitLdcInsn(vA);
            mv.visitInsn(Opcodes.LCMP);
            emitJumpIfFalse(mv, opA, falseRet);
            // Leaf B: if !(v opB vB) goto falseRet
            mv.visitVarInsn(Opcodes.LLOAD, vSlot);
            mv.visitLdcInsn(vB);
            mv.visitInsn(Opcodes.LCMP);
            emitJumpIfFalse(mv, opB, falseRet);
            // both succeeded → trueRet (fall through)
            mv.visitJumpInsn(Opcodes.GOTO, trueRet);
        } else {
            // Leaf A: if (v opA vA) goto trueRet
            mv.visitVarInsn(Opcodes.LLOAD, vSlot);
            mv.visitLdcInsn(vA);
            mv.visitInsn(Opcodes.LCMP);
            emitJumpIfTrue(mv, opA, trueRet);
            // Leaf B: if (v opB vB) goto trueRet
            mv.visitVarInsn(Opcodes.LLOAD, vSlot);
            mv.visitLdcInsn(vB);
            mv.visitInsn(Opcodes.LCMP);
            emitJumpIfTrue(mv, opB, trueRet);
            // both failed → falseRet (fall through)
            mv.visitJumpInsn(Opcodes.GOTO, falseRet);
        }
    }

    // ==================== Int+Int same-column (indexed) ====================

    private static RowMatcher buildIntSameColIndexed(int idx, Operator opA, int vA,
            Operator opB, int vB, boolean isAnd) {
        if (idx < 0) return null;
        String simpleName = (isAnd ? "FusedIIAndIdx_" : "FusedIIOrIdx_") + CLASS_COUNTER.incrementAndGet();
        String owner = PACKAGE + simpleName;
        ClassWriter cw = startClass(owner);
        emitNoArgCtor(cw);

        MethodVisitor mv = startTestMethod(cw);
        Label trueRet = new Label();
        Label falseRet = new Label();

        // RowReader r = (RowReader) row; (slot 2)
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, ROW_READER);
        mv.visitVarInsn(Opcodes.ASTORE, 2);

        // if (r.isNull(idx)) return false
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "isNull", "(I)Z", true);
        mv.visitJumpInsn(Opcodes.IFNE, falseRet);

        // int v = r.getInt(idx); (slot 3)
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getInt", "(I)I", true);
        mv.visitVarInsn(Opcodes.ISTORE, 3);

        emitIntPairBody(mv, 3, vA, opA, vB, opB, isAnd, trueRet, falseRet);
        endTestWithBranches(mv, trueRet, falseRet);
        return defineNoArg(cw);
    }

    /// Pair of `if (v <op> v?) ...` against the int in local slot `vSlot`.
    /// Uses `IF_ICMPxx` directly (no extra ICMP step), matching the
    /// canonical int-comparison shape javac emits.
    private static void emitIntPairBody(MethodVisitor mv, int vSlot,
            int vA, Operator opA, int vB, Operator opB,
            boolean isAnd, Label trueRet, Label falseRet) {
        if (isAnd) {
            mv.visitVarInsn(Opcodes.ILOAD, vSlot);
            mv.visitLdcInsn(vA);
            emitIntCmpJumpIfFalse(mv, opA, falseRet);
            mv.visitVarInsn(Opcodes.ILOAD, vSlot);
            mv.visitLdcInsn(vB);
            emitIntCmpJumpIfFalse(mv, opB, falseRet);
            mv.visitJumpInsn(Opcodes.GOTO, trueRet);
        } else {
            mv.visitVarInsn(Opcodes.ILOAD, vSlot);
            mv.visitLdcInsn(vA);
            emitIntCmpJumpIfTrue(mv, opA, trueRet);
            mv.visitVarInsn(Opcodes.ILOAD, vSlot);
            mv.visitLdcInsn(vB);
            emitIntCmpJumpIfTrue(mv, opB, trueRet);
            mv.visitJumpInsn(Opcodes.GOTO, falseRet);
        }
    }

    // ==================== Float+Float same-column (indexed) ====================

    private static RowMatcher buildFloatSameColIndexed(int idx, Operator opA, float vA,
            Operator opB, float vB, boolean isAnd) {
        if (idx < 0) return null;
        String simpleName = (isAnd ? "FusedFFAndIdx_" : "FusedFFOrIdx_") + CLASS_COUNTER.incrementAndGet();
        String owner = PACKAGE + simpleName;
        ClassWriter cw = startClass(owner);
        emitNoArgCtor(cw);

        MethodVisitor mv = startTestMethod(cw);
        Label trueRet = new Label();
        Label falseRet = new Label();

        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, ROW_READER);
        mv.visitVarInsn(Opcodes.ASTORE, 2);

        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "isNull", "(I)Z", true);
        mv.visitJumpInsn(Opcodes.IFNE, falseRet);

        // float v = r.getFloat(idx); (slot 3)
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getFloat", "(I)F", true);
        mv.visitVarInsn(Opcodes.FSTORE, 3);

        emitFloatPairBody(mv, 3, vA, opA, vB, opB, isAnd, trueRet, falseRet);
        endTestWithBranches(mv, trueRet, falseRet);
        return defineNoArg(cw);
    }

    /// Float comparisons use `Float.compare(F, F)I` to match the legacy
    /// NaN ordering exactly. Embedding `FCMPL` / `FCMPG` directly would
    /// match HotSpot's bytecode-spec for the source-level `<` etc., but
    /// `Float.compare` is what the legacy oracle uses and the
    /// equivalence test compares against it.
    private static void emitFloatPairBody(MethodVisitor mv, int vSlot,
            float vA, Operator opA, float vB, Operator opB,
            boolean isAnd, Label trueRet, Label falseRet) {
        if (isAnd) {
            mv.visitVarInsn(Opcodes.FLOAD, vSlot);
            mv.visitLdcInsn(vA);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Float", "compare", "(FF)I", false);
            emitJumpIfFalse(mv, opA, falseRet);
            mv.visitVarInsn(Opcodes.FLOAD, vSlot);
            mv.visitLdcInsn(vB);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Float", "compare", "(FF)I", false);
            emitJumpIfFalse(mv, opB, falseRet);
            mv.visitJumpInsn(Opcodes.GOTO, trueRet);
        } else {
            mv.visitVarInsn(Opcodes.FLOAD, vSlot);
            mv.visitLdcInsn(vA);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Float", "compare", "(FF)I", false);
            emitJumpIfTrue(mv, opA, trueRet);
            mv.visitVarInsn(Opcodes.FLOAD, vSlot);
            mv.visitLdcInsn(vB);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Float", "compare", "(FF)I", false);
            emitJumpIfTrue(mv, opB, trueRet);
            mv.visitJumpInsn(Opcodes.GOTO, falseRet);
        }
    }

    // ==================== Double+Double same-column (indexed) ====================

    private static RowMatcher buildDoubleSameColIndexed(int idx, Operator opA, double vA,
            Operator opB, double vB, boolean isAnd) {
        if (idx < 0) return null;
        String simpleName = (isAnd ? "FusedDDAndIdx_" : "FusedDDOrIdx_") + CLASS_COUNTER.incrementAndGet();
        String owner = PACKAGE + simpleName;
        ClassWriter cw = startClass(owner);
        emitNoArgCtor(cw);

        MethodVisitor mv = startTestMethod(cw);
        Label trueRet = new Label();
        Label falseRet = new Label();

        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, ROW_READER);
        mv.visitVarInsn(Opcodes.ASTORE, 2);

        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "isNull", "(I)Z", true);
        mv.visitJumpInsn(Opcodes.IFNE, falseRet);

        // double v = r.getDouble(idx); (slot 3-4)
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getDouble", "(I)D", true);
        mv.visitVarInsn(Opcodes.DSTORE, 3);

        emitDoublePairBody(mv, 3, vA, opA, vB, opB, isAnd, trueRet, falseRet);
        endTestWithBranches(mv, trueRet, falseRet);
        return defineNoArg(cw);
    }

    private static void emitDoublePairBody(MethodVisitor mv, int vSlot,
            double vA, Operator opA, double vB, Operator opB,
            boolean isAnd, Label trueRet, Label falseRet) {
        if (isAnd) {
            mv.visitVarInsn(Opcodes.DLOAD, vSlot);
            mv.visitLdcInsn(vA);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "compare", "(DD)I", false);
            emitJumpIfFalse(mv, opA, falseRet);
            mv.visitVarInsn(Opcodes.DLOAD, vSlot);
            mv.visitLdcInsn(vB);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "compare", "(DD)I", false);
            emitJumpIfFalse(mv, opB, falseRet);
            mv.visitJumpInsn(Opcodes.GOTO, trueRet);
        } else {
            mv.visitVarInsn(Opcodes.DLOAD, vSlot);
            mv.visitLdcInsn(vA);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "compare", "(DD)I", false);
            emitJumpIfTrue(mv, opA, trueRet);
            mv.visitVarInsn(Opcodes.DLOAD, vSlot);
            mv.visitLdcInsn(vB);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "compare", "(DD)I", false);
            emitJumpIfTrue(mv, opB, trueRet);
            mv.visitJumpInsn(Opcodes.GOTO, falseRet);
        }
    }

    // ==================== Binary+Binary same-column (indexed) ====================

    /// Binary fusion needs the operand byte arrays available at runtime.
    /// They are passed via an `(byte[] vA, byte[] vB)` constructor and
    /// stored in instance fields; the `signed` flag and column index are
    /// LDC'd into the constant pool. `compareBinary` is invoked statically
    /// against [RecordFilterCompiler] (package-private helper).
    private static RowMatcher buildBinarySameColIndexed(int idx, Operator opA, byte[] vA,
            Operator opB, byte[] vB, boolean signed, boolean isAnd) {
        if (idx < 0) return null;
        String simpleName = (isAnd ? "FusedBBAndIdx_" : "FusedBBOrIdx_") + CLASS_COUNTER.incrementAndGet();
        String owner = PACKAGE + simpleName;
        ClassWriter cw = startClass(owner);
        cw.visitField(Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL, "vA", "[B", null, null).visitEnd();
        cw.visitField(Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL, "vB", "[B", null, null).visitEnd();
        emitTwoByteArrayCtor(cw, owner);

        MethodVisitor mv = startTestMethod(cw);
        Label trueRet = new Label();
        Label falseRet = new Label();

        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, ROW_READER);
        mv.visitVarInsn(Opcodes.ASTORE, 2);

        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "isNull", "(I)Z", true);
        mv.visitJumpInsn(Opcodes.IFNE, falseRet);

        // byte[] v = r.getBinary(idx); (slot 3)
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getBinary", "(I)[B", true);
        mv.visitVarInsn(Opcodes.ASTORE, 3);

        emitBinaryPairBody(mv, owner, 3, opA, opB, signed, isAnd, trueRet, falseRet);
        endTestWithBranches(mv, trueRet, falseRet);
        return defineTwoByteArray(cw, vA, vB);
    }

    private static void emitBinaryPairBody(MethodVisitor mv, String owner, int vSlot,
            Operator opA, Operator opB, boolean signed,
            boolean isAnd, Label trueRet, Label falseRet) {
        // Each "leaf" emits: compareBinary(v, this.v?, signed) [int on stack],
        // followed by an IFxx jump on that int.
        if (isAnd) {
            emitBinaryCompare(mv, owner, vSlot, "vA", signed);
            emitJumpIfFalse(mv, opA, falseRet);
            emitBinaryCompare(mv, owner, vSlot, "vB", signed);
            emitJumpIfFalse(mv, opB, falseRet);
            mv.visitJumpInsn(Opcodes.GOTO, trueRet);
        } else {
            emitBinaryCompare(mv, owner, vSlot, "vA", signed);
            emitJumpIfTrue(mv, opA, trueRet);
            emitBinaryCompare(mv, owner, vSlot, "vB", signed);
            emitJumpIfTrue(mv, opB, trueRet);
            mv.visitJumpInsn(Opcodes.GOTO, falseRet);
        }
    }

    private static void emitBinaryCompare(MethodVisitor mv, String owner, int vSlot,
            String fieldName, boolean signed) {
        mv.visitVarInsn(Opcodes.ALOAD, vSlot);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, owner, fieldName, "[B");
        mv.visitInsn(signed ? Opcodes.ICONST_1 : Opcodes.ICONST_0);
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, COMPILER, "compareBinary", "([B[BZ)I", false);
    }

    private static void emitTwoByteArrayCtor(ClassWriter cw, String owner) {
        MethodVisitor ctor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "([B[B)V", null, null);
        ctor.visitCode();
        ctor.visitVarInsn(Opcodes.ALOAD, 0);
        ctor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        ctor.visitVarInsn(Opcodes.ALOAD, 0);
        ctor.visitVarInsn(Opcodes.ALOAD, 1);
        ctor.visitFieldInsn(Opcodes.PUTFIELD, owner, "vA", "[B");
        ctor.visitVarInsn(Opcodes.ALOAD, 0);
        ctor.visitVarInsn(Opcodes.ALOAD, 2);
        ctor.visitFieldInsn(Opcodes.PUTFIELD, owner, "vB", "[B");
        ctor.visitInsn(Opcodes.RETURN);
        ctor.visitMaxs(0, 0);
        ctor.visitEnd();
    }

    private static RowMatcher defineTwoByteArray(ClassWriter cw, byte[] vA, byte[] vB) {
        cw.visitEnd();
        try {
            Class<?> hidden = MethodHandles.lookup()
                    .defineHiddenClass(cw.toByteArray(), true).lookupClass();
            return (RowMatcher) hidden.getDeclaredConstructor(byte[].class, byte[].class)
                    .newInstance(vA, vB);
        } catch (IllegalAccessException | NoSuchMethodException | InstantiationException
                | InvocationTargetException e) {
            throw new IllegalStateException("failed to define hidden RowMatcher class", e);
        }
    }

    // ==================== Cross-type diff-column (indexed) ====================

    /// Emits the prologue shared by all diff-column indexed builders:
    /// casts `row` (slot 1) to `RowReader` and stores it in slot 2.
    /// Returns `mv` for fluency.
    private static MethodVisitor emitIndexedRowCast(MethodVisitor mv) {
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, ROW_READER);
        mv.visitVarInsn(Opcodes.ASTORE, 2);
        return mv;
    }

    /// Emits `if (rowSlot.isNull(idx)) goto target;` against the row in
    /// local slot `rowSlot`. The row must be a `RowReader`.
    private static void emitIndexedNullThenJump(MethodVisitor mv, int rowSlot, int idx, Label target) {
        mv.visitVarInsn(Opcodes.ALOAD, rowSlot);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "isNull", "(I)Z", true);
        mv.visitJumpInsn(Opcodes.IFNE, target);
    }

    private static RowMatcher buildIntLongDiffColIndexed(int idxI, Operator opI, int vI,
            int idxL, Operator opL, long vL, boolean isAnd) {
        String simpleName = (isAnd ? "FusedILAndDiffIdx_" : "FusedILOrDiffIdx_")
                + CLASS_COUNTER.incrementAndGet();
        String owner = PACKAGE + simpleName;
        ClassWriter cw = startClass(owner);
        emitNoArgCtor(cw);

        MethodVisitor mv = startTestMethod(cw);
        Label trueRet = new Label();
        Label falseRet = new Label();
        emitIndexedRowCast(mv);

        if (isAnd) {
            emitIndexedNullThenJump(mv, 2, idxI, falseRet);
            // int v = r.getInt(idxI); if (!(v opI vI)) goto false
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxI);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getInt", "(I)I", true);
            mv.visitLdcInsn(vI);
            emitIntCmpJumpIfFalse(mv, opI, falseRet);

            emitIndexedNullThenJump(mv, 2, idxL, falseRet);
            // long v = r.getLong(idxL); if (!(v opL vL)) goto false
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxL);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getLong", "(I)J", true);
            mv.visitLdcInsn(vL);
            mv.visitInsn(Opcodes.LCMP);
            emitJumpIfFalse(mv, opL, falseRet);
            mv.visitJumpInsn(Opcodes.GOTO, trueRet);
        } else {
            Label afterI = new Label();
            // if (r.isNull(idxI)) skip leaf I
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxI);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "isNull", "(I)Z", true);
            mv.visitJumpInsn(Opcodes.IFNE, afterI);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxI);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getInt", "(I)I", true);
            mv.visitLdcInsn(vI);
            emitIntCmpJumpIfTrue(mv, opI, trueRet);
            mv.visitLabel(afterI);

            Label afterL = new Label();
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxL);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "isNull", "(I)Z", true);
            mv.visitJumpInsn(Opcodes.IFNE, afterL);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxL);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getLong", "(I)J", true);
            mv.visitLdcInsn(vL);
            mv.visitInsn(Opcodes.LCMP);
            emitJumpIfTrue(mv, opL, trueRet);
            mv.visitLabel(afterL);
            mv.visitJumpInsn(Opcodes.GOTO, falseRet);
        }
        endTestWithBranches(mv, trueRet, falseRet);
        return defineNoArg(cw);
    }

    private static RowMatcher buildIntDoubleDiffColIndexed(int idxI, Operator opI, int vI,
            int idxD, Operator opD, double vD, boolean isAnd) {
        String simpleName = (isAnd ? "FusedIDAndDiffIdx_" : "FusedIDOrDiffIdx_")
                + CLASS_COUNTER.incrementAndGet();
        String owner = PACKAGE + simpleName;
        ClassWriter cw = startClass(owner);
        emitNoArgCtor(cw);

        MethodVisitor mv = startTestMethod(cw);
        Label trueRet = new Label();
        Label falseRet = new Label();
        emitIndexedRowCast(mv);

        if (isAnd) {
            emitIndexedNullThenJump(mv, 2, idxI, falseRet);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxI);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getInt", "(I)I", true);
            mv.visitLdcInsn(vI);
            emitIntCmpJumpIfFalse(mv, opI, falseRet);

            emitIndexedNullThenJump(mv, 2, idxD, falseRet);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxD);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getDouble", "(I)D", true);
            mv.visitLdcInsn(vD);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "compare", "(DD)I", false);
            emitJumpIfFalse(mv, opD, falseRet);
            mv.visitJumpInsn(Opcodes.GOTO, trueRet);
        } else {
            Label afterI = new Label();
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxI);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "isNull", "(I)Z", true);
            mv.visitJumpInsn(Opcodes.IFNE, afterI);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxI);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getInt", "(I)I", true);
            mv.visitLdcInsn(vI);
            emitIntCmpJumpIfTrue(mv, opI, trueRet);
            mv.visitLabel(afterI);

            Label afterD = new Label();
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxD);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "isNull", "(I)Z", true);
            mv.visitJumpInsn(Opcodes.IFNE, afterD);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxD);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getDouble", "(I)D", true);
            mv.visitLdcInsn(vD);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "compare", "(DD)I", false);
            emitJumpIfTrue(mv, opD, trueRet);
            mv.visitLabel(afterD);
            mv.visitJumpInsn(Opcodes.GOTO, falseRet);
        }
        endTestWithBranches(mv, trueRet, falseRet);
        return defineNoArg(cw);
    }

    private static RowMatcher buildLongDoubleDiffColIndexed(int idxL, Operator opL, long vL,
            int idxD, Operator opD, double vD, boolean isAnd) {
        String simpleName = (isAnd ? "FusedLDAndDiffIdx_" : "FusedLDOrDiffIdx_")
                + CLASS_COUNTER.incrementAndGet();
        String owner = PACKAGE + simpleName;
        ClassWriter cw = startClass(owner);
        emitNoArgCtor(cw);

        MethodVisitor mv = startTestMethod(cw);
        Label trueRet = new Label();
        Label falseRet = new Label();
        emitIndexedRowCast(mv);

        if (isAnd) {
            emitIndexedNullThenJump(mv, 2, idxL, falseRet);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxL);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getLong", "(I)J", true);
            mv.visitLdcInsn(vL);
            mv.visitInsn(Opcodes.LCMP);
            emitJumpIfFalse(mv, opL, falseRet);

            emitIndexedNullThenJump(mv, 2, idxD, falseRet);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxD);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getDouble", "(I)D", true);
            mv.visitLdcInsn(vD);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "compare", "(DD)I", false);
            emitJumpIfFalse(mv, opD, falseRet);
            mv.visitJumpInsn(Opcodes.GOTO, trueRet);
        } else {
            Label afterL = new Label();
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxL);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "isNull", "(I)Z", true);
            mv.visitJumpInsn(Opcodes.IFNE, afterL);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxL);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getLong", "(I)J", true);
            mv.visitLdcInsn(vL);
            mv.visitInsn(Opcodes.LCMP);
            emitJumpIfTrue(mv, opL, trueRet);
            mv.visitLabel(afterL);

            Label afterD = new Label();
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxD);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "isNull", "(I)Z", true);
            mv.visitJumpInsn(Opcodes.IFNE, afterD);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitLdcInsn(idxD);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getDouble", "(I)D", true);
            mv.visitLdcInsn(vD);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "compare", "(DD)I", false);
            emitJumpIfTrue(mv, opD, trueRet);
            mv.visitLabel(afterD);
            mv.visitJumpInsn(Opcodes.GOTO, falseRet);
        }
        endTestWithBranches(mv, trueRet, falseRet);
        return defineNoArg(cw);
    }

    // ==================== Boolean+Boolean diff-column (indexed) ====================

    /// Boolean leaves: only `EQ` / `NOT_EQ` honour the value; any other
    /// operator reduces to a non-null check, matching the legacy default
    /// arm in [RecordFilterCompiler#booleanLeaf]. The fused body emits the
    /// boolean comparison only when the operator carries it; otherwise the
    /// non-null gate is the entire leaf.
    private static RowMatcher buildBooleanDiffColIndexed(int idxA, Operator opA, boolean vA,
            int idxB, Operator opB, boolean vB, boolean isAnd) {
        String simpleName = (isAnd ? "FusedBoolAndDiffIdx_" : "FusedBoolOrDiffIdx_")
                + CLASS_COUNTER.incrementAndGet();
        String owner = PACKAGE + simpleName;
        ClassWriter cw = startClass(owner);
        emitNoArgCtor(cw);

        MethodVisitor mv = startTestMethod(cw);
        Label trueRet = new Label();
        Label falseRet = new Label();
        emitIndexedRowCast(mv);

        if (isAnd) {
            emitBooleanLeafAnd(mv, 2, idxA, opA, vA, falseRet);
            emitBooleanLeafAnd(mv, 2, idxB, opB, vB, falseRet);
            mv.visitJumpInsn(Opcodes.GOTO, trueRet);
        } else {
            emitBooleanLeafOr(mv, 2, idxA, opA, vA, trueRet);
            emitBooleanLeafOr(mv, 2, idxB, opB, vB, trueRet);
            mv.visitJumpInsn(Opcodes.GOTO, falseRet);
        }
        endTestWithBranches(mv, trueRet, falseRet);
        return defineNoArg(cw);
    }

    /// AND form: leaf falsity (null OR cmp-fail) jumps to `falseRet`.
    private static void emitBooleanLeafAnd(MethodVisitor mv, int rowSlot, int idx,
            Operator op, boolean v, Label falseRet) {
        emitIndexedNullThenJump(mv, rowSlot, idx, falseRet);
        if (op != Operator.EQ && op != Operator.NOT_EQ) {
            // Non-EQ/NOT_EQ ops on boolean reduce to "non-null" — the
            // null check above is the entire leaf.
            return;
        }
        mv.visitVarInsn(Opcodes.ALOAD, rowSlot);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getBoolean", "(I)Z", true);
        // Stack: int (0/1). If `op == EQ`, want it to equal `v`; else differ.
        if (op == Operator.EQ) {
            // If got != v → jump to false.
            mv.visitJumpInsn(v ? Opcodes.IFEQ : Opcodes.IFNE, falseRet);
        } else { // NOT_EQ
            // If got == v → jump to false.
            mv.visitJumpInsn(v ? Opcodes.IFNE : Opcodes.IFEQ, falseRet);
        }
    }

    /// OR form: leaf truth (non-null AND cmp-success) jumps to `trueRet`.
    private static void emitBooleanLeafOr(MethodVisitor mv, int rowSlot, int idx,
            Operator op, boolean v, Label trueRet) {
        Label afterLeaf = new Label();
        // if (r.isNull(idx)) skip
        mv.visitVarInsn(Opcodes.ALOAD, rowSlot);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "isNull", "(I)Z", true);
        mv.visitJumpInsn(Opcodes.IFNE, afterLeaf);
        if (op != Operator.EQ && op != Operator.NOT_EQ) {
            // Non-null leaf with non-EQ/NOT_EQ op → leaf is true.
            mv.visitJumpInsn(Opcodes.GOTO, trueRet);
            mv.visitLabel(afterLeaf);
            return;
        }
        mv.visitVarInsn(Opcodes.ALOAD, rowSlot);
        mv.visitLdcInsn(idx);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, ROW_READER, "getBoolean", "(I)Z", true);
        if (op == Operator.EQ) {
            // got == v → trueRet
            mv.visitJumpInsn(v ? Opcodes.IFNE : Opcodes.IFEQ, trueRet);
        } else { // NOT_EQ
            mv.visitJumpInsn(v ? Opcodes.IFEQ : Opcodes.IFNE, trueRet);
        }
        mv.visitLabel(afterLeaf);
    }

    // ==================== Int comparison-jump helpers ====================

    /// Stack: ..., int a, int b → consumes both; jump to `target` if the
    /// relation `a <op> b` does NOT hold.
    private static void emitIntCmpJumpIfFalse(MethodVisitor mv, Operator op, Label target) {
        switch (op) {
            case EQ -> mv.visitJumpInsn(Opcodes.IF_ICMPNE, target);
            case NOT_EQ -> mv.visitJumpInsn(Opcodes.IF_ICMPEQ, target);
            case LT -> mv.visitJumpInsn(Opcodes.IF_ICMPGE, target);
            case LT_EQ -> mv.visitJumpInsn(Opcodes.IF_ICMPGT, target);
            case GT -> mv.visitJumpInsn(Opcodes.IF_ICMPLE, target);
            case GT_EQ -> mv.visitJumpInsn(Opcodes.IF_ICMPLT, target);
        }
    }

    private static void emitIntCmpJumpIfTrue(MethodVisitor mv, Operator op, Label target) {
        switch (op) {
            case EQ -> mv.visitJumpInsn(Opcodes.IF_ICMPEQ, target);
            case NOT_EQ -> mv.visitJumpInsn(Opcodes.IF_ICMPNE, target);
            case LT -> mv.visitJumpInsn(Opcodes.IF_ICMPLT, target);
            case LT_EQ -> mv.visitJumpInsn(Opcodes.IF_ICMPLE, target);
            case GT -> mv.visitJumpInsn(Opcodes.IF_ICMPGT, target);
            case GT_EQ -> mv.visitJumpInsn(Opcodes.IF_ICMPGE, target);
        }
    }

    // ==================== Common bytecode helpers ====================

    private static ClassWriter startClass(String owner) {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V21, Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
                owner, null, "java/lang/Object", new String[] { ROW_MATCHER });
        return cw;
    }

    private static void emitNoArgCtor(ClassWriter cw) {
        MethodVisitor ctor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        ctor.visitCode();
        ctor.visitVarInsn(Opcodes.ALOAD, 0);
        ctor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        ctor.visitInsn(Opcodes.RETURN);
        ctor.visitMaxs(0, 0);
        ctor.visitEnd();
    }

    private static MethodVisitor startTestMethod(ClassWriter cw) {
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "test",
                "(L" + STRUCT_ACCESSOR + ";)Z", null, null);
        mv.visitCode();
        return mv;
    }

    /// Emits `trueRet: ICONST_1, IRETURN; falseRet: ICONST_0, IRETURN;`
    /// and closes the method.
    private static void endTestWithBranches(MethodVisitor mv, Label trueRet, Label falseRet) {
        mv.visitLabel(trueRet);
        mv.visitInsn(Opcodes.ICONST_1);
        mv.visitInsn(Opcodes.IRETURN);
        mv.visitLabel(falseRet);
        mv.visitInsn(Opcodes.ICONST_0);
        mv.visitInsn(Opcodes.IRETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    /// Stack: ..., int cmp (-1/0/1) → consumed; jump to `target` if the
    /// relation `cmp <op> 0` does NOT hold.
    private static void emitJumpIfFalse(MethodVisitor mv, Operator op, Label target) {
        switch (op) {
            case EQ -> mv.visitJumpInsn(Opcodes.IFNE, target);
            case NOT_EQ -> mv.visitJumpInsn(Opcodes.IFEQ, target);
            case LT -> mv.visitJumpInsn(Opcodes.IFGE, target);
            case LT_EQ -> mv.visitJumpInsn(Opcodes.IFGT, target);
            case GT -> mv.visitJumpInsn(Opcodes.IFLE, target);
            case GT_EQ -> mv.visitJumpInsn(Opcodes.IFLT, target);
        }
    }

    /// Stack: ..., int cmp (-1/0/1) → consumed; jump to `target` if the
    /// relation `cmp <op> 0` DOES hold.
    private static void emitJumpIfTrue(MethodVisitor mv, Operator op, Label target) {
        switch (op) {
            case EQ -> mv.visitJumpInsn(Opcodes.IFEQ, target);
            case NOT_EQ -> mv.visitJumpInsn(Opcodes.IFNE, target);
            case LT -> mv.visitJumpInsn(Opcodes.IFLT, target);
            case LT_EQ -> mv.visitJumpInsn(Opcodes.IFLE, target);
            case GT -> mv.visitJumpInsn(Opcodes.IFGT, target);
            case GT_EQ -> mv.visitJumpInsn(Opcodes.IFGE, target);
        }
    }

    // ==================== Hidden class definition ====================

    private static RowMatcher defineNoArg(ClassWriter cw) {
        cw.visitEnd();
        try {
            Class<?> hidden = MethodHandles.lookup()
                    .defineHiddenClass(cw.toByteArray(), true).lookupClass();
            return (RowMatcher) hidden.getDeclaredConstructor().newInstance();
        } catch (IllegalAccessException | NoSuchMethodException | InstantiationException
                | InvocationTargetException e) {
            throw new IllegalStateException("failed to define hidden RowMatcher class", e);
        }
    }
}
