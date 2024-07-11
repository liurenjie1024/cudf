package ai.rapids.cudf.serde.kudo2;

import ai.rapids.cudf.BufferType;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.Schema;
import ai.rapids.cudf.schema.SchemaWithColumnsVisitor;

import java.util.ArrayList;
import java.util.List;

class SerializedTableHeaderCalc implements SchemaWithColumnsVisitor<Void, SerializedTableHeader> {
    private final Kudo2Serializer kudo2Serializer;
    private final SliceInfo sliceInfo;
    private final List<Boolean> hasValidityBuffer = new ArrayList<>(1024);
    private long totalDataLen;
    private int columnCount;

    SerializedTableHeaderCalc(Kudo2Serializer kudo2Serializer, long rowOffset, long numRows) {
        this.kudo2Serializer = kudo2Serializer;
        this.sliceInfo = new SliceInfo(rowOffset, numRows);
        this.totalDataLen = 0;
        this.columnCount = 0;
    }

    @Override
    public SerializedTableHeader visitTopSchema(Schema schema, List<Void> children) {
        byte[] hasValidityBuffer = new byte[this.hasValidityBuffer.size()];
        for (int i = 0; i < this.hasValidityBuffer.size(); i++) {
            hasValidityBuffer[i] = (byte) (this.hasValidityBuffer.get(i) ? 1 : 0);
        }
        CompressionMode mode = kudo2Serializer.getCompressionMode();
        if (mode.equals(CompressionMode.AUTO)) {
            mode = kudo2Serializer.decide(columnCount, totalDataLen);
        }
        return new SerializedTableHeader(
                (int) sliceInfo.offset,
                (int) sliceInfo.rowCount,
                mode,
                totalDataLen,
                hasValidityBuffer,
                -1);
    }

    @Override
    public Void visitStruct(Schema structType, HostColumnVectorCore col, List<Void> children) {
        throw new UnsupportedOperationException("Structs are not supported in this context");
    }

    @Override
    public Void preVisitList(Schema listType, HostColumnVectorCore col) {
        throw new UnsupportedOperationException("Lists are not supported in this context");
    }

    @Override
    public Void visitList(Schema listType, HostColumnVectorCore col, Void preVisitResult, Void childResult) {
        throw new UnsupportedOperationException("Lists are not supported in this context");
    }


    @Override
    public Void visit(Schema primitiveType, HostColumnVectorCore col) {
        long validityBufferLen = calcPrimitiveDataLen(primitiveType, col, BufferType.VALIDITY);
        long offsetBufferLen = calcPrimitiveDataLen(primitiveType, col, BufferType.OFFSET);
        long dataBufferLen = calcPrimitiveDataLen(primitiveType, col, BufferType.DATA);

        this.columnCount += 1;
        this.totalDataLen += validityBufferLen + offsetBufferLen + dataBufferLen;

        hasValidityBuffer.add(col.getValidity() != null);

        return null;
    }

    private long calcPrimitiveDataLen(Schema primitiveType,
                                      HostColumnVectorCore col,
                                      BufferType bufferType) {
        switch (bufferType) {
            case VALIDITY:
                if (col.hasValidityVector()) {
                    return Kudo2Serializer.padFor64byteAlignment(sliceInfo.getValidityBufferInfo().getBufferLength());
                } else {
                    return 0;
                }
            case OFFSET:
                if (DType.STRING.equals(primitiveType.getType()) && sliceInfo.rowCount > 0) {
                    return Kudo2Serializer.padFor64byteAlignment((sliceInfo.rowCount + 1) * Integer.BYTES);
                } else {
                    return 0;
                }
            case DATA:
                if (DType.STRING.equals(primitiveType.getType())) {
                    long startByteOffset = col.getOffsets().getInt(sliceInfo.offset * Integer.BYTES);
                    long endByteOffset = col.getOffsets().getInt((sliceInfo.offset + sliceInfo.rowCount) * Integer.BYTES);
                    return Kudo2Serializer.padFor64byteAlignment(endByteOffset - startByteOffset);
                } else {
                    if (primitiveType.getType().getSizeInBytes() > 0) {
                        return Kudo2Serializer.padFor64byteAlignment(primitiveType.getType().getSizeInBytes() * sliceInfo.rowCount);
                    } else {
                        return 0;
                    }
                }
            default:
                throw new IllegalArgumentException("Unexpected buffer type: " + bufferType);

        }
    }
}
