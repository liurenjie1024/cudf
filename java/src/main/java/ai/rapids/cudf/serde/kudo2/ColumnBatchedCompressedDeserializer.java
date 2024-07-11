package ai.rapids.cudf.serde.kudo2;

import ai.rapids.cudf.*;
import ai.rapids.cudf.schema.SchemaVisitor;
import ai.rapids.cudf.utils.Arms;

import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

class ColumnBatchedCompressedDeserializer implements SchemaVisitor<ColumnView.NestedColumnVector, List<ColumnVector>>, AutoCloseable {
    private final SerializedTable table;
    private final HostMemoryBuffer hostBuffer;
    private final DeviceMemoryBuffer deviceBuffer;
    private final SliceInfo root;
    private long curOffset;
    private int currentIdx;

    public ColumnBatchedCompressedDeserializer(SerializedTable table) {
        this.table = table;
        this.root = new SliceInfo(table.getHeader().getOffset(), table.getHeader().getNumRows());

        this.hostBuffer = Arms.closeIfException(Kudo2Serializer.decompressHostBuffer(table.getBuffer(),
                table.getHeader().getTotalDataLen()), Function.identity());

        this.deviceBuffer = Arms.closeIfException(DeviceMemoryBuffer.allocate(table.getHeader().getTotalDataLen()), buffer -> {
            buffer.copyFromHostBuffer(hostBuffer, 0, table.getHeader().getTotalDataLen());
            return buffer;
        });

        this.curOffset = 0;
        this.currentIdx = 0;
    }

    @Override
    public List<ColumnVector> visitTopSchema(Schema schema, List<ColumnView.NestedColumnVector> children) {
        return children.stream().map(ColumnView.NestedColumnVector::toColumnVector).collect(Collectors.toList());
    }

    @Override
    public ColumnView.NestedColumnVector visitStruct(Schema structType, List<ColumnView.NestedColumnVector> children) {
        throw new UnsupportedOperationException("Struct not supported yet!");
    }

    @Override
    public ColumnView.NestedColumnVector preVisitList(Schema listType) {
        throw new UnsupportedOperationException("List not supported yet!");
    }

    @Override
    public ColumnView.NestedColumnVector visitList(Schema listType, ColumnView.NestedColumnVector preVisitResult, ColumnView.NestedColumnVector childResult) {
        throw new UnsupportedOperationException("List not supported yet!");
    }

    @Override
    public ColumnView.NestedColumnVector visit(Schema primitiveType) {
        DeviceMemoryBuffer validityBuffer = null;
        if (table.getHeader().hasValidityBuffer(currentIdx)) {
            validityBuffer = this.deserializeValidityBuffer();
        }

        DeviceMemoryBuffer offsetBuffer = null;
        int dataLen;
        if (primitiveType.getType().hasOffsets()) {
            if (root.rowCount > 0) {
                // Cuda always uses little endian
                int firstOffsetValue = hostBuffer
                        .asByteBuffer(curOffset, Integer.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .getInt();
                int lastOffsetValue = hostBuffer
                        .asByteBuffer(curOffset + (long) Integer.BYTES * root.rowCount, Integer.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .getInt();

                dataLen = lastOffsetValue - firstOffsetValue;
                offsetBuffer = this.deserializeOffsetBuffer(firstOffsetValue);
            } else {
                dataLen = 0;
                offsetBuffer = DeviceMemoryBuffer.allocate(0);
            }
        } else {
            dataLen = (int) (primitiveType.getType().getSizeInBytes() * root.rowCount);
        }

        DeviceMemoryBuffer dataBuffer;
        if (dataLen > 0) {
            dataBuffer = this.deserializeDataBuffer(dataLen);
        } else {
            dataBuffer = DeviceMemoryBuffer.allocate(0);
        }

        currentIdx += 1;
        return new ColumnView.NestedColumnVector(primitiveType.getType(), root.rowCount, Optional.empty(),
                dataBuffer, validityBuffer, offsetBuffer, Collections.emptyList());
    }

    private DeviceMemoryBuffer deserializeValidityBuffer() {
        long startAddress = deviceBuffer.getAddress() + curOffset;
        curOffset += Kudo2Serializer.padFor64byteAlignment(root.getValidityBufferInfo().getBufferLength());
        return ColumnVector.copyBitmask(startAddress,
                root.getValidityBufferInfo().getBeginBit(),
                root.getValidityBufferInfo().getEndBit());
    }

    private DeviceMemoryBuffer deserializeOffsetBuffer(int firstOffsetValue) {
        long bufferLen = (root.rowCount + 1) * Integer.BYTES;
        ColumnView offsetView = ColumnView.fromDeviceBuffer(deviceBuffer, this.curOffset, DType.INT32,
                (int) (root.rowCount + 1));
        try (Scalar first = Scalar.fromInt(firstOffsetValue)) {
            try (ColumnVector col = offsetView.sub(first, DType.INT32)) {
                this.curOffset += Kudo2Serializer.padFor64byteAlignment(bufferLen);
                return col.getData().sliceWithCopy(0, bufferLen);
            }
        }
    }

    private DeviceMemoryBuffer deserializeDataBuffer(long dataLen) {
        return Arms.closeIfException(deviceBuffer.sliceWithCopy(curOffset, dataLen), deviceDataBuffer -> {
            this.curOffset += Kudo2Serializer.padFor64byteAlignment(dataLen);
            return deviceDataBuffer;
        });
    }

    @Override
    public String toString() {
        return "NoCompressedColumnVectorDeserializer{" +
                "table=" + table +
                ", deviceBuffer=" + deviceBuffer +
                ", root=" + root +
                ", curOffset=" + curOffset +
                '}';
    }

    @Override
    public void close() throws Exception {
        if (this.table != null) {
            Arms.closeQuietly(this.table);
        }

        if (this.deviceBuffer != null) {
            Arms.closeQuietly(this.deviceBuffer);
        }

        if (this.hostBuffer != null) {
            Arms.closeQuietly(this.hostBuffer);
        }
    }
}
