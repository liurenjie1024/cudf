package ai.rapids.cudf.serde.kudo2;

import ai.rapids.cudf.*;
import ai.rapids.cudf.schema.SchemaVisitor;
import ai.rapids.cudf.utils.Arms;
import ai.rapids.cudf.utils.ByteBufferInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class BufferCompressedDeserializer implements SchemaVisitor<ColumnView.NestedColumnVector, List<ColumnVector>>, AutoCloseable {
    private final SerializedTable table;
    private final SliceInfo root;
    private long curOffset;
    private int currentIdx;

    public BufferCompressedDeserializer(SerializedTable table) {
        this.table = table;
        this.root = new SliceInfo(table.getHeader().getOffset(), table.getHeader().getNumRows());

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
        if (primitiveType.getType().hasOffsets()) {
            if (root.rowCount > 0) {
                offsetBuffer = deserializeOffsetBuffer();
            } else {
                offsetBuffer = DeviceMemoryBuffer.allocate(0);
            }
        }

        DeviceMemoryBuffer dataBuffer = this.deserializeDataBuffer();

        currentIdx += 1;
        return new ColumnView.NestedColumnVector(primitiveType.getType(), root.rowCount, Optional.empty(),
                dataBuffer, validityBuffer, offsetBuffer, Collections.emptyList());
    }

    private DeviceMemoryBuffer deserializeValidityBuffer() {
        try (DeviceMemoryBuffer buffer = decompressToDeviceBuffer()) {
            return ColumnVector.copyBitmask(buffer.getAddress(),
                    root.getValidityBufferInfo().getBeginBit(),
                    root.getValidityBufferInfo().getEndBit());
        }
    }


    private DeviceMemoryBuffer deserializeOffsetBuffer() {
        try (HostMemoryBuffer hostBuffer = decompressBuffer()) {
            int firstOffsetValue = hostBuffer.asByteBuffer(0, Integer.BYTES)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getInt();
            long bufferLen = (root.rowCount + 1) * Integer.BYTES;
            try (DeviceMemoryBuffer deviceBuffer = DeviceMemoryBuffer.allocate(hostBuffer.getLength())) {
                deviceBuffer.copyFromHostBuffer(hostBuffer);
                ColumnView offsetView = ColumnView.fromDeviceBuffer(deviceBuffer, 0, DType.INT32,
                        (int) (root.rowCount + 1));
                try (Scalar first = Scalar.fromInt(firstOffsetValue)) {
                    try (ColumnVector col = offsetView.sub(first, DType.INT32)) {
                        return col.getData().sliceWithCopy(0, bufferLen);
                    }
                }
            }
        }
    }

    private HostMemoryBuffer decompressBuffer() {
        long decompressedLen = table.getBuffer()
                .asByteBuffer(curOffset, Long.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .getLong();
        long compressedLen = table.getBuffer()
                .asByteBuffer(curOffset + Long.BYTES, Long.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .getLong();

        curOffset += 2 * Long.BYTES;

        try (HostMemoryBuffer compressedBuffer = table.getBuffer().slice(curOffset, compressedLen)) {
            try (InputStream is = ByteBufferInputStream.wrap(compressedBuffer.asByteBuffer())) {
                try (ZstdCompressorInputStream in = new ZstdCompressorInputStream(is)) {
                    return Arms.closeIfException(HostMemoryBuffer.allocate(decompressedLen), buffer -> {
                        try {
                            buffer.copyFromStream(0, in, decompressedLen);
                            curOffset += compressedLen;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        return buffer;
                    });
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DeviceMemoryBuffer decompressToDeviceBuffer() {
        try (HostMemoryBuffer hostBuffer = decompressBuffer()) {
            DeviceMemoryBuffer deviceBuffer = DeviceMemoryBuffer.allocate(hostBuffer.getLength());
            deviceBuffer.copyFromHostBuffer(hostBuffer);
            return deviceBuffer;
        }
    }

    private DeviceMemoryBuffer deserializeDataBuffer() {
        return decompressToDeviceBuffer();
    }


    @Override
    public String toString() {
        return "CompressedColumnVectorDeserializer{" +
                "table=" + table +
                ", root=" + root +
                ", curOffset=" + curOffset +
                ", currentIdx=" + currentIdx +
                '}';
    }

    @Override
    public void close() throws Exception {
        if (this.table != null) {
            Arms.closeQuietly(this.table);
        }
    }
}
