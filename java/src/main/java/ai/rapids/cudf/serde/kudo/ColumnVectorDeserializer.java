package ai.rapids.cudf.serde.kudo;

import ai.rapids.cudf.*;
import ai.rapids.cudf.schema.SchemaVisitor;
import ai.rapids.cudf.utils.Arms;

import java.nio.ByteOrder;
import java.util.*;
import java.util.stream.Collectors;

class ColumnVectorDeserializer implements SchemaVisitor<ColumnView.NestedColumnVector, List<ColumnVector>>, AutoCloseable {
    private final SerializedTable table;
    private final DeviceMemoryBuffer validityBuffer;
    private final DeviceMemoryBuffer offsetBuffer;
    private final HostMemoryBuffer dataBuffer;
    private final SliceInfo root;
    private final Deque<SliceInfo> sliceInfos = new ArrayDeque<>();
    private long currentValidityOffset;
    private long currentOffsetOffset;
    private long currentDataOffset;
    private int currentIdx;

    public ColumnVectorDeserializer(SerializedTable table) {
        this.table = table;
        this.root = new SliceInfo(table.getHeader().getOffset(), table.getHeader().getNumRows());
        this.sliceInfos.addLast(root);


        MemoryBuffer[] buffersArray = Arms.closeIfException(CloseableArray.wrap(new MemoryBuffer[3]), buffers -> {
            buffers.set(0, DeviceMemoryBuffer.allocate(table.getHeader().getValidityBufferLen()));
            ((DeviceMemoryBuffer) buffers.get(0))
                    .copyFromHostBuffer(table.getBuffer(), 0, table.getHeader().getValidityBufferLen());
            buffers.set(1, DeviceMemoryBuffer.allocate(table.getHeader().getOffsetBufferLen()));
            ((DeviceMemoryBuffer) buffers.get(1))
                    .copyFromHostBuffer(table.getBuffer(), table.getHeader().getValidityBufferLen(), table.getHeader().getOffsetBufferLen());
            buffers.set(2, table.getBuffer().slice(table.getHeader().getValidityBufferLen() + table.getHeader().getOffsetBufferLen(),
                    table.getBuffer().getLength() - table.getHeader().getValidityBufferLen() - table.getHeader().getOffsetBufferLen()));
            return buffers.release();
        });

        this.validityBuffer = (DeviceMemoryBuffer) buffersArray[0];
        this.offsetBuffer = (DeviceMemoryBuffer) buffersArray[1];
        this.dataBuffer = (HostMemoryBuffer) buffersArray[2];
        this.currentIdx = 0;
    }

    @Override
    public List<ColumnVector> visitTopSchema(Schema schema, List<ColumnView.NestedColumnVector> children) {
        return children.stream().map(ColumnView.NestedColumnVector::toColumnVector).collect(Collectors.toList());
    }

    @Override
    public ColumnView.NestedColumnVector visitStruct(Schema structType, List<ColumnView.NestedColumnVector> children) {
        SliceInfo parent = sliceInfos.peekLast();
        DeviceMemoryBuffer validityBuffer = null;
        if (table.getHeader().hasValidityBuffer(currentIdx)) {
            validityBuffer = this.deserializeValidityBuffer(parent);
        }

        currentIdx += 1;
        return new ColumnView.NestedColumnVector(structType.getType(), parent.rowCount, Optional.empty(),
                null, validityBuffer, null, children);
    }

    @Override
    public ColumnView.NestedColumnVector preVisitList(Schema listType) {
        SliceInfo parent = sliceInfos.peekLast();

        DeviceMemoryBuffer validityBuffer = null;
        if (table.getHeader().hasValidityBuffer(currentIdx)) {
            validityBuffer = this.deserializeValidityBuffer(parent);
        }

        DeviceMemoryBuffer offsetBuffer;
        // Cuda always uses little endian
        int firstOffsetValue = table.getBuffer()
                .asByteBuffer(table.getHeader().getValidityBufferLen() + currentOffsetOffset, Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
        int lastOffsetValue = table.getBuffer()
                .asByteBuffer(table.getHeader().getValidityBufferLen() + currentOffsetOffset + (long) Integer.BYTES * parent.rowCount, Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
        offsetBuffer = this.deserializeOffsetBuffer(firstOffsetValue, parent);

        sliceInfos.addLast(new SliceInfo(firstOffsetValue, lastOffsetValue - firstOffsetValue));
        currentIdx += 1;


        return new ColumnView.NestedColumnVector(listType.getType(), parent.rowCount,
                Optional.empty(), null, validityBuffer, offsetBuffer, Collections.emptyList());
    }

    @Override
    public ColumnView.NestedColumnVector visitList(Schema listType, ColumnView.NestedColumnVector preVisitResult, ColumnView.NestedColumnVector childResult) {
        sliceInfos.removeLast();

        return preVisitResult.withNewChildren(Collections.singletonList(childResult));
    }

    @Override
    public ColumnView.NestedColumnVector visit(Schema primitiveType) {
        SliceInfo parent = sliceInfos.peekLast();
        DeviceMemoryBuffer validityBuffer = null;
        if (table.getHeader().hasValidityBuffer(currentIdx)) {
            validityBuffer = this.deserializeValidityBuffer(parent);
        }

        DeviceMemoryBuffer offsetBuffer = null;
        int dataLen;
        if (primitiveType.getType().hasOffsets()) {
            if (parent.rowCount > 0) {
                // Cuda always uses little endian
                int firstOffsetValue = table.getBuffer()
                        .asByteBuffer(table.getHeader().getValidityBufferLen() + currentOffsetOffset, Integer.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .getInt();
                int lastOffsetValue = table.getBuffer()
                        .asByteBuffer(table.getHeader().getValidityBufferLen() + currentOffsetOffset + (long) Integer.BYTES * parent.rowCount, Integer.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .getInt();

                dataLen = lastOffsetValue - firstOffsetValue;
                offsetBuffer = this.deserializeOffsetBuffer(firstOffsetValue, parent);
            } else {
                dataLen = 0;
                offsetBuffer = DeviceMemoryBuffer.allocate(0);
            }
        } else {
            dataLen = (int) (primitiveType.getType().getSizeInBytes() * parent.rowCount);
        }

        DeviceMemoryBuffer dataBuffer;
        if (dataLen > 0) {
            dataBuffer = this.deserializeDataBuffer(dataLen);
        } else {
            dataBuffer = DeviceMemoryBuffer.allocate(0);
        }

        currentIdx += 1;
        return new ColumnView.NestedColumnVector(primitiveType.getType(), parent.rowCount, Optional.empty(), dataBuffer, validityBuffer, offsetBuffer, Collections.emptyList());
    }

    private DeviceMemoryBuffer deserializeValidityBuffer(SliceInfo sliceInfo) {
        long startAddress = validityBuffer.getAddress() + currentValidityOffset;
        currentValidityOffset += KudoSerializer.padFor64byteAlignment(sliceInfo.getValidityBufferInfo().getBufferLength());
        return ColumnVector.copyBitmask(startAddress,
                sliceInfo.getValidityBufferInfo().getBeginBit(),
                sliceInfo.getValidityBufferInfo().getEndBit());
    }

    private DeviceMemoryBuffer deserializeOffsetBuffer(int firstOffsetValue, SliceInfo sliceInfo) {
        long bufferLen = (sliceInfo.rowCount + 1) * Integer.BYTES;
        ColumnView offsetView = ColumnView.fromDeviceBuffer(offsetBuffer, this.currentOffsetOffset, DType.INT32,
                (int) (sliceInfo.rowCount + 1));
        try (Scalar first = Scalar.fromInt(firstOffsetValue)) {
            try (ColumnVector col = offsetView.sub(first, DType.INT32)) {
                this.currentOffsetOffset += bufferLen;
                return col.getData().sliceWithCopy(0, bufferLen);
            }
        }
    }

    private DeviceMemoryBuffer deserializeDataBuffer(long dataLen) {
        return Arms.closeIfException(DeviceMemoryBuffer.allocate(dataLen), deviceDataBuffer -> {
            deviceDataBuffer.copyFromHostBuffer(dataBuffer, currentDataOffset, dataLen);
            this.currentDataOffset += dataLen;
            return deviceDataBuffer;
        });
    }

    @Override
    public String toString() {
        return "ColumnVectorDeserializer{" +
                "table=" + table +
                ", validityBuffer=" + validityBuffer +
                ", offsetBuffer=" + offsetBuffer +
                ", dataBuffer=" + dataBuffer +
                ", root=" + root +
                ", sliceInfos=" + sliceInfos +
                ", currentValidityOffset=" + currentValidityOffset +
                ", currentOffsetOffset=" + currentOffsetOffset +
                ", currentDataOffset=" + currentDataOffset +
                ", currentIdx=" + currentIdx +
                '}';
    }

    @Override
    public void close() throws Exception {
        if (this.table != null) {
            Arms.closeQuietly(this.table);
        }

        if (this.validityBuffer != null) {
            Arms.closeQuietly(this.validityBuffer);
        }

        if (this.offsetBuffer != null) {
            Arms.closeQuietly(this.offsetBuffer);
        }

        if (this.dataBuffer != null) {
            Arms.closeQuietly(this.dataBuffer);
        }
    }
}
