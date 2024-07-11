package ai.rapids.cudf.serde;

import ai.rapids.cudf.*;
import ai.rapids.cudf.schema.SchemaVisitor;
import ai.rapids.cudf.schema.SchemaWithColumnsVisitor;
import ai.rapids.cudf.schema.Visitors;
import ai.rapids.cudf.utils.Arms;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KudoSerializer implements TableSerializer {

    /**
     * Magic number "KUDO" in ASCII.
     */
    private static final int SER_FORMAT_MAGIC_NUMBER = 0x4B55444F;
    private static final short VERSION_NUMBER = 0x0001;

    private static final byte[] PADDING = new byte[64];

    static {
        Arrays.fill(PADDING, (byte) 0);
    }

    @Override
    public String version() {
        return "v1";
    }

    @Override
    public long writeToStream(HostColumnVector[] columns, OutputStream out, long rowOffset, long numRows) {
        if (numRows < 0) {
            throw new IllegalArgumentException("numRows must be >= 0");
        }

        if (numRows == 0 || columns.length == 0) {
            return 0;
        }

        try {
            return writeSliced(columns, writerFrom(out), rowOffset, numRows);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long writeRowsToStream(OutputStream out, long numRows) {
        if (numRows <= 0) {
            throw new IllegalArgumentException("Number of rows must be > 0, but was " + numRows);
        }
        try {
            DataWriter writer = writerFrom(out);
            SerializedTableHeader header = new SerializedTableHeader(0, (int) numRows, 0, 0, 0, new byte[0]);
            header.writeTo(writer);
            writer.flush();
            return header.getSerializedSize();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object readOneTableBuffer(InputStream in) {
        Objects.requireNonNull(in, "Input stream must not be null");

        try {
            DataInputStream din = readerFrom(in);
            SerializedTableHeader header = new SerializedTableHeader(din);
            if (!header.wasInitialized()) {
                return null;
            }

            if (header.numRows <= 0) {
                throw new IllegalArgumentException("Number of rows must be > 0, but was " + header.numRows);
            }

            // Header only
            if (header.hasValidityBuffer.length == 0) {
                return new SerializedTable(header, null);
            }

            HostMemoryBuffer buffer = HostMemoryBuffer.allocate(header.getTotalDataLen());
            buffer.copyFromStream(0, din, header.getTotalDataLen());
            return new SerializedTable(header, buffer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Table mergeTable(List<Object> buffers, Schema schema) {
        List<List<ColumnVector>> batches = buffers
                .stream()
                .map(o -> (SerializedTable) o)
                .map(serializedTable -> serializedTable.deserialize(schema)).collect(Collectors.toList());

        ColumnVector[] columnVectors = IntStream.range(0, schema.getNumChildren()).mapToObj(colIdx -> {
            ColumnVector[] cols = batches.stream().map(batch -> batch.get(colIdx)).toArray(ColumnVector[]::new);
            if (cols.length > 1) {
                ColumnVector ret = ColumnVector.concatenate(cols);
                Arms.closeQuietly(cols);
                return ret;
            } else {
                return cols[0];
            }
        }).toArray(ColumnVector[]::new);


        try (CloseableArray<ColumnVector> ignored = CloseableArray.wrap(columnVectors)) {
            return new Table(columnVectors);
        }
    }

    private static long writeSliced(HostColumnVector[] columns, DataWriter out, long rowOffset, long numRows) throws Exception {
        List<HostColumnVector> columnList = Arrays.stream(columns).collect(Collectors.toList());

        Schema.Builder schemaBuilder = Schema.builder();
        for (int i = 0; i < columns.length; i++) {
            columns[i].toSchema("col_" + i + "_", schemaBuilder);
        }
        Schema schema = schemaBuilder.build();


        SerializedTableHeaderCalc headerCalc = new SerializedTableHeaderCalc(rowOffset, numRows);
        SerializedTableHeader header = Visitors.visitSchemaWithColumns(schema, columnList, headerCalc);
        header.writeTo(out);

        long bytesWritten = 0;
        for (BufferType bufferType : Arrays.asList(BufferType.VALIDITY, BufferType.OFFSET, BufferType.DATA)) {
            bytesWritten += Visitors.visitSchemaWithColumns(schema, columnList, new SlicedBufferSerializer(rowOffset, numRows, bufferType, out));
        }

        if (bytesWritten != header.totalDataLen) {
            throw new IllegalStateException("Header total data length: " + header.totalDataLen + " does not match actual written data length: " + bytesWritten);
        }

        out.flush();

        return header.getSerializedSize() + bytesWritten;
    }

    private static DataInputStream readerFrom(InputStream in) {
        if (!(in instanceof DataInputStream)) {
            in = new DataInputStream(in);
        }
        return new DataInputStream(in);
    }

    private static DataWriter writerFrom(OutputStream out) {
        if (!(out instanceof DataOutputStream)) {
            out = new DataOutputStream(new BufferedOutputStream(out));
        }
        return new DataOutputStreamWriter((DataOutputStream) out);
    }


    /**
     * Visible for testing
     */
    static abstract class DataWriter {

        public abstract void writeByte(byte b) throws IOException;

        public abstract void writeShort(short s) throws IOException;

        public abstract void writeInt(int i) throws IOException;

        public abstract void writeIntNativeOrder(int i) throws IOException;

        public abstract void writeLong(long val) throws IOException;

        /**
         * Copy data from src starting at srcOffset and going for len bytes.
         *
         * @param src       where to copy from.
         * @param srcOffset offset to start at.
         * @param len       amount to copy.
         */
        public abstract void copyDataFrom(HostMemoryBuffer src, long srcOffset, long len) throws IOException;

        public void flush() throws IOException {
            // NOOP by default
        }

        public abstract void write(byte[] arr, int offset, int length) throws IOException;
    }

    /**
     * Visible for testing
     */
    static final class DataOutputStreamWriter extends DataWriter {
        private final DataOutputStream dout;
        private final WritableByteChannel channel;

        public DataOutputStreamWriter(DataOutputStream dout) {
            this.dout = dout;
            this.channel = Channels.newChannel(dout);
        }

        @Override
        public void writeByte(byte b) throws IOException {
            dout.writeByte(b);
        }

        @Override
        public void writeShort(short s) throws IOException {
            dout.writeShort(s);
        }

        @Override
        public void writeInt(int i) throws IOException {
            dout.writeInt(i);
        }

        @Override
        public void writeIntNativeOrder(int i) throws IOException {
            // TODO this only works on Little Endian Architectures, x86.  If we need
            // to support others we need to detect the endianness and switch on the right implementation.
            writeInt(Integer.reverseBytes(i));
        }

        @Override
        public void writeLong(long val) throws IOException {
            dout.writeLong(val);
        }

        @Override
        public void copyDataFrom(HostMemoryBuffer src, long srcOffset, long len) throws IOException {
            ByteBuffer buffer = src.asByteBuffer(srcOffset, (int) len);
            channel.write(buffer);
        }

        @Override
        public void flush() throws IOException {
            dout.flush();
        }

        @Override
        public void write(byte[] arr, int offset, int length) throws IOException {
            dout.write(arr, offset, length);
        }
    }

/////////////////////////////////////////////
// METHODS
/////////////////////////////////////////////


    /////////////////////////////////////////////
// PADDING FOR ALIGNMENT
/////////////////////////////////////////////
    private static long padFor64byteAlignment(long orig) {
        return ((orig + 63) / 64) * 64;
    }

    private static long padFor64byteAlignment(DataWriter out, long bytes) throws IOException {
        final long paddedBytes = padFor64byteAlignment(bytes);
        if (paddedBytes > bytes) {
            out.write(PADDING, 0, (int) (paddedBytes - bytes));
        }
        return paddedBytes;
    }

    /**
     * Holds the metadata about a serialized table. If this is being read from a stream
     * isInitialized will return true if the metadata was read correctly from the stream.
     * It will return false if an EOF was encountered at the beginning indicating that
     * there was no data to be read.
     */
    public static final class SerializedTableHeader {
        // Useful for reducing calculations in writing.
        private int offset;
        private int numRows;
        private long validityBufferLen;
        private long offsetBufferLen;
        private long totalDataLen;
        // This is used to indicate the validity buffer for the columns.
        // 1 means that this column has validity data, 0 means it does not.
        private byte[] hasValidityBuffer;

        private boolean initialized = false;


        public SerializedTableHeader(DataInputStream din) throws IOException {
            readFrom(din);
        }

        SerializedTableHeader(int offset, int numRows, long validityBufferLen, long offsetBufferLen, long totalDataLen, byte[] hasValidityBuffer) {
            this.offset = offset;
            this.numRows = numRows;
            this.validityBufferLen = validityBufferLen;
            this.offsetBufferLen = offsetBufferLen;
            this.totalDataLen = totalDataLen;
            this.hasValidityBuffer = hasValidityBuffer;

            this.initialized = true;
        }

        /**
         * Returns the size of a buffer needed to read data into the stream.
         */
        public long getTotalDataLen() {
            return totalDataLen;
        }

        /**
         * Returns the number of rows stored in this table.
         */
        public int getNumRows() {
            return numRows;
        }

        public int getOffset() {
            return offset;
        }

        /**
         * Returns true if the metadata for this table was read, else false indicating an EOF was
         * encountered.
         */
        public boolean wasInitialized() {
            return initialized;
        }

        public boolean hasValidityBuffer(int columnIndex) {
            return hasValidityBuffer[columnIndex] != 0;
        }

        public long getSerializedSize() {
            return 4 + 2 + 4 + 4 + 8 + 8 + 8 + 4 + hasValidityBuffer.length;
        }

        public int getNumColumns() {
            return Optional.ofNullable(hasValidityBuffer).map(arr -> arr.length).orElse(0);
        }

        private void readFrom(DataInputStream din) throws IOException {
            try {
                int num = din.readInt();
                if (num != SER_FORMAT_MAGIC_NUMBER) {
                    throw new IllegalStateException("THIS DOES NOT LOOK LIKE CUDF SERIALIZED DATA. " + "Expected magic number " + SER_FORMAT_MAGIC_NUMBER + " Found " + num);
                }
            } catch (EOFException e) {
                // If we get an EOF at the very beginning don't treat it as an error because we may
                // have finished reading everything...
                return;
            }
            short version = din.readShort();
            if (version != VERSION_NUMBER) {
                throw new IllegalStateException("READING THE WRONG SERIALIZATION FORMAT VERSION FOUND " + version + " EXPECTED " + VERSION_NUMBER);
            }

            offset = din.readInt();
            numRows = din.readInt();

            validityBufferLen = din.readLong();
            offsetBufferLen = din.readLong();
            totalDataLen = din.readLong();
            int validityBufferLength = din.readInt();
            hasValidityBuffer = new byte[validityBufferLength];
            din.readFully(hasValidityBuffer);

            initialized = true;
        }

        public void writeTo(DataWriter dout) throws IOException {
            // Now write out the data
            dout.writeInt(SER_FORMAT_MAGIC_NUMBER);
            dout.writeShort(VERSION_NUMBER);

            dout.writeInt(offset);
            dout.writeInt(numRows);
            dout.writeLong(validityBufferLen);
            dout.writeLong(offsetBufferLen);
            dout.writeLong(totalDataLen);
            dout.writeInt(hasValidityBuffer.length);
            dout.write(hasValidityBuffer, 0, hasValidityBuffer.length);
        }

        @Override
        public String toString() {
            return "SerializedTableHeader{" +
                    "offset=" + offset +
                    ", numRows=" + numRows +
                    ", validityBufferLen=" + validityBufferLen +
                    ", offsetBufferLen=" + offsetBufferLen +
                    ", totalDataLen=" + totalDataLen +
                    ", hasValidityBuffer=" + Arrays.toString(hasValidityBuffer) +
                    ", initialized=" + initialized +
                    '}';
        }
    }

    public static class SerializedTable implements AutoCloseable {
        private final SerializedTableHeader header;
        private final HostMemoryBuffer buffer;

        SerializedTable(SerializedTableHeader header, HostMemoryBuffer buffer) {
            this.header = header;
            this.buffer = buffer;
        }

        public SerializedTableHeader getHeader() {
            return header;
        }

        List<ColumnVector> deserialize(Schema schema) {
            try (ColumnVectorDeserializer deserializer = new ColumnVectorDeserializer(this)) {
                return Visitors.visitSchema(schema, deserializer);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String toString() {
            return "SerializedTable{" +
                    "header=" + header +
                    ", buffer=" + buffer +
                    '}';
        }

        @Override
        public void close() throws Exception {
            if (buffer != null) {
                buffer.close();
            }
        }
    }

    static class SlicedValidityBufferInfo {
        private final long bufferOffset;
        private final long bufferLength;
        /// The bit offset within the buffer where the slice starts
        private final long beginBit;
        private final long endBit; // Exclusive

        SlicedValidityBufferInfo(long bufferOffset, long bufferLength, long beginBit, long endBit) {
            this.bufferOffset = bufferOffset;
            this.bufferLength = bufferLength;
            this.beginBit = beginBit;
            this.endBit = endBit;
        }

        @Override
        public String toString() {
            return "SlicedValidityBufferInfo{" + "bufferOffset=" + bufferOffset + ", bufferLength=" + bufferLength + ", beginBit=" + beginBit + ", endBit=" + endBit + '}';
        }

        static SlicedValidityBufferInfo calc(long rowOffset, long numRows) {
            if (rowOffset < 0) {
                throw new IllegalArgumentException("rowOffset must be >= 0, but was " + rowOffset);
            }
            if (numRows < 0) {
                throw new IllegalArgumentException("numRows must be >= 0, but was " + numRows);
            }
            long bufferOffset = rowOffset / 8;
            long beginBit = rowOffset % 8;
            long bufferLength = 0;
            if (numRows > 0) {
                bufferLength = (rowOffset + numRows) / 8 - bufferOffset + 1;
            }
            long endBit = beginBit + numRows;
            return new SlicedValidityBufferInfo(bufferOffset, bufferLength, beginBit, endBit);
        }
    }

    static class SlicedBufferSerializer implements SchemaWithColumnsVisitor<Long, Long> {
        private final SliceInfo root;
        private final BufferType bufferType;
        private final DataWriter writer;

        private final Deque<SliceInfo> sliceInfos = new ArrayDeque<>();

        SlicedBufferSerializer(long rowOffset, long numRows, BufferType bufferType, DataWriter writer) {
            this.root = new SliceInfo(rowOffset, numRows);
            this.bufferType = bufferType;
            this.writer = writer;
            this.sliceInfos.addLast(root);
        }

        @Override
        public Long visitTopSchema(Schema schema, List<Long> children) {
            return children.stream().mapToLong(Long::longValue).sum();
        }

        @Override
        public Long visitStruct(Schema structType, HostColumnVectorCore col, List<Long> children) {
            SliceInfo parent = sliceInfos.peekLast();

            long bytesCopied = children.stream().mapToLong(Long::longValue).sum();
            try {
                switch (bufferType) {
                    case VALIDITY:
                        bytesCopied += this.copySlicedValidity(col, parent);
                        return bytesCopied;
                    case OFFSET:
                    case DATA:
                        return bytesCopied;
                    default:
                        throw new IllegalArgumentException("Unexpected buffer type: " + bufferType);
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Long preVisitList(Schema listType, HostColumnVectorCore col) {
            SliceInfo parent = sliceInfos.peekLast();


            long bytesCopied = 0;
            try {
                switch (bufferType) {
                    case VALIDITY:
                        bytesCopied = this.copySlicedValidity(col, parent);
                        break;
                    case OFFSET:
                        bytesCopied = this.copySlicedOffset(col, parent);
                        break;
                    case DATA:
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected buffer type: " + bufferType);
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            long start = col.getOffsets().getInt(parent.offset * Integer.BYTES);
            long end = col.getOffsets().getInt((parent.offset + parent.rowCount) * Integer.BYTES);
            long rowCount = end - start;

            SliceInfo current = new SliceInfo(start, rowCount);
            sliceInfos.addLast(current);
            return bytesCopied;
        }

        @Override
        public Long visitList(Schema listType, HostColumnVectorCore col, Long preVisitResult, Long childResult) {
            sliceInfos.removeLast();
            return preVisitResult + childResult;
        }

        @Override
        public Long visit(Schema primitiveType, HostColumnVectorCore col) {
            SliceInfo parent = sliceInfos.peekLast();
            try {
                switch (bufferType) {
                    case VALIDITY:
                        return this.copySlicedValidity(col, parent);
                    case OFFSET:
                        return this.copySlicedOffset(col, parent);
                    case DATA:
                        return this.copySlicedData(col, parent);
                    default:
                        throw new IllegalArgumentException("Unexpected buffer type: " + bufferType);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private long copySlicedValidity(HostColumnVectorCore column, SliceInfo sliceInfo) throws IOException {
            if (column.getValidity() != null) {
                HostMemoryBuffer buff = column.getValidity();
                writer.copyDataFrom(buff, sliceInfo.getValidityBufferInfo().bufferOffset,
                        sliceInfo.getValidityBufferInfo().bufferLength);
                return padFor64byteAlignment(writer, sliceInfo.getValidityBufferInfo().bufferLength);
            } else {
                return 0;
            }
        }

        private long copySlicedOffset(HostColumnVectorCore column, SliceInfo sliceInfo) throws IOException {
            if (sliceInfo.rowCount <= 0 || column.getOffsets() == null) {
                // Don't copy anything, there are no rows
                return 0;
            }
            long bytesToCopy = (sliceInfo.rowCount + 1) * Integer.BYTES;
            long srcOffset = sliceInfo.offset * Integer.BYTES;
            HostMemoryBuffer buff = column.getOffsets();
            writer.copyDataFrom(buff, srcOffset, bytesToCopy);
            return bytesToCopy;
        }

        private long copySlicedData(HostColumnVectorCore column, SliceInfo sliceInfo) throws IOException {
            if (sliceInfo.rowCount > 0) {
                DType type = column.getType();
                if (type.equals(DType.STRING)) {
                    long startByteOffset = column.getOffsets().getInt(sliceInfo.offset * Integer.BYTES);
                    long endByteOffset = column.getOffsets().getInt((sliceInfo.offset + sliceInfo.rowCount) * Integer.BYTES);
                    long bytesToCopy = endByteOffset - startByteOffset;
                    if (column.getData() == null) {
                        if (bytesToCopy != 0) {
                            throw new IllegalStateException("String column has no data buffer, " +
                                    "but bytes to copy is not zero: " + bytesToCopy);
                        }
                    } else {
                        writer.copyDataFrom(column.getData(), startByteOffset, bytesToCopy);
                    }
                    return bytesToCopy;
                } else if (type.getSizeInBytes() > 0) {
                    long bytesToCopy = sliceInfo.rowCount * type.getSizeInBytes();
                    long srcOffset = sliceInfo.offset * type.getSizeInBytes();
                    writer.copyDataFrom(column.getData(), srcOffset, bytesToCopy);
                    return bytesToCopy;
                } else {
                    return 0;
                }
            } else {
                return 0;
            }
        }
    }

    static class ColumnVectorDeserializer implements SchemaVisitor<ColumnView.NestedColumnVector, List<ColumnVector>>, AutoCloseable {
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
            this.root = new SliceInfo(table.header.offset, table.header.numRows);
            this.sliceInfos.addLast(root);


            MemoryBuffer[] buffersArray = Arms.closeIfException(CloseableArray.wrap(new MemoryBuffer[3]), buffers -> {
                buffers.set(0, DeviceMemoryBuffer.allocate(table.header.validityBufferLen));
                ((DeviceMemoryBuffer) buffers.get(0))
                        .copyFromHostBuffer(table.buffer, 0, table.header.validityBufferLen);
                buffers.set(1, DeviceMemoryBuffer.allocate(table.header.offsetBufferLen));
                ((DeviceMemoryBuffer) buffers.get(1))
                        .copyFromHostBuffer(table.buffer, table.header.validityBufferLen, table.header.offsetBufferLen);
                buffers.set(2, table.buffer.slice(table.header.validityBufferLen + table.header.offsetBufferLen,
                        table.buffer.getLength() - table.header.validityBufferLen - table.header.offsetBufferLen));
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
            if (table.header.hasValidityBuffer(currentIdx)) {
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
            if (table.header.hasValidityBuffer(currentIdx)) {
                validityBuffer = this.deserializeValidityBuffer(parent);
            }

            DeviceMemoryBuffer offsetBuffer;
            // Cuda always uses little endian
            int firstOffsetValue = table.buffer
                    .asByteBuffer(table.header.validityBufferLen + currentOffsetOffset, Integer.BYTES)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getInt();
            int lastOffsetValue = table.buffer
                    .asByteBuffer(table.header.validityBufferLen + currentOffsetOffset + (long) Integer.BYTES * parent.rowCount, Integer.BYTES)
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
            if (table.header.hasValidityBuffer(currentIdx)) {
                validityBuffer = this.deserializeValidityBuffer(parent);
            }

            DeviceMemoryBuffer offsetBuffer = null;
            int dataLen;
            if (primitiveType.getType().hasOffsets()) {
                if (parent.rowCount > 0) {
                    // Cuda always uses little endian
                    int firstOffsetValue = table.buffer
                            .asByteBuffer(table.header.validityBufferLen + currentOffsetOffset, Integer.BYTES)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .getInt();
                    int lastOffsetValue = table.buffer
                            .asByteBuffer(table.header.validityBufferLen + currentOffsetOffset + (long) Integer.BYTES * parent.rowCount, Integer.BYTES)
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
            currentValidityOffset += padFor64byteAlignment(sliceInfo.getValidityBufferInfo().bufferLength);
            return ColumnVector.copyBitmask(startAddress,
                    sliceInfo.getValidityBufferInfo().beginBit,
                    sliceInfo.getValidityBufferInfo().endBit);
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

    static class SerializedTableHeaderCalc implements SchemaWithColumnsVisitor<Void, SerializedTableHeader> {
        private final SliceInfo root;
        private final List<Boolean> hasValidityBuffer = new ArrayList<>(1024);
        private long validityBufferLen;
        private long offsetBufferLen;
        private long totalDataLen;

        private Deque<SliceInfo> sliceInfos = new ArrayDeque<>();

        SerializedTableHeaderCalc(long rowOffset, long numRows) {
            this.root = new SliceInfo(rowOffset, numRows);
            this.totalDataLen = 0;
            sliceInfos.addLast(this.root);
        }

        @Override
        public SerializedTableHeader visitTopSchema(Schema schema, List<Void> children) {
            byte[] hasValidityBuffer = new byte[this.hasValidityBuffer.size()];
            for (int i = 0; i < this.hasValidityBuffer.size(); i++) {
                hasValidityBuffer[i] = (byte) (this.hasValidityBuffer.get(i) ? 1 : 0);
            }
            return new SerializedTableHeader((int) root.offset, (int) root.rowCount,
                    validityBufferLen, offsetBufferLen,
                    totalDataLen, hasValidityBuffer);
        }

        @Override
        public Void visitStruct(Schema structType, HostColumnVectorCore col, List<Void> children) {
            SliceInfo parent = sliceInfos.peekLast();

            long validityBufferLength = 0;
            if (col.hasValidityVector()) {
                validityBufferLength = padFor64byteAlignment(parent.getValidityBufferInfo().bufferLength);
            }

            this.validityBufferLen += validityBufferLength;

            totalDataLen += validityBufferLength;
            hasValidityBuffer.add(col.getValidity() != null);
            return null;
        }

        @Override
        public Void preVisitList(Schema listType, HostColumnVectorCore col) {
            SliceInfo parent = sliceInfos.peekLast();


            long validityBufferLength = 0;
            if (col.hasValidityVector()) {
                validityBufferLength = padFor64byteAlignment(parent.getValidityBufferInfo().bufferLength);
            }

            long offsetBufferLength = (parent.rowCount + 1) * Integer.BYTES;

            this.validityBufferLen += validityBufferLength;
            this.offsetBufferLen += offsetBufferLength;
            this.totalDataLen += validityBufferLength + offsetBufferLength;

            hasValidityBuffer.add(col.getValidity() != null);

            long start = col.getOffsets().getInt(parent.offset * Integer.BYTES);
            long end = col.getOffsets().getInt((parent.offset + parent.rowCount) * Integer.BYTES);
            long rowCount = end - start;

            SliceInfo current = new SliceInfo(start, rowCount);
            sliceInfos.addLast(current);
            return null;
        }

        @Override
        public Void visitList(Schema listType, HostColumnVectorCore col, Void preVisitResult, Void childResult) {
            sliceInfos.removeLast();

            return null;
        }


        @Override
        public Void visit(Schema primitiveType, HostColumnVectorCore col) {
            SliceInfo parent = sliceInfos.peekLast();
            long validityBufferLen = calcPrimitiveDataLen(primitiveType, col, BufferType.VALIDITY, parent);
            long offsetBufferLen = calcPrimitiveDataLen(primitiveType, col, BufferType.OFFSET, parent);
            long dataBufferLen = calcPrimitiveDataLen(primitiveType, col, BufferType.DATA, parent);

            this.validityBufferLen += validityBufferLen;
            this.offsetBufferLen += offsetBufferLen;
            this.totalDataLen += validityBufferLen + offsetBufferLen + dataBufferLen;

            hasValidityBuffer.add(col.getValidity() != null);

            return null;
        }

        private long calcPrimitiveDataLen(Schema primitiveType,
                                          HostColumnVectorCore col,
                                          BufferType bufferType,
                                          SliceInfo info) {
            switch (bufferType) {
                case VALIDITY:
                    if (col.hasValidityVector()) {
                        return padFor64byteAlignment(info.getValidityBufferInfo().bufferLength);
                    } else {
                        return 0;
                    }
                case OFFSET:
                    if (DType.STRING.equals(primitiveType.getType()) && col.getOffsets() != null && info.rowCount > 0) {
                        return (info.rowCount + 1) * Integer.BYTES;
                    } else {
                        return 0;
                    }
                case DATA:
                    if (DType.STRING.equals(primitiveType.getType())) {
                        long startByteOffset = col.getOffsets().getInt(info.offset * Integer.BYTES);
                        long endByteOffset = col.getOffsets().getInt((info.offset + info.rowCount) * Integer.BYTES);
                        return endByteOffset - startByteOffset;
                    } else {
                        if (primitiveType.getType().getSizeInBytes() > 0) {
                            return primitiveType.getType().getSizeInBytes() * info.rowCount;
                        } else {
                            return 0;
                        }
                    }
                default:
                    throw new IllegalArgumentException("Unexpected buffer type: " + bufferType);

            }
        }
    }

    static class SliceInfo {
        final long offset;
        final long rowCount;
        private final SlicedValidityBufferInfo validityBufferInfo;

        SliceInfo(long offset, long rowCount) {
            this.offset = offset;
            this.rowCount = rowCount;
            this.validityBufferInfo = SlicedValidityBufferInfo.calc(offset, rowCount);
        }

        public SlicedValidityBufferInfo getValidityBufferInfo() {
            return validityBufferInfo;
        }

        @Override
        public String toString() {
            return "SliceInfo{" +
                    "offset=" + offset +
                    ", rowCount=" + rowCount +
                    ", validityBufferInfo=" + validityBufferInfo +
                    '}';
        }
    }
}