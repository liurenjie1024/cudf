package ai.rapids.cudf.serde;

import ai.rapids.cudf.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.IntStream;

public class KuduSerializer implements TableSerializer {

    /**
     * Magic number "CUDF" in ASCII, which is 1178883395 if read in LE from big endian, which is
     * too large for any reasonable metadata for arrow, so we should probably be okay detecting
     * this, and switching back/forth at a later time.
     */
    private static final int SER_FORMAT_MAGIC_NUMBER = 0x43554446;
    private static final short VERSION_NUMBER = 0x0000;

    @Override
    public void writeToStream(HostColumnVector[] columns, OutputStream out, long rowOffset, long numRows) {
        if (Arrays.stream(columns).map(HostColumnVectorCore::getType).anyMatch(DType::isNestedType)) {
            throw new IllegalArgumentException("Nested types are not supported yet!");
        }

        ColumnBufferProvider[] providers = providersFrom(columns, false);
        try {
            DataWriter writer = writerFrom(out);
            writeSliced(providers, writer, rowOffset, numRows);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            closeAll(providers);
        }

    }

    private static void writeSliced(ColumnBufferProvider[] columns,
                                    DataWriter out,
                                    long rowOffset,
                                    long numRows) throws IOException {
        assert rowOffset >= 0;
        assert numRows >= 0;
        for (ColumnBufferProvider column : columns) {
            long rows = column.getRowCount();
            assert rowOffset + numRows <= rows;
            assert rows == (int) rows : "can only support an int for indexes";
        }

        SerializedTableHeader header = calcHeader(columns, rowOffset, (int) numRows);
        header.writeTo(out);

        try (NvtxRange range = new NvtxRange("Write Sliced", NvtxColor.GREEN)) {
            for (ColumnBufferProvider column : columns) {
                writeSlicedValidityBuffer(out, column, rowOffset, numRows);
            }
            for (ColumnBufferProvider column : columns) {
                writeSlicedOffset(out, column, rowOffset, numRows);
            }
            for (ColumnBufferProvider column : columns) {
                writeSlicedBasicData(out, column, rowOffset, numRows);
            }
        }
        out.flush();
    }

    private static void writeSlicedValidityBuffer(DataWriter out,
                                                  ColumnBufferProvider column,
                                                  long rowOffset,
                                                  long numRows) throws IOException {
        if (needsValidityBuffer(column.getNullCount())) {
            try (NvtxRange range = new NvtxRange("Write Validity", NvtxColor.DARK_GREEN)) {
                copySlicedValidity(out, column, rowOffset, numRows);
            }
        }
    }

    private static void writeSlicedOffset(DataWriter out,
                                          ColumnBufferProvider column,
                                          long rowOffset,
                                          long numRows) throws IOException {
        DType type = column.getType();
        if (type.hasOffsets()) {
            if (numRows > 0) {
                try (NvtxRange offsetRange = new NvtxRange("Write Offset Data", NvtxColor.ORANGE)) {
                    copySlicedOffsets(out, column, rowOffset, numRows);
                    if (type.equals(DType.STRING)) {
                        try (NvtxRange dataRange = new NvtxRange("Write String Data", NvtxColor.RED)) {
                            copySlicedStringData(out, column, rowOffset, numRows);
                        }
                    }
                }
            }
        }
    }

    private static void writeSlicedBasicData(DataWriter out,
                                             ColumnBufferProvider column,
                                             long rowOffset,
                                             long numRows) throws IOException {
        DType type = column.getType();
        if (type.getSizeInBytes() > 0) {
            try (NvtxRange range = new NvtxRange("Write Data", NvtxColor.BLUE)) {
                sliceBasicData(out, column, rowOffset, numRows);
            }
        }
    }

    private static long sliceBasicData(DataWriter out,
                                       ColumnBufferProvider column,
                                       long rowOffset,
                                       long numRows) throws IOException {
        DType type = column.getType();
        long bytesToCopy = numRows * type.getSizeInBytes();
        long srcOffset = rowOffset * type.getSizeInBytes();
        return copySlicedAndPad(out, column, BufferType.DATA, srcOffset, bytesToCopy);
    }

    private static long copySlicedStringData(DataWriter out, ColumnBufferProvider column, long rowOffset,
                                             long numRows) throws IOException {
        if (numRows > 0) {
            long startByteOffset = column.getOffset(rowOffset);
            long endByteOffset = column.getOffset(rowOffset + numRows);
            long bytesToCopy = endByteOffset - startByteOffset;
            long srcOffset = startByteOffset;
            return copySlicedAndPad(out, column, BufferType.DATA, srcOffset, bytesToCopy);
        }
        return 0;
    }

    private static SerializedTableHeader calcHeader(ColumnBufferProvider[] columns,
                                                                        long rowOffset,
                                                                        int numRows) {
        Integer[] colIdxes = IntStream.range(0, columns.length).boxed().toArray(Integer[]::new);
        Arrays.sort(colIdxes, Comparator.comparingInt(i -> columns[i].getType().getTypeId().getNativeId()));
        int[] originalColumnIdxes = Arrays.stream(colIdxes).mapToInt(Integer::intValue).toArray();
        int[] nullCount = Arrays.stream(columns)
                .mapToLong(ColumnBufferProvider::getNullCount)
                .mapToInt(n -> (int) n)
                .toArray();


        long dataLength = getSlicedSerializedDataSizeInBytes(columns, rowOffset, numRows);
        return new SerializedTableHeader(originalColumnIdxes, nullCount, numRows, dataLength);
    }

    private static DataWriter writerFrom(OutputStream out) {
        if (!(out instanceof DataOutputStream)) {
            out = new DataOutputStream(new BufferedOutputStream(out));
        }
        return new DataOutputStreamWriter((DataOutputStream) out);
    }

    private static long getSlicedSerializedDataSizeInBytes(ColumnBufferProvider[] columns, long rowOffset, long numRows) {
        long totalDataSize = 0;
        for (ColumnBufferProvider column : columns) {
            totalDataSize += getSlicedSerializedDataSizeInBytes(column, rowOffset, numRows);
        }
        return totalDataSize;
    }

    private static boolean needsValidityBuffer(long nullCount) {
        return nullCount > 0 || nullCount == ColumnView.UNKNOWN_NULL_COUNT;
    }

    private static long getSlicedSerializedDataSizeInBytes(ColumnBufferProvider column, long rowOffset, long numRows) {
        long totalDataSize = 0;
        DType type = column.getType();
        if (needsValidityBuffer(column.getNullCount())) {
            totalDataSize += padFor64byteAlignment(BitVectorHelper.getValidityLengthInBytes(numRows));
        }

        if (type.hasOffsets()) {
            if (numRows > 0) {
                // Add in size of offsets vector
                totalDataSize += padFor64byteAlignment((numRows + 1) * Integer.BYTES);
                if (type.equals(DType.STRING)) {
                    totalDataSize += padFor64byteAlignment(getRawStringDataLength(column, rowOffset, numRows));
                }
            }
        } else if (type.getSizeInBytes() > 0) {
            totalDataSize += padFor64byteAlignment(column.getType().getSizeInBytes() * numRows);
        }

        if (numRows > 0 && type.isNestedType()) {
            if (type.equals(DType.LIST)) {
                ColumnBufferProvider child = column.getChildProviders()[0];
                long childStartRow = column.getOffset(rowOffset);
                long childNumRows = column.getOffset(rowOffset + numRows) - childStartRow;
                totalDataSize += getSlicedSerializedDataSizeInBytes(child, childStartRow, childNumRows);
            } else if (type.equals(DType.STRUCT)) {
                for (ColumnBufferProvider childProvider : column.getChildProviders()) {
                    totalDataSize += getSlicedSerializedDataSizeInBytes(childProvider, rowOffset, numRows);
                }
            } else {
                throw new IllegalStateException("Unexpected nested type: " + type);
            }
        }
        return totalDataSize;
    }

    private static long getRawStringDataLength(ColumnBufferProvider column, long rowOffset, long numRows) {
        if (numRows <= 0) {
            return 0;
        }
        long start = column.getOffset(rowOffset);
        long end = column.getOffset(rowOffset + numRows);
        return end - start;
    }

    private static long copySlicedOffsets(DataWriter out, ColumnBufferProvider column, long rowOffset,
                                          long numRows) throws IOException {
        if (numRows <= 0) {
            // Don't copy anything, there are no rows
            return 0;
        }
        long bytesToCopy = (numRows + 1) * Integer.BYTES;
        long srcOffset = rowOffset * Integer.BYTES;
        if (rowOffset == 0) {
            return copySlicedAndPad(out, column, BufferType.OFFSET, srcOffset, bytesToCopy);
        }
        HostMemoryBuffer buff = column.getHostBufferFor(BufferType.OFFSET);
        long startOffset = column.getBufferStartOffset(BufferType.OFFSET) + srcOffset;
        if (bytesToCopy >= Integer.MAX_VALUE) {
            throw new IllegalStateException("Copy is too large, need to do chunked copy");
        }
        ByteBuffer bb = buff.asByteBuffer(startOffset, (int) bytesToCopy);
        int start = bb.getInt();
        out.writeIntNativeOrder(0);
        long total = Integer.BYTES;
        for (int i = 1; i < (numRows + 1); i++) {
            int offset = bb.getInt();
            out.writeIntNativeOrder(offset - start);
            total += Integer.BYTES;
        }
        assert total == bytesToCopy;
        long ret = padFor64byteAlignment(out, total);
        return ret;
    }

    private static long copySlicedAndPad(DataWriter out,
                                         ColumnBufferProvider column,
                                         BufferType buffer,
                                         long offset,
                                         long length) throws IOException {
        out.copyDataFrom(column, buffer, offset, length);
        return padFor64byteAlignment(out, length);
    }

    /**
     * Visible for testing
     */
    static abstract class ColumnBufferProvider implements AutoCloseable {

        public abstract DType getType();

        public abstract long getNullCount();

        public abstract long getOffset(long index);

        public abstract long getRowCount();

        public abstract HostMemoryBuffer getHostBufferFor(BufferType buffType);

        public abstract long getBufferStartOffset(BufferType buffType);

        public abstract ColumnBufferProvider[] getChildProviders();

        @Override
        public abstract void close();
    }

    private static ColumnBufferProvider[] providersFrom(HostColumnVector[] columns, boolean closeAtEnd) {
        ColumnBufferProvider[] providers = new ColumnBufferProvider[columns.length];
        for (int i = 0; i < columns.length; i++) {
            providers[i] = new ColumnProvider(columns[i], closeAtEnd);
        }
        return providers;
    }

    private static void closeAll(ColumnBufferProvider[] providers) {
        for (int i = 0; i < providers.length; i++) {
            providers[i].close();
        }
    }

    /**
     * Visible for testing
     */
    static class ColumnProvider extends ColumnBufferProvider {
        private final HostColumnVectorCore column;
        private final boolean closeAtEnd;
        private final ColumnBufferProvider[] childProviders;

        ColumnProvider(HostColumnVectorCore column, boolean closeAtEnd) {
            this.column = column;
            this.closeAtEnd = closeAtEnd;
            if (getType().isNestedType()) {
                int numChildren = column.getNumChildren();
                childProviders = new ColumnBufferProvider[numChildren];
                for (int i = 0; i < numChildren; i++) {
                    childProviders[i] = new ColumnProvider(column.getChildColumnView(i), false);
                }
            } else {
                childProviders = null;
            }
        }

        @Override
        public DType getType() {
            return column.getType();
        }

        @Override
        public long getNullCount() {
            return column.getNullCount();
        }

        @Override
        public long getOffset(long index) {
            return column.getOffsets().getInt(index * Integer.BYTES);
        }

        @Override
        public long getRowCount() {
            return column.getRowCount();
        }

        @Override
        public HostMemoryBuffer getHostBufferFor(BufferType buffType) {
            switch (buffType) {
                case VALIDITY:
                    return column.getValidity();
                case OFFSET:
                    return column.getOffsets();
                case DATA:
                    return column.getData();
                default:
                    throw new IllegalStateException("Unexpected buffer type: " + buffType);
            }
        }

        @Override
        public long getBufferStartOffset(BufferType buffType) {
            // All of the buffers start at 0 for this.
            return 0;
        }

        @Override
        public ColumnBufferProvider[] getChildProviders() {
            return childProviders;
        }

        @Override
        public void close() {
            if (closeAtEnd) {
                column.close();
            }
        }
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
        public abstract void copyDataFrom(HostMemoryBuffer src, long srcOffset, long len)
                throws IOException;

        public void copyDataFrom(ColumnBufferProvider column, BufferType buffType,
                                 long offset, long length) throws IOException {
            HostMemoryBuffer buff = column.getHostBufferFor(buffType);
            long startOffset = column.getBufferStartOffset(buffType);
            copyDataFrom(buff, startOffset + offset, length);
        }

        public void flush() throws IOException {
            // NOOP by default
        }

        public abstract void write(byte[] arr, int offset, int length) throws IOException;
    }

    /**
     * Visible for testing
     */
    static final class DataOutputStreamWriter extends DataWriter {
        private final byte[] arrayBuffer = new byte[1024 * 128];
        private final DataOutputStream dout;

        public DataOutputStreamWriter(DataOutputStream dout) {
            this.dout = dout;
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
            long dataLeft = len;
            while (dataLeft > 0) {
                int amountToCopy = (int) Math.min(arrayBuffer.length, dataLeft);
                src.getBytes(arrayBuffer, 0, srcOffset, amountToCopy);
                dout.write(arrayBuffer, 0, amountToCopy);
                srcOffset += amountToCopy;
                dataLeft -= amountToCopy;
            }
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

    private static final class HostDataWriter extends DataWriter {
        private final HostMemoryBuffer buffer;
        private long offset = 0;

        public HostDataWriter(HostMemoryBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void writeByte(byte b) {
            buffer.setByte(offset, b);
            offset += 1;
        }

        @Override
        public void writeShort(short s) {
            buffer.setShort(offset, s);
            offset += 2;
        }

        @Override
        public void writeInt(int i) {
            buffer.setInt(offset, i);
            offset += 4;
        }

        @Override
        public void writeIntNativeOrder(int i) {
            // This is already in the native order...
            writeInt(i);
        }

        @Override
        public void writeLong(long val) {
            buffer.setLong(offset, val);
            offset += 8;
        }

        @Override
        public void copyDataFrom(HostMemoryBuffer src, long srcOffset, long len) {
            buffer.copyFromHostBuffer(offset, src, srcOffset, len);
            offset += len;
        }

        @Override
        public void write(byte[] arr, int srcOffset, int length) {
            buffer.setBytes(offset, arr, srcOffset, length);
            offset += length;
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
        while (paddedBytes > bytes) {
            out.writeByte((byte) 0);
            bytes++;
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
        private int[] originalColumnIdxes;
        private int[] nullCounts;
        private int numRows;
        private long dataLen;

        private boolean initialized = false;
        private boolean dataRead = false;

        public SerializedTableHeader(DataInputStream din) throws IOException {
            readFrom(din);
        }

        SerializedTableHeader(int[] originalColumnIdxes, int[] nullCounts, int numRows, long dataLen) {
            this.originalColumnIdxes = originalColumnIdxes;
            this.nullCounts = nullCounts;
            this.numRows = numRows;
            this.dataLen = dataLen;
            initialized = true;
            dataRead = true;
        }

        /**
         * Constructor for a row-count only table (no columns)
         */
        public SerializedTableHeader(int numRows) {
            this(new int[0], new int[0], numRows, 0);
        }

        /**
         * Set to true once data is successfully read from a stream by readTableIntoBuffer.
         *
         * @return true if data was read, else false.
         */
        public boolean wasDataRead() {
            return dataRead;
        }

        /**
         * Returns the size of a buffer needed to read data into the stream.
         */
        public long getDataLen() {
            return dataLen;
        }

        /**
         * Returns the number of rows stored in this table.
         */
        public int getNumRows() {
            return numRows;
        }

        /**
         * Returns the number of columns stored in this table
         */
        public int getNumColumns() {
            return originalColumnIdxes.length;
        }

        /**
         * Returns true if the metadata for this table was read, else false indicating an EOF was
         * encountered.
         */
        public boolean wasInitialized() {
            return initialized;
        }

        /**
         * Returns the number of bytes needed to serialize this table header.
         * Note that this is only the metadata for the table (i.e.: column types, row counts, etc.)
         * and does not include the bytes needed to serialize the table data.
         */
        public long getSerializedHeaderSizeInBytes() {
            // table header always has:
            // - 4-byte magic number
            // - 2-byte version number
            // - 4-byte column count
            // - 4 * column count bytes for the original column indexes
            // - 4 * column count bytes for the null counts
            // - 4-byte row count
            // - 8-byte data buffer length
            return 4 + 2 + 4 + 4L * getNumColumns() + 4L * getNumColumns() + 4 + 8;
        }

        /**
         * Returns the number of bytes needed to serialize this table header and the table data.
         */
        public long getTotalSerializedSizeInBytes() {
            return getSerializedHeaderSizeInBytes() + dataLen;
        }

        private void readFrom(DataInputStream din) throws IOException {
            try {
                int num = din.readInt();
                if (num != SER_FORMAT_MAGIC_NUMBER) {
                    throw new IllegalStateException("THIS DOES NOT LOOK LIKE CUDF SERIALIZED DATA. " +
                            "Expected magic number " + SER_FORMAT_MAGIC_NUMBER + " Found " + num);
                }
            } catch (EOFException e) {
                // If we get an EOF at the very beginning don't treat it as an error because we may
                // have finished reading everything...
                return;
            }
            short version = din.readShort();
            if (version != VERSION_NUMBER) {
                throw new IllegalStateException("READING THE WRONG SERIALIZATION FORMAT VERSION FOUND "
                        + version + " EXPECTED " + VERSION_NUMBER);
            }
            int numColumns = din.readInt();
            originalColumnIdxes = new int[numColumns];
            for (int i = 0; i < numColumns; i++) {
                originalColumnIdxes[i] = din.readInt();
            }

            nullCounts = new int[numColumns];
            for (int i = 0; i < numColumns; i++) {
                nullCounts[i] = din.readInt();
            }

            numRows = din.readInt();

            dataLen = din.readLong();
            initialized = true;
        }

        public void writeTo(DataWriter dout) throws IOException {
            // Now write out the data
            dout.writeInt(SER_FORMAT_MAGIC_NUMBER);
            dout.writeShort(VERSION_NUMBER);
            dout.writeInt(originalColumnIdxes.length);
            for (int originalColumnIdx : originalColumnIdxes) {
                dout.writeInt(originalColumnIdx);
            }
            for (int nullCount : nullCounts) {
                dout.writeInt(nullCount);
            }
            dout.writeInt(numRows);

            dout.writeLong(dataLen);
        }
    }

    static long copySlicedValidity(DataWriter out,
                                   ColumnBufferProvider column,
                                   long rowOffset,
                                   long numRows) throws IOException {
        long validityLen = BitVectorHelper.getValidityLengthInBytes(numRows);
        long byteOffset = (rowOffset / 8);
        long bytesLeft = validityLen;

        int lshift = (int) rowOffset % 8;
        if (lshift == 0) {
            out.copyDataFrom(column, BufferType.VALIDITY, byteOffset, bytesLeft);
        } else {
            byte[] arrayBuffer = new byte[128 * 1024];
            int rowsStoredInArray = 0;
            int rowsLeftInBatch = (int) numRows;
            int validityBitOffset = (int) rowOffset;
            while (rowsLeftInBatch > 0) {
                int rowsStoredJustNow = copyPartialValidity(arrayBuffer, rowsStoredInArray, column, validityBitOffset, rowsLeftInBatch);
                assert rowsStoredJustNow > 0;
                rowsLeftInBatch -= rowsStoredJustNow;
                rowsStoredInArray += rowsStoredJustNow;
                validityBitOffset += rowsStoredJustNow;
                if (rowsStoredInArray == arrayBuffer.length * 8) {
                    out.write(arrayBuffer, 0, arrayBuffer.length);
                    rowsStoredInArray = 0;
                }
            }
            if (rowsStoredInArray > 0) {
                out.write(arrayBuffer, 0, (rowsStoredInArray + 7) / 8);
            }
        }
        return padFor64byteAlignment(out, validityLen);
    }

    private static int copyPartialValidity(byte[] dest,
                                           int destBitOffset,
                                           ColumnBufferProvider provider,
                                           int srcBitOffset,
                                           int lengthBits) {
        HostMemoryBuffer src = provider.getHostBufferFor(BufferType.VALIDITY);
        long baseSrcByteOffset = provider.getBufferStartOffset(BufferType.VALIDITY);

        int destStartBytes = destBitOffset / 8;
        int destStartBitOffset = destBitOffset % 8;
        long srcStartBytes = baseSrcByteOffset + (srcBitOffset / 8);
        int srcStartBitOffset = srcBitOffset % 8;
        int availableDestBits = (dest.length * 8) - destBitOffset;
        int bitsToCopy = Math.min(lengthBits, availableDestBits);

        int lastIndex = (bitsToCopy + destStartBitOffset + 7) / 8;

        byte allBitsSet = ~0;
        byte firstSrcMask = (byte) (allBitsSet << destStartBitOffset);

        int srcShift = destStartBitOffset - srcStartBitOffset;
        if (srcShift > 0) {
            // Shift left. If we are going to shift this is the path typically taken.

            byte current = src.getByte(srcStartBytes);
            byte result = (byte) (current << srcShift);
            // The first time we need to include any data already in dest.
            result |= dest[destStartBytes] & ~firstSrcMask;
            dest[destStartBytes] = result;

            // Keep the previous bytes around so we don't have to keep reading from src, which is not free
            byte previous = current;

            for (int index = 1; index < lastIndex; index++) {
                current = src.getByte(index + srcStartBytes);
                result = (byte) (current << srcShift);
                result |= (previous & 0xFF) >>> (8 - srcShift);
                dest[index + destStartBytes] = result;
                previous = current;
            }
            return bitsToCopy;
        } else if (srcShift < 0) {
            srcShift = -srcShift;

            // shifting right only happens when the buffer runs out of space.

            byte result = src.getByte(srcStartBytes);
            result = (byte) ((result & 0xFF) >>> srcShift);
            byte next = 0;
            if (srcStartBytes + 1 < src.getLength()) {
                next = src.getByte(srcStartBytes + 1);
            }
            result |= (byte) (next << 8 - srcShift);
            result &= firstSrcMask;

            // The first time through we need to include the data already in dest.
            result |= dest[destStartBytes] & ~firstSrcMask;
            dest[destStartBytes] = result;

            for (int index = 1; index < lastIndex - 1; index++) {
                result = next;
                result = (byte) ((result & 0xFF) >>> srcShift);
                next = src.getByte(srcStartBytes + index + 1);
                result |= (byte) (next << 8 - srcShift);
                dest[index + destStartBytes] = result;
            }

            int idx = lastIndex - 1;
            if (idx > 0) {
                result = next;
                result = (byte) ((result & 0xFF) >>> srcShift);
                next = 0;
                if (srcStartBytes + idx + 1 < src.getLength()) {
                    next = src.getByte(srcStartBytes + idx + 1);
                }
                result |= (byte) (next << 8 - srcShift);
                dest[idx + destStartBytes] = result;
            }
            return bitsToCopy;
        } else {
            src.getBytes(dest, destStartBytes, srcStartBytes, (bitsToCopy + 7) / 8);
            return bitsToCopy;
        }
    }
}
