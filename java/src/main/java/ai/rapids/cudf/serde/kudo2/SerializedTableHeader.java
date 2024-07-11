package ai.rapids.cudf.serde.kudo2;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Holds the metadata about a serialized table. If this is being read from a stream
 * isInitialized will return true if the metadata was read correctly from the stream.
 * It will return false if an EOF was encountered at the beginning indicating that
 * there was no data to be read.
 */
public final class SerializedTableHeader {
    /**
     * Magic number "KUD2" in ASCII.
     */
    private static final int SER_FORMAT_MAGIC_NUMBER = 0x4B554488;
    private static final short VERSION_NUMBER = 0x0001;

    // Useful for reducing calculations in writing.
    private int offset;
    private int numRows;
    // See CompressionMode for the values
    private CompressionMode compressMode;

    // The length of uncompressed data length
    private long totalDataLen;
    // This is used to indicate the validity buffer for the columns.
    // 1 means that this column has validity data, 0 means it does not.
    private byte[] hasValidityBuffer;

    // The length of the compressed data.
    private long compressedDataLen;

    private boolean initialized = false;


    public SerializedTableHeader(DataInputStream din) throws IOException {
        readFrom(din);
    }

    SerializedTableHeader(int offset,
                          int numRows,
                          CompressionMode compressionMode,
                          long totalDataLen,
                          byte[] hasValidityBuffer,
                          long compressedDataLen) {
        Objects.requireNonNull(compressionMode, "compressionMode cannot be null");
        Objects.requireNonNull(hasValidityBuffer, "hasValidityBuffer cannot be null");
        this.offset = offset;
        this.numRows = numRows;
        this.compressMode = compressionMode;
        this.totalDataLen = totalDataLen;
        this.hasValidityBuffer = hasValidityBuffer;
        this.compressedDataLen = compressedDataLen;

        this.initialized = true;
    }

    public void setCompressedDataLen(long compressedDataLen) {
        this.compressedDataLen = compressedDataLen;
    }

    public long getCompressedDataLen() {
        return compressedDataLen;
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
        return 4 + 2 + 4 + 4 + 1 + 8 + 8 + 4 + hasValidityBuffer.length;
    }

    public int getNumColumns() {
        return hasValidityBuffer.length;
    }

    public CompressionMode getCompressMode() {
        return compressMode;
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

        compressMode = CompressionMode.from(din.readByte());

        totalDataLen = din.readLong();
        compressedDataLen = din.readLong();
        int validityBufferLength = din.readInt();
        hasValidityBuffer = new byte[validityBufferLength];
        din.readFully(hasValidityBuffer);

        initialized = true;
    }

    void writeTo(DataWriter dout) throws IOException {
        // Now write out the data
        dout.writeInt(SER_FORMAT_MAGIC_NUMBER);
        dout.writeShort(VERSION_NUMBER);

        dout.writeInt(offset);
        dout.writeInt(numRows);
        dout.writeByte(compressMode.toByte());
        dout.writeLong(totalDataLen);
        dout.writeLong(compressedDataLen);
        dout.writeInt(hasValidityBuffer.length);
        dout.write(hasValidityBuffer, 0, hasValidityBuffer.length);
    }

    @Override
    public String toString() {
        return "SerializedTableHeader{" +
                "offset=" + offset +
                ", numRows=" + numRows +
                ", compressMode=" + compressMode +
                ", totalDataLen=" + totalDataLen +
                ", hasValidityBuffer=" + Arrays.toString(hasValidityBuffer) +
                ", compressedDataLen=" + compressedDataLen +
                ", initialized=" + initialized +
                '}';
    }
}
