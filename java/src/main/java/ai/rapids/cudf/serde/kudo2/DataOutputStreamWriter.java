package ai.rapids.cudf.serde.kudo2;

import ai.rapids.cudf.HostMemoryBuffer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * Visible for testing
 */
final class DataOutputStreamWriter extends DataWriter {
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
