package ai.rapids.cudf.utils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

// See https://github.com/apache/parquet-java/blob/master/parquet-common/src/main/java/org/apache/parquet/bytes/ByteBufferInputStream.java
public class ByteBufferInputStream extends InputStream {

    // Used to maintain the deprecated behavior of instantiating ByteBufferInputStream directly
    private final ByteBufferInputStream delegate;

    public static ByteBufferInputStream wrap(ByteBuffer... buffers) {
        if (buffers.length == 1) {
            return new SingleBufferInputStream(buffers[0]);
        } else {
            return new MultiBufferInputStream(Arrays.asList(buffers));
        }
    }

    public static ByteBufferInputStream wrap(List<ByteBuffer> buffers) {
        if (buffers.size() == 1) {
            return new SingleBufferInputStream(buffers.get(0));
        } else {
            return new MultiBufferInputStream(buffers);
        }
    }

    ByteBufferInputStream() {
        delegate = null;
    }

    /**
     * @param buffer the buffer to be wrapped in this input stream
     * @deprecated Will be removed in 2.0.0; Use {@link #wrap(ByteBuffer...)} instead
     */
    @Deprecated
    public ByteBufferInputStream(ByteBuffer buffer) {
        delegate = wrap(buffer);
    }

    /**
     * @param buffer the buffer to be wrapped in this input stream
     * @param offset the offset of the data in the buffer
     * @param count  the number of bytes to be read from the buffer
     * @deprecated Will be removed in 2.0.0; Use {@link #wrap(ByteBuffer...)} instead
     */
    @Deprecated
    public ByteBufferInputStream(ByteBuffer buffer, int offset, int count) {
        ByteBuffer temp = buffer.duplicate();
        temp.position(offset);
        ByteBuffer byteBuf = temp.slice();
        byteBuf.limit(count);
        delegate = wrap(byteBuf);
    }

    /**
     * @return the slice of the byte buffer inside this stream
     * @deprecated Will be removed in 2.0.0; Use {@link #slice(int)} instead
     */
    @Deprecated
    public ByteBuffer toByteBuffer() {
        try {
            return slice(available());
        } catch (EOFException e) {
            throw new RuntimeException(e);
        }
    }

    public long position() {
        return delegate.position();
    }

    public void skipFully(long n) throws IOException {
        long skipped = skip(n);
        if (skipped < n) {
            throw new EOFException("Not enough bytes to skip: " + skipped + " < " + n);
        }
    }

    public int read(ByteBuffer out) {
        return delegate.read(out);
    }

    public ByteBuffer slice(int length) throws EOFException {
        return delegate.slice(length);
    }

    public List<ByteBuffer> sliceBuffers(long length) throws EOFException {
        return delegate.sliceBuffers(length);
    }

    public ByteBufferInputStream sliceStream(long length) throws EOFException {
        return ByteBufferInputStream.wrap(sliceBuffers(length));
    }

    public List<ByteBuffer> remainingBuffers() {
        return delegate.remainingBuffers();
    }

    public ByteBufferInputStream remainingStream() {
        return ByteBufferInputStream.wrap(remainingBuffers());
    }

    public int read() throws IOException {
        return delegate.read();
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return delegate.read(b, off, len);
    }

    public long skip(long n) {
        return delegate.skip(n);
    }

    public int available() {
        return delegate.available();
    }

    public void mark(int readlimit) {
        delegate.mark(readlimit);
    }

    public void reset() throws IOException {
        delegate.reset();
    }

    public boolean markSupported() {
        return delegate.markSupported();
    }
}
