package ai.rapids.cudf.serde.kudo2;

import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.Schema;
import ai.rapids.cudf.schema.SchemaWithColumnsVisitor;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

class BufferCompressedSerializer implements SchemaWithColumnsVisitor<Long, Long> {
    private final SliceInfo root;
    private final DataWriter writer;
    private final int compressionLevel;

    BufferCompressedSerializer(long rowOffset, long numRows, DataWriter writer, int compressionLevel) {
        this.root = new SliceInfo(rowOffset, numRows);
        this.writer = writer;
        this.compressionLevel = compressionLevel;
    }

    @Override
    public Long visitTopSchema(Schema schema, List<Long> children) {
        return children
                .stream()
                .mapToLong(Long::longValue)
                .sum();
    }

    @Override
    public Long visitStruct(Schema structType, HostColumnVectorCore col, List<Long> children) {
        throw new UnsupportedOperationException("Struct not supported yet!");
    }

    @Override
    public Long preVisitList(Schema listType, HostColumnVectorCore col) {
        throw new UnsupportedOperationException("List not supported yet!");
    }

    @Override
    public Long visitList(Schema listType, HostColumnVectorCore col, Long preVisitResult, Long childResult) {
        throw new UnsupportedOperationException("List not supported yet!");
    }

    @Override
    public Long visit(Schema primitiveType, HostColumnVectorCore col) {
        try {
            return this.copySlicedValidity(col) +
                    this.copySlicedOffset(col) +
                    this.copySlicedData(col);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long copySlicedValidity(HostColumnVectorCore column) throws IOException {
        if (column.getValidity() != null) {
            try (HostMemoryBuffer buff = column.getValidity().slice(
                    root.getValidityBufferInfo().getBufferOffset(),
                    root.getValidityBufferInfo().getBufferLength())) {
                return compressBuffer(buff);
            }
        } else {
            return 0;
        }
    }

    private long compressBuffer(HostMemoryBuffer buffer) {
        byte[] tmp = new byte[16 * 1024];
        long paddingLength = Kudo2Serializer.padFor64byteAlignment(buffer.getLength());
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream((int) (buffer.getLength() / 8))) {
            try (ZstdCompressorOutputStream zout = new ZstdCompressorOutputStream(bos, compressionLevel)) {
                long remaining = buffer.getLength();
                long offset = 0;
                while (remaining > 0) {
                    int toRead = (int) Math.min(remaining, tmp.length);
                    buffer.getBytes(tmp, 0, offset, toRead);
                    zout.write(tmp, 0, toRead);
                    offset += toRead;
                    remaining -= toRead;
                }

                if (paddingLength > buffer.getLength()) {
                    zout.write(Kudo2Serializer.PADDING, 0, (int) (paddingLength - buffer.getLength()));
                }
            }

            byte[] compressed = bos.toByteArray();
            writer.writeLong(paddingLength);
            writer.writeLong(compressed.length);
            writer.write(compressed, 0, compressed.length);
            return compressed.length + 2 * Long.BYTES;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long writeEmptyBuffer() {
        try {
            writer.writeLong(0L);
            writer.writeLong(0L);

            return 2 * Long.BYTES;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long copySlicedOffset(HostColumnVectorCore column) throws IOException {
        if (root.rowCount <= 0 || column.getOffsets() == null) {
            // Don't copy anything, there are no rows
            return 0;
        }
        long bytesToCopy = (root.rowCount + 1) * Integer.BYTES;
        long srcOffset = root.offset * Integer.BYTES;
        try (HostMemoryBuffer buff = column.getOffsets().slice(srcOffset, bytesToCopy)) {
            return compressBuffer(buff);
        }
    }

    private long copySlicedData(HostColumnVectorCore column) throws IOException {
        if (root.rowCount > 0) {
            DType type = column.getType();
            if (type.equals(DType.STRING)) {
                long startByteOffset = column.getOffsets().getInt(root.offset * Integer.BYTES);
                long endByteOffset = column.getOffsets().getInt((root.offset + root.rowCount) * Integer.BYTES);
                long bytesToCopy = endByteOffset - startByteOffset;
                if (column.getData() == null) {
                    if (bytesToCopy != 0) {
                        throw new IllegalStateException("String column has no data buffer, " +
                                "but bytes to copy is not zero: " + bytesToCopy);
                    } else {
                        return writeEmptyBuffer();
                    }
                } else {
                    try (HostMemoryBuffer buffer = column.getData().slice(startByteOffset, bytesToCopy)) {
                        return compressBuffer(buffer);
                    }
                }
            } else if (type.getSizeInBytes() > 0) {
                long bytesToCopy = root.rowCount * type.getSizeInBytes();
                long srcOffset = root.offset * type.getSizeInBytes();
                try (HostMemoryBuffer buffer = column.getData().slice(srcOffset, bytesToCopy)) {
                    return compressBuffer(buffer);
                }
            } else {
                return 0;
            }
        } else {
            return 0;
        }
    }
}
