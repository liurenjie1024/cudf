package ai.rapids.cudf.serde.kudo2;

import ai.rapids.cudf.*;
import ai.rapids.cudf.schema.SchemaWithColumnsVisitor;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

class ColumnBatchCompressedSerializer implements SchemaWithColumnsVisitor<Void, Long> {
    private final SliceInfo root;
    private final ByteArrayOutputStream bout;
    private final ZstdCompressorOutputStream compressor;
    private final DataWriter writer;
    private final int compressionLevel;


    ColumnBatchCompressedSerializer(long rowOffset, long numRows, DataWriter writer, int compressionLevel) {
        try {
            this.root = new SliceInfo(rowOffset, numRows);
            this.bout = new ByteArrayOutputStream(32 * 1024);
            this.compressor = new ZstdCompressorOutputStream(bout);
            this.writer = writer;
            this.compressionLevel = compressionLevel;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Long visitTopSchema(Schema schema, List<Void> children) {
        try {
            this.compressor.close();

            byte[] compressed = bout.toByteArray();
            writer.write(compressed, 0, compressed.length);
            return (Long) (long) compressed.length;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Void visitStruct(Schema structType, HostColumnVectorCore col, List<Void> children) {
        throw new UnsupportedOperationException("Struct not supported yet!");
    }

    @Override
    public Void preVisitList(Schema listType, HostColumnVectorCore col) {
        throw new UnsupportedOperationException("List not supported yet!");
    }

    @Override
    public Void visitList(Schema listType, HostColumnVectorCore col, Void preVisitResult, Void childResult) {
        throw new UnsupportedOperationException("List not supported yet!");
    }

    @Override
    public Void visit(Schema primitiveType, HostColumnVectorCore col) {
        try {
            this.copySlicedValidity(col);
            this.copySlicedOffset(col);
            this.copySlicedData(col);
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void copySlicedValidity(HostColumnVectorCore column) throws IOException {
        if (column.getValidity() != null) {
            HostMemoryBuffer buff = column.getValidity();
            try (HostMemoryBuffer slice = buff.slice(root.getValidityBufferInfo().getBufferOffset(),
                    root.getValidityBufferInfo().getBufferLength())) {
                writeBuffer(slice);
            }
        }
    }

    private void copySlicedOffset(HostColumnVectorCore column) throws IOException {
        if (root.rowCount <= 0 || column.getOffsets() == null) {
            // Don't copy anything, there are no rows
            return;
        }
        long bytesToCopy = (root.rowCount + 1) * Integer.BYTES;
        long srcOffset = root.offset * Integer.BYTES;
        HostMemoryBuffer buff = column.getOffsets();
        try(HostMemoryBuffer slice = buff.slice(srcOffset, bytesToCopy)) {
            writeBuffer(slice);
        }
    }

    private void copySlicedData(HostColumnVectorCore column) throws IOException {
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
                    }
                } else {
                    try(HostMemoryBuffer slice = column.getData().slice(startByteOffset, bytesToCopy)) {
                        writeBuffer(slice);
                    }
                }
            } else if (type.getSizeInBytes() > 0) {
                long bytesToCopy = root.rowCount * type.getSizeInBytes();
                long srcOffset = root.offset * type.getSizeInBytes();
                try (HostMemoryBuffer slice = column.getData().slice(srcOffset, bytesToCopy)) {
                    writeBuffer(slice);
                }
            }
        }
    }

    private void writeBuffer(HostMemoryBuffer buffer) {
        try {
            byte[] data = new byte[16 * 1024];
            long remaining = buffer.getLength();
            long offset = 0;
            while (remaining > 0) {
                int toRead = (int) Math.min(data.length, remaining);
                buffer.getBytes(data, 0, offset, toRead);
                compressor.write(data, 0, toRead);
                remaining -= toRead;
                offset += toRead;
            }

            long paddingLength = Kudo2Serializer.padFor64byteAlignment(buffer.getLength());
            if (paddingLength > buffer.getLength()) {
                compressor.write(Kudo2Serializer.PADDING, 0, (int) (paddingLength - buffer.getLength()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
