package ai.rapids.cudf.serde.kudo2;

import ai.rapids.cudf.*;
import ai.rapids.cudf.schema.Visitors;
import ai.rapids.cudf.serde.TableSerializer;
import ai.rapids.cudf.utils.Arms;
import ai.rapids.cudf.utils.ByteBufferInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Kudo2Serializer implements TableSerializer {
    static final byte[] PADDING = new byte[64];

    static {
        Arrays.fill(PADDING, (byte) 0);
    }


    @Override
    public String version() {
        return "v1";
    }

    private final CompressionMode compressionMode;
    private final int columnarBatchModeMinRows;
    private final long columnarBatchModeMaxBytes;
    private final int compressionLevel;

    public Kudo2Serializer(CompressionMode compressionMode, int columnarBatchModeMinRows,
                           long columnarBatchModeMaxBytes,
                           int compressionLevel) {
        this.compressionMode = compressionMode;
        this.columnarBatchModeMinRows = columnarBatchModeMinRows;
        this.columnarBatchModeMaxBytes = columnarBatchModeMaxBytes;
        this.compressionLevel = compressionLevel;
    }

    CompressionMode getCompressionMode() {
        return compressionMode;
    }

    public int getCompressionLevel() {
        return compressionLevel;
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
            SerializedTableHeader header = new SerializedTableHeader(0,
                    (int) numRows, CompressionMode.AUTO, 0, new byte[0], 0);
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

            if (header.getNumRows() <= 0) {
                throw new IllegalArgumentException("Number of rows must be > 0, but was " + header.getNumRows());
            }

            // Header only
            if (header.getNumColumns() == 0) {
                return new SerializedTable(header, null);
            }

            HostMemoryBuffer buffer = HostMemoryBuffer.allocate(header.getCompressedDataLen());
            buffer.copyFromStream(0, din, header.getCompressedDataLen());
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

    private long writeSliced(HostColumnVector[] columns, DataWriter output, long rowOffset, long numRows) throws Exception {
        List<HostColumnVector> columnList = Arrays
                .stream(columns)
                .collect(Collectors.toList());

        Schema.Builder schemaBuilder = Schema.builder();
        for (int i = 0; i < columns.length; i++) {
            columns[i].toSchema("col_" + i + "_", schemaBuilder);
        }
        Schema schema = schemaBuilder.build();


        SerializedTableHeaderCalc headerCalc = new SerializedTableHeaderCalc(this, rowOffset, numRows);
        SerializedTableHeader header = Visitors.visitSchemaWithColumns(schema, columnList, headerCalc);

        try (ByteArrayOutputStream bout = new ByteArrayOutputStream(32 * 1024)) {
            DataWriter tmpDataWriter = new DataOutputStreamWriter(new DataOutputStream(bout));

            long compressedDataLen;

            switch (header.getCompressMode()) {
                case BUFFER:
                    compressedDataLen = Visitors.visitSchemaWithColumns(schema, columnList,
                            new BufferCompressedSerializer(rowOffset, numRows, tmpDataWriter, compressionLevel));
                    break;
                case COLUMNAR_BATCH:
                    compressedDataLen = Visitors.visitSchemaWithColumns(schema, columnList,
                            new ColumnBatchCompressedSerializer(rowOffset, numRows, tmpDataWriter, compressionLevel));
                    break;
                default:
                    throw new IllegalStateException("Unknown compression mode after header: " + header.getCompressMode());
            }

            tmpDataWriter.flush();

            header.setCompressedDataLen(compressedDataLen);
            header.writeTo(output);

            byte[] compressOutput = bout.toByteArray();
            if (compressOutput.length != compressedDataLen) {
                throw new IllegalArgumentException("Compressed data length mismatch. Expected " + compressedDataLen +
                        " but got " + compressOutput.length);
            }

            output.write(compressOutput, 0, compressOutput.length);
            output.flush();
        }

        return header.getSerializedSize() + header.getTotalDataLen();
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

    CompressionMode decide(int columnCount, long totalBytes) {
        switch (compressionMode) {
            case AUTO:
                if (columnCount >= columnarBatchModeMinRows && totalBytes <= columnarBatchModeMaxBytes) {
                    return CompressionMode.COLUMNAR_BATCH;
                } else {
                    return CompressionMode.BUFFER;
                }
            default:
                return compressionMode;
        }
    }


    static long padFor64byteAlignment(long orig) {
        return ((orig + 63) / 64) * 64;
    }

    static long padFor64byteAlignment(DataWriter out, long bytes) throws IOException {
        final long paddedBytes = padFor64byteAlignment(bytes);
        if (paddedBytes > bytes) {
            out.write(PADDING, 0, (int) (paddedBytes - bytes));
        }
        return paddedBytes;
    }

    static HostMemoryBuffer decompressHostBuffer(HostMemoryBuffer compressedBuffer, long decompressedSize) {
        try (InputStream is = ByteBufferInputStream.wrap(compressedBuffer.asByteBuffer())) {
            try (ZstdCompressorInputStream in = new ZstdCompressorInputStream(is)) {
                return Arms.closeIfException(HostMemoryBuffer.allocate(decompressedSize), buffer -> {
                    try {
                        buffer.copyFromStream(0, in, decompressedSize);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return buffer;
                });
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}