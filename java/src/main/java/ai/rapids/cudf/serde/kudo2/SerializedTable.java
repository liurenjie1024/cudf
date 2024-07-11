package ai.rapids.cudf.serde.kudo2;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.Schema;
import ai.rapids.cudf.schema.Visitors;

import java.util.List;

public class SerializedTable implements AutoCloseable {
    private final SerializedTableHeader header;
    private final HostMemoryBuffer buffer;

    SerializedTable(SerializedTableHeader header, HostMemoryBuffer buffer) {
        this.header = header;
        this.buffer = buffer;
    }

    public SerializedTableHeader getHeader() {
        return header;
    }

    HostMemoryBuffer getBuffer() {
        return buffer;
    }

    List<ColumnVector> deserialize(Schema schema) {
        switch (header.getCompressMode()) {
            case BUFFER:
                try(BufferCompressedDeserializer deserializer = new BufferCompressedDeserializer(this)) {
                    return Visitors.visitSchema(schema, deserializer);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            case COLUMNAR_BATCH:
                try(ColumnBatchedCompressedDeserializer deserializer = new ColumnBatchedCompressedDeserializer(this)) {
                    return Visitors.visitSchema(schema, deserializer);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            default:
                throw new IllegalArgumentException("Unsupported compression mode in deserialization: " +
                        header.getCompressMode());
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
