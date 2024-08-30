package ai.rapids.cudf.serde.kudo;

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

    public HostMemoryBuffer getBuffer() {
        return buffer;
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
