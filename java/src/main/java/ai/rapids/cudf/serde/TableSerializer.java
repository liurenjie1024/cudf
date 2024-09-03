package ai.rapids.cudf.serde;

import ai.rapids.cudf.*;
import ai.rapids.cudf.utils.Arms;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.IntStream;

public interface TableSerializer {
    /**
     * Get the version of the serializer format.
     * <p>
     * This method is mainly used for debugging and testing purposes.
     */
    String version();

    default long writeToStream(Table table, OutputStream out, long rowOffset, long numRows) {

        HostColumnVector[] columns = null;
        try {
            columns = IntStream.range(0, table.getNumberOfColumns())
                    .mapToObj(table::getColumn)
                    .map(ColumnView::copyToHost)
                    .toArray(HostColumnVector[]::new);
            return writeToStream(columns, out, rowOffset, numRows);
        } finally {
            if (columns != null) {
                for (HostColumnVector column : columns) {
                    column.close();
                }
            }
        }
    }

    long writeToStream(HostColumnVector[] columns, OutputStream out, long rowOffset,
                       long numRows);


    /**
     * Write a rowcount only header to the output stream in a case
     * where a columnar batch with no columns but a non zero row count is received
     * @param out the stream to write the serialized table out to.
     * @param numRows the number of rows to write out.
     */
    long writeRowsToStream(OutputStream out, long numRows);

    /**
     * Read a table from a stream, with given schema.
     *
     * @return An intermediate buffer of the table. We use object here since it's implementation specific. The
     * caller determines when to merge buffers.
     */
    Object readOneTableBuffer(InputStream in);

    /**
     * Merge multiple table buffers returned in {@link #readOneTableBuffer}  into a single table on gpu.
     *
     * @param buffers An array of table buffers.
     * @param schema  The schema of the table.
     * @return The merged table.
     */
    default Table mergeTable(List<Object> buffers, Schema schema) {
        List<HostColumnVector> children = mergeToHost(buffers, schema);

        ColumnVector[] cols = new ColumnVector[children.size()];
        try (CloseableArray<ColumnVector> array = CloseableArray.wrap(cols)) {
            for (int i = 0; i < children.size(); i += 1) {
                array.set(i, children.get(i).copyToDevice());
            }
            return new Table(array.getArray());
        } finally {
            Arms.closeQuietly(children);
        }
    }

    /**
     * Merge multiple serialized table buffers returned in {@link #readOneTableBuffer}  into a single table on host.
     *
     * @param buffers An array of table buffers.
     * @param schema  The schema of the table.
     * @return The merged table.
     */
    List<HostColumnVector> mergeToHost(List<Object> buffers, Schema schema);
}
