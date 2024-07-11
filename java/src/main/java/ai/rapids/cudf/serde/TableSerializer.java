package ai.rapids.cudf.serde;

import ai.rapids.cudf.HostColumnVector;

import java.io.OutputStream;

public interface TableSerializer {
    void writeToStream(HostColumnVector[] columns, OutputStream out, long rowOffset,
                       long numRows);
}
