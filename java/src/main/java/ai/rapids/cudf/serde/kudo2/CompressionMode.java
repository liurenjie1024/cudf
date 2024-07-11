package ai.rapids.cudf.serde.kudo2;

public enum CompressionMode {
    AUTO,
    COLUMNAR_BATCH,
    BUFFER;

    public static CompressionMode from(byte mode) {
        switch (mode) {
            case 0:
                return AUTO;
            case 1:
                return COLUMNAR_BATCH;
            case 2:
                return BUFFER;
            default:
                throw new IllegalArgumentException("Unknown compress mode: " + mode);
        }
    }

    public byte toByte() {
        switch (this) {
            case AUTO:
                return 0;
            case COLUMNAR_BATCH:
                return 1;
            case BUFFER:
                return 2;
            default:
                throw new IllegalArgumentException("Unknown compress mode: " + this);
        }
    }
}
