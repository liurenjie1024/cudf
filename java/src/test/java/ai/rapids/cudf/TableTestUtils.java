package ai.rapids.cudf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

public class TableTestUtils {

    /**
     * Helper for constructing BigInteger from int
     *
     * @param x Integer value
     * @return BigInteger equivalent of x
     */
    public static BigInteger big(int x) {
        return new BigInteger("" + x);
    }

    /**
     * Helper to get scalar for preceding == Decimal(value),
     * with data width depending upon the order-by
     * column index:
     * orderby_col_idx = 2 -> Decimal32
     * orderby_col_idx = 3 -> Decimal64
     * orderby_col_idx = 4 -> Decimal128
     */
    public static Scalar getDecimalScalarRangeBounds(int scale, int unscaledValue, int orderby_col_idx) {
        switch (orderby_col_idx) {
            case 2:
                return Scalar.fromDecimal(scale, unscaledValue);
            case 3:
                return Scalar.fromDecimal(scale, Long.valueOf(unscaledValue));
            case 4:
                return Scalar.fromDecimal(scale, big(unscaledValue));
            default:
                throw new IllegalStateException("Unexpected order by column index: "
                        + orderby_col_idx);
        }
    }

    /**
     * Helper to get scalar for preceding == Decimal(value),
     * with data width depending upon the order-by column index:
     * orderby_col_idx = 2 -> FLOAT32
     * orderby_col_idx = 3 -> FLOAT64
     */
    public static Scalar getFloatingPointScalarRangeBounds(float value, int orderby_col_idx) {
        switch (orderby_col_idx) {
            case 2:
                return Scalar.fromFloat(value);
            case 3:
                return Scalar.fromDouble(Double.valueOf(value));
            default:
                throw new IllegalStateException("Unexpected order by column index: "
                        + orderby_col_idx);
        }
    }

    public static HostColumnVector.StructData struct(Object... values) {
        return new HostColumnVector.StructData(values);
    }

    public static HostColumnVector.StructData[] structs(HostColumnVector.StructData... values) {
        return values;
    }

    public static String[] strings(String... values) {
        return values;
    }

    public static Integer[] integers(Integer... values) {
        return Arrays
                .stream(values)
                .toArray(Integer[]::new);
    }

    public static ColumnVector decimalFromBoxedInts(boolean isDec64, int scale, Integer... values) {
        BigDecimal[] decimals = new BigDecimal[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                decimals[i] = null;
            } else {
                decimals[i] = BigDecimal.valueOf(values[i], -scale);
            }
        }
        DType type = isDec64 ? DType.create(DType.DTypeEnum.DECIMAL64, scale) : DType.create(DType.DTypeEnum.DECIMAL32, scale);
        return ColumnVector.build(type, decimals.length, (b) -> b.appendBoxed(decimals));
    }

    /**
     * Get schema of a table. Field names are random.
     */
    public static Schema schemaOf(Table t) {
        return t.toSchema();
    }
}
