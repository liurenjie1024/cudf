package ai.rapids.cudf.serde;

import ai.rapids.cudf.*;
import ai.rapids.cudf.serde.kudo2.CompressionMode;
import ai.rapids.cudf.serde.kudo2.Kudo2Serializer;
import ai.rapids.cudf.serde.kudo2.SerializedTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static ai.rapids.cudf.AssertUtils.assertTablesAreEqual;
import static ai.rapids.cudf.TableTestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Execution(ExecutionMode.SAME_THREAD)
public class Kudo2SerializerTest extends CudfTestBase {
    private static Table table;

    @BeforeAll
    public static void setupAll() {
        table = buildTestTable();
    }

    @AfterAll
    public static void tearDownAll() {
        if (table != null) {
            table.close();
        }
    }

    static class TestParam {
        int sliceStep;
        Kudo2Serializer serializer;
    }

    static Stream<TestParam> params() {
        IntStream sliceStepStream = IntStream.rangeClosed(1, (int) table.getRowCount());
        List<Kudo2Serializer> serializers = Arrays.asList(
                new Kudo2Serializer(CompressionMode.BUFFER, 0, 0, 1)
                , new Kudo2Serializer(CompressionMode.COLUMNAR_BATCH, 0, 0, 1)
                , new Kudo2Serializer(CompressionMode.AUTO, 1, 200, 1)
        );

        return sliceStepStream
                .boxed()
                .flatMap(sliceStep -> serializers.stream().map(serializer -> {
                    TestParam param = new TestParam();
                    param.sliceStep = sliceStep;
                    param.serializer = serializer;
                    return param;
                }));
    }

    @DisplayName("Test kudo serialization round trip")
    @MethodSource("params")
    @ParameterizedTest
    void testSerializationRoundTripConcatHostSide(TestParam param) throws Exception {
        int sliceStep = param.sliceStep;
        int sliceCount = (int) ((table.getRowCount() + sliceStep - 1) / sliceStep);
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        for (int i = 0; i < table.getRowCount(); i += sliceStep) {
            int len = (int) Math.min(table.getRowCount() - i, sliceStep);
            param.serializer.writeToStream(table, bout, i, len);
        }
        bout.flush();
        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        List<Object> serializedBatched = IntStream.range(0, sliceCount)
                .mapToObj(idx -> param.serializer.readOneTableBuffer(bin))
                .collect(Collectors.toList());

        long numRows = 0;
        for (Object obj : serializedBatched) {
            assertThat(obj).isInstanceOf(SerializedTable.class);
            SerializedTable serializedBatch = (SerializedTable) obj;
            System.out.println("Found serialized batch header:" + serializedBatch.getHeader());
            numRows += serializedBatch.getHeader().getNumRows();
            assertThat(numRows).isLessThan(Integer.MAX_VALUE);
        }

        assertThat(numRows).isEqualTo(table.getRowCount());

        try (Table found = param.serializer.mergeTable(serializedBatched, schemaOf(table))) {
            System.out.println("Found table: " + found);
            for (int i = 0; i < found.getNumberOfColumns(); i++) {
                try (HostColumnVector hostCol = found.getColumn(i).copyToHost()) {
                    System.out.println("Col " + i);
                    if (hostCol.hasNulls()) {
                        hostCol.getValidity().printBuffer(10);
                    }
                    String row = LongStream.range(0, found.getRowCount())
                            .mapToInt(x -> (int) x)
                            .mapToObj(hostCol::getElement)
                            .map(Objects::toString)
                            .collect(Collectors.joining("|"));
                    System.out.println(row);
                }
            }
            assertTablesAreEqual(table, found);
        }
    }

    @Test
    void testSerializationRoundTripEmpty() throws IOException {
        try (ColumnVector emptyInt = ColumnVector.fromInts();
             ColumnVector emptyDouble = ColumnVector.fromDoubles();
             ColumnVector emptyString = ColumnVector.fromStrings();
             Table t = new Table(emptyInt, emptyInt, emptyDouble, emptyString)) {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            Kudo2Serializer serializer = new Kudo2Serializer(CompressionMode.BUFFER, 0, 0, 1);
            serializer.writeToStream(t, bout, 0, 0);
            bout.flush();
            assertThat(bout.toByteArray()).isEmpty();
        }
    }

    @Test
    void testSerializationZeroColumns() throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        Kudo2Serializer serializer = new Kudo2Serializer(CompressionMode.AUTO, 10, 10, 1);
        serializer.writeRowsToStream(bout, 10);
        bout.flush();
        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        Object obj = serializer.readOneTableBuffer(bin);
        assertThat(obj).isInstanceOf(SerializedTable.class);
        SerializedTable serializedBatch = (SerializedTable) obj;
        assertEquals(10, serializedBatch.getHeader().getNumRows());
    }

    private static Table buildTestTable() {
        return new Table.TestBuilder()
                .column(100, 202, 3003, 40004, 5, -60, 1, null, 3, null, 5, null, 7, null, 9, null, 11, null, 13, null, 15)
                .column(Boolean.TRUE , Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, Boolean.TRUE, null, Boolean.TRUE, Boolean.TRUE, null, Boolean.FALSE, Boolean.FALSE, null, Boolean.TRUE, Boolean.TRUE, null, Boolean.FALSE, Boolean.FALSE, null, Boolean.TRUE, Boolean.TRUE, null)
                .column((byte) 1, (byte) 2, null, (byte) 4, (byte) 5, (byte) 6, (byte) 1, (byte) 2, (byte) 3, null, (byte) 5, (byte) 6, (byte) 7, null, (byte) 9, (byte) 10, (byte) 11, null, (byte) 13, (byte) 14, (byte) 15)
                .column((short) 6, (short) 5, (short) 4, null, (short) 2, (short) 1, (short) 1, (short) 2, (short) 3, null, (short) 5, (short) 6, (short) 7, null, (short) 9, (short) 10, null, (short) 12, (short) 13, (short) 14, null)
                .column(1L, null, 1001L, 50L, -2000L, null, 1L, 2L, 3L, 4L, null, 6L, 7L, 8L, 9L, null, 11L, 12L, 13L, 14L, null)
                .column(10.1f, 20f, Float.NaN, 3.1415f, -60f, null, 1f, 2f, 3f, 4f, 5f, null, 7f, 8f, 9f, 10f, 11f, null, 13f, 14f, 15f)
                .column(10.1f, 20f, Float.NaN, 3.1415f, -60f, -50f, 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, 11f, 12f, 13f, 14f, 15f)
                .column(10.1, 20.0, 33.1, 3.1415, -60.5, null, 1., 2., 3., 4., 5., 6., null, 8., 9., 10., 11., 12., null, 14., 15.)
                .timestampDayColumn(99, 100, 101, 102, 103, 104, 1, 2, 3, 4, 5, 6, 7, null, 9, 10, 11, 12, 13, null, 15)
                .timestampMillisecondsColumn(9L, 1006L, 101L, 5092L, null, 88L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, null, 10L, 11L, 12L, 13L, 14L, 15L)
                .timestampSecondsColumn(1L, null, 3L, 4L, 5L, 6L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, null, 11L, 12L, 13L, 14L, 15L)
                .decimal32Column(-3, 100, 202, 3003, 40004, 5, -60, 1, null, 3, null, 5, null, 7, null, 9, null, 11, null, 13, null, 15)
                .decimal64Column(-8, 1L, null, 1001L, 50L, -2000L, null, 1L, 2L, 3L, 4L, null, 6L, 7L, 8L, 9L, null, 11L, 12L, 13L, 14L, null)
                .column("A", "B", "C", "D", null, "TESTING", "1", "2", "3", "4", "5", "6", "7", null, "9", "10", "11", "12", "13", null, "15")
                .column("A", "A", "C", "C", null, "TESTING", "1", "2", "3", "4", "5", "6", "7", null, "9", "10", "11", "12", "13", null, "15")
                .build();
    }
}
