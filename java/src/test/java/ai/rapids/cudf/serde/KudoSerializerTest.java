package ai.rapids.cudf.serde;

import ai.rapids.cudf.*;
import ai.rapids.cudf.serde.kudo.KudoSerializer;
import ai.rapids.cudf.serde.kudo.SerializedTable;
import ai.rapids.cudf.utils.Arms;
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
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static ai.rapids.cudf.TableTestUtils.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.*;

import static ai.rapids.cudf.AssertUtils.assertTablesAreEqual;

import static org.junit.jupiter.api.Assertions.*;

@Execution(ExecutionMode.SAME_THREAD)
public class KudoSerializerTest extends CudfTestBase {
    private static final int RANDOM_SEED = 1726735216;

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

    public static IntStream sliceSteps() {
        return IntStream.rangeClosed(1, (int) table.getRowCount());
//        return IntStream.of(2, (int) table.getRowCount());
    }

    @DisplayName("Test kudo serialization round trip")
    @ParameterizedTest(name = "sliceStep={0}")
    @MethodSource("sliceSteps")
    void testSerializationRoundTripConcatHostSide(int sliceStep) throws Exception {
        int sliceCount = (int) ((table.getRowCount() + sliceStep - 1) / sliceStep);
        KudoSerializer serializer = new KudoSerializer();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        for (int i = 0; i < table.getRowCount(); i += sliceStep) {
            int len = (int) Math.min(table.getRowCount() - i, sliceStep);
            serializer.writeToStream(table, bout, i, len);
        }
        bout.flush();
        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        List<Object> serializedBatches = IntStream.range(0, sliceCount)
                .mapToObj(idx -> serializer.readOneTableBuffer(bin))
                .collect(Collectors.toList());

        long numRows = 0;
        for (Object obj : serializedBatches) {
            assertThat(obj).isInstanceOf(SerializedTable.class);
            SerializedTable serializedBatch = (SerializedTable) obj;
            numRows += serializedBatch.getHeader().getNumRows();
            assertThat(numRows).isLessThan(Integer.MAX_VALUE);
        }

        assertThat(numRows).isEqualTo(table.getRowCount());

        try (ContiguousTable ct = serializer.mergeTable(serializedBatches, schemaOf(table))) {
            Table found = ct.getTable();
            for (int i = 0; i < found.getNumberOfColumns(); i++) {
                found.getColumn(i).getNullCount();
            }
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
        } finally {
            List<SerializedTable> serializedTables = serializedBatches.stream()
                    .map(o -> (SerializedTable) o)
                    .collect(Collectors.toList());
            Arms.closeQuietly(serializedTables);
        }
    }

    @Test
    void testSerializationRoundTripEmpty() throws IOException {
        try (ColumnVector emptyInt = ColumnVector.fromInts();
             ColumnVector emptyDouble = ColumnVector.fromDoubles();
             ColumnVector emptyString = ColumnVector.fromStrings();
             Table t = new Table(emptyInt, emptyInt, emptyDouble, emptyString)) {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            KudoSerializer serializer = new KudoSerializer();
            serializer.writeToStream(t, bout, 0, 0);
            bout.flush();
            assertThat(bout.toByteArray()).isEmpty();
        }
    }

    @Test
    void testSerializationZeroColumns() throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        KudoSerializer serializer = new KudoSerializer();
        serializer.writeRowsToStream(bout, 10);
        bout.flush();
        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        Object obj = serializer.readOneTableBuffer(bin);
        assertThat(obj).isInstanceOf(SerializedTable.class);
        SerializedTable serializedBatch = (SerializedTable) obj;
        assertEquals(10, serializedBatch.getHeader().getNumRows());
    }

    @Test
    void testMergeTableWithDifferentValidity() throws Exception {
        Table[] tables = new Table[4];

        try (CloseableArray<Table> ignored = CloseableArray.wrap(tables)) {
            tables[0] = new Table.TestBuilder()
                    .column(-83182L, 5822L, 3389L, 7384L, 7297L)
                    .column(-2.06,-2.14, 8.04, 1.16, -1.0)
                    .build();

            tables[1] = new Table.TestBuilder()
                    .column(-47L, null, -83L, -166L, -220L, 470L, 619L, 803L, 661L)
                    .column(-6.08, 1.6, 1.78, -8.01, 1.22, 1.43, 2.13, -1.65, null)
                    .build();

            tables[2] = new Table.TestBuilder()
                    .column(8722L, 8733L)
                    .column(2.51, 0.0)
                    .build();


            tables[3] = new Table.TestBuilder()
                    .column(7384L, 7297L, 803L, 661L, 8733L)
                    .column(1.16, -1.0, -1.65, null, 0.0)
                    .build();

            checkMergeTable(tables[3], asList(
                    SlicedTable.from(tables[0], 3, 2),
                    SlicedTable.from(tables[1], 7, 2),
                    SlicedTable.from(tables[2], 1, 1)));
        }
    }

    @Test
    void testMergeList() throws Exception {
        Table[] tables = new Table[5];

        try (CloseableArray<Table> ignored = CloseableArray.wrap(tables)) {
            tables[0] = new Table.TestBuilder()
                    .column(-881L, 482L, 660L, 896L, -129L, -108L, -428L, 0L, 617L, 782L)
                    .column(integers(665), integers(-267), integers(398), integers(-314),
                            integers(-370), integers(181), integers(665, 544), integers(222), integers(-587), integers(544))
                    .build();

            tables[1] = new Table.TestBuilder()
                    .column(-881L, 482L, 660L, 896L, 122L, 241L, 281L, 680L, 783L, null)
                    .column(integers(-370), integers(398), integers(-587, 398), integers(-314),
                            integers(307), integers(-397, -633), integers(-314, 307), integers(-633), integers(-397), integers(181, -919, -175))
                    .build();


            tables[3] = new Table.TestBuilder()
                    .column(896L, -129L, -108L, -428L, 0L, 617L, 782L, 482L, 660L, 896L, 122L, 241L, 281L, 680L, 783L, null)
                    .column(integers(-314), integers(-370), integers(181), integers(665, 544), integers(222), integers(-587), integers(544),
                            integers(398), integers(-587, 398), integers(-314),
                            integers(307), integers(-397, -633), integers(-314, 307), integers(-633), integers(-397), integers(181, -919, -175))
                    .build();

            checkMergeTable(tables[3], asList(
                    SlicedTable.from(tables[0], 3, 7),
                    SlicedTable.from(tables[1], 1, 9)));
        }
    }

    @Test
    void testSerializeValidity() throws Exception {
        Table[] tables = new Table[2];

        try (CloseableArray<Table> ignored = CloseableArray.wrap(tables)) {
            Integer[] values = new Integer[512];
            for (int i = 0; i < values.length; i++) {
                values[i] = i;
            }
            values[1] = null;
            values[2] = null;
            tables[0] = new Table.TestBuilder()
                    .column(values)
                    .build();

            tables[1] = new Table.TestBuilder()
                    .column(509, 510, 511)
                    .build();


            checkMergeTable(tables[1], singletonList(
                    SlicedTable.from(tables[0], 509, 3)));
        }
    }

    private static void checkMergeTable(Table expectedTable, List<SlicedTable> slicedTables) throws Exception {
        KudoSerializer serializer = new KudoSerializer();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        for (SlicedTable t : slicedTables) {
            serializer.writeToStream(t.getTable(), bout, t.getStartRow(), (int) t.getNumRows());
        }

        bout.flush();

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        List<Object> serializedBatches = IntStream.range(0, slicedTables.size())
                .mapToObj(idx -> serializer.readOneTableBuffer(bin))
                .collect(Collectors.toList());

        long numRows = 0;
        for (Object obj : serializedBatches) {
            assertThat(obj).isInstanceOf(SerializedTable.class);
            SerializedTable serializedBatch = (SerializedTable) obj;
            numRows += serializedBatch.getHeader().getNumRows();
            assertThat(numRows).isLessThan(Integer.MAX_VALUE);
        }

        assertThat(numRows).isEqualTo(expectedTable.getRowCount());

        try (ContiguousTable ct = serializer.mergeTable(serializedBatches, schemaOf(expectedTable))) {
            Table found = ct.getTable();
            for (int i = 0; i < found.getNumberOfColumns(); i++) {
                found.getColumn(i).getNullCount();
            }
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
            assertTablesAreEqual(expectedTable, found);
        } finally {
            List<SerializedTable> serializedTables = serializedBatches.stream()
                    .map(o -> (SerializedTable) o)
                    .collect(Collectors.toList());
            Arms.closeQuietly(serializedTables);
        }
    }

    private static Table buildTestTable() {
        HostColumnVector.ListType listMapType = new HostColumnVector.ListType(true,
                new HostColumnVector.ListType(true, new HostColumnVector.StructType(true,
                        new HostColumnVector.BasicType(false, DType.STRING),
                        new HostColumnVector.BasicType(true, DType.STRING))));
        HostColumnVector.StructType mapStructType = new HostColumnVector.StructType(true,
                new HostColumnVector.BasicType(false, DType.STRING),
                new HostColumnVector.BasicType(false, DType.STRING));
        HostColumnVector.StructType structType = new HostColumnVector.StructType(true,
                new HostColumnVector.BasicType(true, DType.INT32),
                new HostColumnVector.BasicType(false, DType.FLOAT32));
        HostColumnVector.ListType listDateType = new HostColumnVector.ListType(true,
                new HostColumnVector.StructType(false,
                        new HostColumnVector.BasicType(false, DType.INT32),
                        new HostColumnVector.BasicType(true, DType.INT32)));
        return new Table.TestBuilder()
                .column(100, 202, 3003, 40004, 5, -60, 1, null, 3, null, 5, null, 7, null, 9, null, 11, null, 13, null, 15)
                .column(true, true, false, false, true, null, true, true, null, false, false, null, true, true, null, false, false, null, true, true, null)
                .column((byte) 1, (byte) 2, null, (byte) 4, (byte) 5, (byte) 6, (byte) 1, (byte) 2, (byte) 3, null, (byte) 5, (byte) 6, (byte) 7, null, (byte) 9, (byte) 10, (byte) 11, null, (byte) 13, (byte) 14, (byte) 15)
                .column((short) 6, (short) 5, (short) 4, null, (short) 2, (short) 1, (short) 1, (short) 2, (short) 3, null, (short) 5, (short) 6, (short) 7, null, (short) 9, (short) 10, null, (short) 12, (short) 13, (short) 14, null)
                .column(1L, null, 1001L, 50L, -2000L, null, 1L, 2L, 3L, 4L, null, 6L, 7L, 8L, 9L, null, 11L, 12L, 13L, 14L, null)
                .column(10.1f, 20f, Float.NaN, 3.1415f, -60f, null, 1f, 2f, 3f, 4f, 5f, null, 7f, 8f, 9f, 10f, 11f, null, 13f, 14f, 15f)
                .column(10.1f, 20f, Float.NaN, 3.1415f, -60f, -50f, 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, 11f, 12f, 13f, 14f, 15f)
                .column(10.1, 20.0, 33.1, 3.1415, -60.5, null, 1., 2., 3., 4., 5., 6., null, 8., 9., 10., 11., 12., null, 14., 15.)
                .column(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, (Float) null)
                .timestampDayColumn(99, 100, 101, 102, 103, 104, 1, 2, 3, 4, 5, 6, 7, null, 9, 10, 11, 12, 13, null, 15)
                .timestampMillisecondsColumn(9L, 1006L, 101L, 5092L, null, 88L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, null, 10L, 11L, 12L, 13L, 14L, 15L)
                .timestampSecondsColumn(1L, null, 3L, 4L, 5L, 6L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, null, 11L, 12L, 13L, 14L, 15L)
                .decimal32Column(-3, 100, 202, 3003, 40004, 5, -60, 1, null, 3, null, 5, null, 7, null, 9, null, 11, null, 13, null, 15)
                .decimal64Column(-8, 1L, null, 1001L, 50L, -2000L, null, 1L, 2L, 3L, 4L, null, 6L, 7L, 8L, 9L, null, 11L, 12L, 13L, 14L, null)
                .column("A", "B", "C", "D", null, "TESTING", "1", "2", "3", "4", "5", "6", "7", null, "9", "10", "11", "12", "13", null, "15")
                .column("A", "A", "C", "C", "E", "TESTING", "1", "2", "3", "4", "5", "6", "7", "", "9", "10", "11", "12", "13", "", "15")
                .column("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
                .column("", null, "", "", null, "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
                .column(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, (String) null)
                .column(mapStructType,
                        structs(struct("1", "2")), structs(struct("3", "4")),
                        null, null,
                        structs(struct("key", "value"), struct("a", "b")), null,
                        null, structs(struct("3", "4"), struct("1", "2")),
                        structs(), structs(null, struct("foo", "bar")),
                        structs(null, null, null), null,
                        null, null,
                        null, null,
                        null, null,
                        null, null,
                        structs(struct("the", "end")))
                .column(structType,
                        struct(1, 1f), null, struct(2, 3f), null, struct(8, 7f),
                        struct(0, 0f), null, null, struct(-1, -1f), struct(-100, -100f),
                        struct(Integer.MAX_VALUE, Float.MAX_VALUE), null, null, null, null,
                        null, null, null, null, null,
                        struct(Integer.MIN_VALUE, Float.MIN_VALUE))
                .column(integers(1, 2), null, integers(3, 4, null, 5, null), null, null, integers(6, 7, 8),
                        integers(null, null, null), integers(1, 2, 3), integers(4, 5, 6), integers(7, 8, 9),
                        integers(10, 11, 12), integers((Integer) null), integers(14, null), integers(14, 15, null, 16, 17, 18),
                        integers(19, 20, 21), integers(22, 23, 24), integers(25, 26, 27), integers(28, 29, 30), integers(31, 32, 33),
                        null, integers(37, 38, 39))
                .column(integers(), integers(), integers(), integers(), integers(), integers(), integers(), integers(), integers(),
                        integers(), integers(), integers(), integers(), integers(), integers(), integers(), integers(), integers(),
                        integers(), integers(), integers())
                .column(integers(null, null), integers(null, null, null, null), integers(),
                        integers(null, null, null), integers(), integers(null, null, null, null, null),
                        integers((Integer) null), integers(null, null, null), integers(null, null),
                        integers(null, null, null, null), integers(null, null, null, null, null), integers(),
                        integers(null, null, null, null), integers(null, null, null), integers(null, null),
                        integers(null, null, null), integers(null, null), integers((Integer) null),
                        integers((Integer) null), integers(null, null), integers(null, null, null, null, null))
                .column(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, (Integer[]) null)
                .column(
                        strings("1", "2", "3"), strings("4"), strings("5"), strings("6, 7"),
                        strings("", "9", null), strings("11"), strings(""), strings(null, null),
                        strings("15", null), null, null, strings("18", "19", "20"),
                        null, strings("22"), strings("23", ""), null,
                        null, null, null, strings(),
                        strings("the end"))
                .column(
                        strings(), strings(), strings(), strings(), strings(), strings(), strings(), strings(),
                        strings(), strings(), strings(), strings(), strings(), strings(), strings(), strings(),
                        strings(), strings(), strings(), strings(), strings())
                .column(strings(null, null), strings(null, null, null, null), strings(),
                        strings(null, null, null), strings(), strings(null, null, null, null, null),
                        strings((String) null), strings(null, null, null), strings(null, null),
                        strings(null, null, null, null), strings(null, null, null, null, null), strings(),
                        strings(null, null, null, null), strings(null, null, null), strings(null, null),
                        strings(null, null, null), strings(null, null), strings((String) null),
                        strings((String) null), strings(null, null), strings(null, null, null, null, null))
                .column(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, (String[]) null)
                .column(listMapType,
                        asList(asList(struct("k1", "v1"), struct("k2", "v2")), singletonList(struct("k3", "v3"))),
                        asList(asList(struct("k4", "v4"), struct("k5", "v5"), struct("k6", "v6")), singletonList(struct("k7", "v7"))),
                        null, null, null,
                        asList(asList(struct("k8", "v8"), struct("k9", "v9")), asList(struct("k10", "v10"), struct("k11", "v11"), struct("k12", "v12"), struct("k13", "v13"))),
                        singletonList(asList(struct("k14", "v14"), struct("k15", "v15"))),
                        null, null, null, null,
                        asList(asList(struct("k16", "v16"), struct("k17", "v17")), singletonList(struct("k18", "v18"))),
                        asList(asList(struct("k19", "v19"), struct("k20", "v20")), singletonList(struct("k21", "v21"))),
                        asList(singletonList(struct("k22", "v22")), singletonList(struct("k23", "v23"))),
                        asList(null, null, null),
                        asList(singletonList(struct("k22", null)), singletonList(struct("k23", null))),
                        null, null, null, null, null)
                .column(listDateType,
                        asList(struct(-210, 293), struct(-719, 205), struct(-509, 183), struct(174, 122), struct(647, 683)),
                        asList(struct(311, 992), struct(-169, 482), struct(166, 525)),
                        asList(struct(156, 197), struct(926, 134), struct(747, 312), struct(293, 801)),
                        asList(struct(647, null), struct(293, 387)),
                        emptyList(),
                        null,
                        emptyList(),
                        null,
                        asList(struct(-210, 293), struct(-719, 205), struct(-509, 183), struct(174, 122), struct(647, 683)),
                        asList(struct(311, 992), struct(-169, 482), struct(166, 525)),
                        asList(struct(156, 197), struct(926, 134), struct(747, 312), struct(293, 801)),
                        asList(struct(647, null), struct(293, 387)),
                        emptyList(),
                        null,
                        emptyList(),
                        null,
                        singletonList(struct(778, 765)),
                        asList(struct(7, 87), struct(8, 96)),
                        asList(struct(9, 56), struct(10, 532), struct(11, 456)),
                        null,
                        emptyList())
                .build();
    }

}
