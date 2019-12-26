package com.alibaba.alink.common.io;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test cases for {@link CsvSourceBatchOp}, {@link CsvSinkBatchOp},
 * {@link CsvSourceStreamOp} and {@link CsvSinkStreamOp}.
 */
public class CsvSourceSinkTest {
    private String path;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Row[] rows;
    private Map<String, String> actual;

    @Before
    public void setup() {
        path = "/tmp/" + FileUtils.getRandomFilename("csv_test") + "/";
        if (!new File(path).mkdirs()) {
            throw new RuntimeException("Fail to create dir: " + path);
        }

        rows = new Row[]{
            Row.of(1L, "hello, world"),
            Row.of(2L, "hello world"),
            Row.of(3L, "hello \"world\""),
            Row.of(4L, null),
            Row.of(5L, ""),
            Row.of(6L, "\"\"")
        };

        actual = new HashMap<>();
        actual.put("1", "1,\"hello, world\"");
        actual.put("2", "2,hello world");
        actual.put("3", "3,\"hello \"\"world\"\"\"");
        actual.put("4", "4,");
        actual.put("5", "5,\"\"");
        actual.put("6", "6,\"\"\"\"\"\"");
    }

    @After
    public void clear() throws Exception {
        FileUtils.deleteFileOrDirectory(new File(path));
    }

    @Test
    public void testBatchCsvSinkAndSource() throws Exception {
        String filePath = path + "file1.csv";
        String[] columnNames = new String[]{"id", "content"};
        BatchOperator data = new MemSourceBatchOp(rows, columnNames);
        CsvSinkBatchOp sink = new CsvSinkBatchOp()
            .setFilePath(filePath)
            .setOverwriteSink(true);
        data.link(sink);
        BatchOperator.execute();

        List<String> lines = Files.readAllLines(Paths.get(filePath));
        lines.forEach(line -> {
            int pos = line.indexOf(',');
            String key = line.substring(0, pos);
            Assert.assertEquals(line, actual.get(key));
        });

        BatchOperator source = new CsvSourceBatchOp()
            .setFilePath(filePath)
            .setSchemaStr("id bigint, content string");

        List<Row> result = source.collect();
        Assert.assertEquals(result.size(), 6);

        for (Row row : result) {
            boolean found = false;
            for (Row ref : rows) {
                if (row.getField(0).equals(ref.getField(0))) {
                    Assert.assertEquals(row.getField(1), ref.getField(1));
                    found = true;
                    break;
                }
            }
            Assert.assertTrue(found);
        }
    }

    @Test
    public void testStreamCsvSinkAndSource() throws Exception {
        String filePath = path + "file2.csv";
        String[] columnNames = new String[]{"id", "content"};
        StreamOperator data = new MemSourceStreamOp(rows, columnNames);
        CsvSinkStreamOp sink = new CsvSinkStreamOp()
            .setFilePath(filePath)
            .setOverwriteSink(true);
        data.link(sink);
        StreamOperator.execute();

        List<String> lines = Files.readAllLines(Paths.get(filePath));
        lines.forEach(line -> {
            int pos = line.indexOf(',');
            String key = line.substring(0, pos);
            Assert.assertEquals(line, actual.get(key));
        });

        StreamOperator source = new CsvSourceStreamOp()
            .setFilePath(filePath)
            .setSchemaStr("id bigint, content string");

        source.print();
        StreamOperator.execute();
    }

    @Test
    public void testInvalidField() throws Exception {
        String data = "11,a12,13\n" +
            "21,,23\n" +
            "31,32,33";

        String filePath = path + "file3.csv";
        Files.write(Paths.get(filePath), data.getBytes(), StandardOpenOption.CREATE);

        thrown.expect(RuntimeException.class);
        new CsvSourceBatchOp().setFilePath(filePath).setSchemaStr("f1 bigint, f2 bigint, f3 bigint").collect();
    }

    @Test
    public void testSkipBlankLine() throws Exception {
        String data = "\n" +
            "f\n" +
            "\n";

        String filePath = path + "file4.csv";
        Files.write(Paths.get(filePath), data.getBytes(), StandardOpenOption.CREATE);

        Assert.assertEquals(new CsvSourceBatchOp().setFilePath(filePath).setSchemaStr("f1 string").collect().size(), 1);
    }
}