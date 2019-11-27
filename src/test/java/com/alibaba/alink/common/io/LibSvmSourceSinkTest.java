package com.alibaba.alink.common.io;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.LibSvmSinkBatchOp;
import com.alibaba.alink.operator.batch.source.LibSvmSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class LibSvmSourceSinkTest {
    private String path;

    @Before
    public void setup() {
        path = "/tmp/" + FileUtils.getRandomFilename("libsvm_test") + "/";
        if (!new File(path).mkdirs()) {
            throw new RuntimeException("Fail to create dir: " + path);
        }
    }

    @Test
    public void testLibSvmBatchSource() throws Exception {
        String data = "-1 3:1 11:1 14:1 19:1 39:1 42:1 55:1 64:1 67:1 73:1 75:1 76:1 80:1 83:1\n" +
            "-1 3:1 6:1 17:1 27:1 35:1 40:1 57:1 63:1 69:1 73:1 74:1 76:1 81:1 103:1\n" +
            "-1 4:1 6:1 15:1 21:1 35:1 40:1 57:1 63:1 67:1 73:1 74:1 77:1 80:1 83:1\n" +
            "-1 5:1 6:1 15:1 22:1 36:1 41:1 47:1 66:1 67:1 72:1 74:1 76:1 80:1 83:1";
        String fn = path + "libsvm2.txt";
        Path path = Paths.get(fn);
        byte[] strToBytes = data.getBytes();
        Files.write(path, strToBytes);

        Assert.assertEquals(new LibSvmSourceBatchOp(fn).count(), 4);
    }

    @Test
    public void testLibSvmBatchSink() throws Exception {
        Row[] rows = new Row[]{
            Row.of(1, "0:1 1:1"),
            Row.of(-1, "1:1 3:1"),
        };

        String fn = path + "libsvm1.txt";
        MemSourceBatchOp source = new MemSourceBatchOp(rows, new String[]{"label", "features"});

        new LibSvmSinkBatchOp().setFilePath(fn)
            .setLabelCol("label").setVectorCol("features").setOverwriteSink(true).linkFrom(source);

        BatchOperator.execute();

        List<String> lines = Files.readAllLines(Paths.get(fn));
        Assert.assertEquals(lines.size(), 2);
    }
}