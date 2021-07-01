package com.alibaba.alink.operator.batch.prophet;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.prophet.ProphetTrainBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;

public class ProphetBatchTest extends AlinkTestBase {
    @Test
    public void testProphetBatchTrain() throws Exception {
        Row[] testArray =
                new Row[] {
                        Row.of("2007-12-10", 9.59076113897809),
                        Row.of("2007-12-11",8.51959031601596),
                        Row.of("2007-12-12",8.18367658262066),
                        Row.of("2007-12-13",8.07246736935477),
                        Row.of("2007-12-14",7.8935720735049),
                        Row.of("2007-12-15",7.78364059622125),
                        Row.of("2007-12-16",8.41405243249672),
                        Row.of("2007-12-17",8.82922635473185),
                        Row.of("2007-12-18",8.38251828808963),
                        Row.of("2007-12-19",8.06965530688617)
                };

        String[] colnames = new String[] {"ds", "y"};
        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colnames);

        BatchOperator sink = new CsvSinkBatchOp()
                .setOverwriteSink(true)
                .setFieldDelimiter(" ")
                .setNumFiles(1)
                .setFilePath(new FilePath("/tmp/prophet_info"));

        ProphetTrainBatchOp ProphetTrainBatchOp = new ProphetTrainBatchOp()
                .setSelectedCols("ds", "y");

        ProphetTrainBatchOp.linkFrom(source).link(sink);
        BatchOperator.execute();
    }
}
