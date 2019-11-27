package com.alibaba.alink.pipeline.feature;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for Bucketizer.
 */
public class BucketizerTest {

    private Row[] rows = new Row[] {
        Row.of(-999.9, -999.9),
        Row.of(-0.5, -0.2),
        Row.of(-0.3, -0.1),
        Row.of(0.0, 0.0),
        Row.of(0.2, 0.4),
        Row.of(999.9, 999.9)
    };
    private Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[]{"features1", "features2"});
    private Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[]{"features1", "features2"});
    private String[] splitsArray = new String[] {"-Infinity: -0.5: 0.0: 0.5: Infinity",
        "-Infinity: -0.3: 0.0: 0.3: 0.4: Infinity"};

    @Test
    public void testBucketizer() throws Exception{
        Bucketizer op = new Bucketizer()
            .setSelectedCols(new String[]{"features1", "features2"})
            .setOutputCols(new String[]{"bucket1", "bucket2"})
            .setSplitsArray(splitsArray);

        Table res = op.transform(data);

        List<Integer> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select("bucket1"), Integer.class).collect();
        Assert.assertArrayEquals(list.toArray(new Integer[0]), new Integer[]{0, 1, 1, 2, 2, 3});

        res = op.transform(dataStream);

        DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,res).print();

        MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
    }
}
