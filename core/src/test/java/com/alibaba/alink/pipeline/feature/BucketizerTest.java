package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.params.feature.BucketizerParams;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import org.apache.flink.util.Preconditions;
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
    private static double[][] cutsArray = new double[][]{{-0.5, 0.0, 0.5}, {-0.3, 0.0, 0.3, 0.4}};

    @Test
    public void testBucketizer() throws Exception{
        Bucketizer op = new Bucketizer()
            .setSelectedCols(new String[]{"features1", "features2"})
            .setOutputCols(new String[]{"bucket1", "bucket2"})
            .setCutsArray(cutsArray);

        Table res = op.transform(data);

        List<Long> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select("bucket1"), Long.class).collect();
        Assert.assertArrayEquals(list.toArray(new Long[0]), new Long[]{0L, 0L, 1L, 1L, 2L, 3L});

        res = op.transform(dataStream);

        DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,res).print();

        MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
    }

    @Test
    public void testBucketizerParam(){
        Bucketizer op = new Bucketizer().setCutsArray(new double[]{-0.5, 0.0, 0.5, -0.3, 0.0, 0.3, 0.4}, new int[]{3, 4});
        double[][] newCutsArray = op.getParams().get(BucketizerParams.CUTS_ARRAY);
        Assert.assertEquals(cutsArray.length, newCutsArray.length);
        for(int i = 0; i < cutsArray.length; i++){
            Assert.assertEquals(cutsArray[i].length, newCutsArray[i].length);
            for(int j = 0; j < cutsArray[i].length; j++){
                Assert.assertEquals(cutsArray[i][j], newCutsArray[i][j], 0.01);
            }
        }
    }
}
