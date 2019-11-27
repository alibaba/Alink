package examples;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.pipeline.TestUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.pipeline.dataproc.vector.VectorImputer;
import com.alibaba.alink.pipeline.dataproc.vector.VectorImputerModel;
import org.junit.Test;

import java.util.Arrays;

public class VectorImputerExample {

    @Test
    public void testPipelineMean2() throws Exception {
        Row[] testArray =
            new Row[]{
                Row.of(new Object[]{"1.0 NaN"}),
                Row.of(new Object[]{"-1.0 -3.0"}),
                Row.of(new Object[]{"4.0 2.0"})
            };

        String selectedColName = "vec";
        String[] colNames = new String[]{selectedColName};
        String strategy = "value";
        String fillValue = "0.3";

        Table batchData = MLEnvironmentFactory.getDefault().createBatchTable(Arrays.asList(testArray), colNames);
        Table streamData = MLEnvironmentFactory.getDefault().createStreamTable(Arrays.asList(testArray), colNames);

        VectorImputer fillMissingValue = new VectorImputer()
                .setSelectedCol(selectedColName)
                .setStrategy(strategy)
                .setFillValue(fillValue);

        VectorImputerModel model = fillMissingValue.fit(batchData);
        TestUtil.printTable(model.getModelData());
        TestUtil.printTable(model.transform(batchData));
        TestUtil.printTable(model.transform(streamData));
    }
}
