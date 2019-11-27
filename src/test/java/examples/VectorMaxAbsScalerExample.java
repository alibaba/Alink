package examples;

import com.alibaba.alink.pipeline.TestUtil;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.pipeline.dataproc.vector.VectorMaxAbsScaler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorMaxAbsScalerModel;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import org.junit.Test;

public class VectorMaxAbsScalerExample {
    private static void testPipelineI() throws Exception {

        ///////////////// dense vector /////////////
        Table vectorSource = GenerateData.getDenseBatch();
        Table vectorSSource = GenerateData.getDenseStream();

        String selectedColName = "vec";

        VectorMaxAbsScaler scaler = new VectorMaxAbsScaler()
            .setSelectedCol(selectedColName);

        VectorMaxAbsScalerModel denseModel = scaler.fit(vectorSource);
        TestUtil.printTable(denseModel.getModelData());
        TestUtil.printTable(denseModel.transform(vectorSource));
        TestUtil.printTable(denseModel.transform(vectorSSource));

        ///////////////// sparse vector /////////////
        Table sparseSource = GenerateData.getSparseBatch();
        Table sSparseSource = GenerateData.getSparseStream();

        VectorMaxAbsScaler scaler2 = new VectorMaxAbsScaler()
            .setSelectedCol(selectedColName);

        VectorMaxAbsScalerModel denseModel2 = scaler2.fit(sparseSource);
        TestUtil.printTable(denseModel2.transform(sparseSource));
        TestUtil.printTable(denseModel2.transform(sSparseSource));

    }

    @Test
    public void testPipeline() throws Exception {
        testPipelineI();
    }
}
