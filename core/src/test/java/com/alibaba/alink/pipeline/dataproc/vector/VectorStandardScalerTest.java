package com.alibaba.alink.pipeline.dataproc.vector;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.TestUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.Table;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class VectorStandardScalerTest {


    public static void testPipeline(boolean withMean, boolean withStdv) throws Exception {
        Row[] rowData =
            new Row[]{
                Row.of("0", "1.0 2.0"),
                Row.of("1", "-1.0 -3.0"),
                Row.of("2", "4.0 2.0"),
            };
        TableSchema schema = new TableSchema(
            new String[] {"id", "vec"},
            new TypeInformation<?>[] {Types.STRING, Types.STRING}
        );
        BatchOperator batchData = new MemSourceBatchOp(Arrays.asList(rowData), schema);
        StreamOperator streamData = new MemSourceStreamOp(Arrays.asList(rowData), schema);
        String selectedColName = "vec";

        VectorStandardScaler scaler = new VectorStandardScaler()
            .setSelectedCol(selectedColName)
            .setWithMean(withMean)
            .setWithStd(withStdv);

        VectorStandardScalerModel denseModel = scaler.fit(batchData);
        BatchOperator res = denseModel.transform(batchData);
        List rows = res.getDataSet().collect();
        HashMap<String, Vector> map = new HashMap<String, Vector>();
        map.put((String) ((Row) rows.get(0)).getField(0), VectorUtil.getVector(((Row) rows.get(0)).getField(1)));
        map.put((String) ((Row) rows.get(1)).getField(0), VectorUtil.getVector(((Row) rows.get(1)).getField(1)));
        map.put((String) ((Row) rows.get(2)).getField(0), VectorUtil.getVector(((Row) rows.get(2)).getField(1)));
        assertEquals(map.get("0"),
            VectorUtil.getVector("-0.13245323570650433 0.5773502691896257"));
        assertEquals(map.get("1"),
            VectorUtil.getVector("-0.9271726499455304 -1.1547005383792515"));
        assertEquals(map.get("2"),
            VectorUtil.getVector("1.059625885652035 0.5773502691896257"));


        denseModel.transform(streamData).print();
        StreamOperator.execute();
    }

    @Test
    public void testPipeline1() throws Exception {
        testPipeline(true, true);
    }
}