package io.pravega.alink;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.regression.LinearRegression;


public class test_readFromCsv {
    public static void main(String[] args) throws Exception {

        String csvSchema = "time Timestamp ,PDP2GGERDJKS01000000_2 Double, PDP2GHDEDJKS01000017_2 Double";
        CsvSourceBatchOp csvSource = new CsvSourceBatchOp().setFilePath("C:\\Sandbox\\initial_data_result_nozero_alink_test.csv").setIgnoreFirstLine(true).setSchemaStr(csvSchema);

        System.out.println("Orignal Table Schema........");
        System.out.print( csvSource.getSchema());
        csvSource.print();


        BatchOperator csvdata = csvSource.link(new AppendIdBatchOp());
        System.out.println("Table Schema after added AppendId........");
        csvdata.print();
        System.out.print( csvSource.getSchema());

        LinearRegression lrop = new LinearRegression()
                .setLabelCol("PDP2GGERDJKS01000000_2")
                .setFeatureCols(new String[]{"append_id"})
                .setPredictionCol("pre");

        lrop.fit(csvdata).transform(csvdata).print();
    }

}

