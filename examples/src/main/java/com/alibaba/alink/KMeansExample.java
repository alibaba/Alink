package com.alibaba.alink;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.clustering.KMeans;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;

/**
 * Example for KMeans.
 */
public class KMeansExample {

    public static void main(String[] args) throws Exception {
        String URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv";
        String SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";

        BatchOperator data = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);

        VectorAssembler va = new VectorAssembler()
            .setSelectedCols(new String[]{"sepal_length", "sepal_width", "petal_length", "petal_width"})
            .setOutputCol("features");

        KMeans kMeans = new KMeans().setVectorCol("features").setK(3)
            .setPredictionCol("prediction_result")
            .setPredictionDetailCol("prediction_detail")
            .setReservedCols("category")
            .setMaxIter(100);

        Pipeline pipeline = new Pipeline().add(va).add(kMeans);
        pipeline.fit(data).transform(data).print();
    }
}
