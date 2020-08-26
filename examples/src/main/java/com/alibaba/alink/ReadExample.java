package com.alibaba.alink;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.nlp.DocCountVectorizer;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.List;

public class ReadExample {
    private Row[] rows = new Row[] {
            Row.of(0, "That book is an English book", 1),
            Row.of(1, "Have a good day", 1)
    };
    private Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence", "label"});
    private Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"id", "sentence", "label"});
    public void test() throws Exception {
        DocCountVectorizer op = new DocCountVectorizer()
                .setSelectedCol("sentence")
                .setOutputCol("features")
                .setFeatureType("TF");

        PipelineModel model = new Pipeline().add(op).fit(data);

        Table res = model.transform(data);

        List<SparseVector> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select("features"), SparseVector.class).collect();
        System.out.println(list.toString());
    }

    public static void main(String[] args) throws Exception {
        ReadExample readExample = new ReadExample();

        String url = "/Users/cynsier/movie.csv";
        //url = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/movielens_ratings.csv";
        String schema = "userid bigint, movieid bigint, rating double, timestamp string";

        //CsvSourceBatchOp data =  new CsvSourceBatchOp().setFilePath(url).setSchemaStr(schema);
        //data.firstN(10).print();
        readExample.test();
    }
}
