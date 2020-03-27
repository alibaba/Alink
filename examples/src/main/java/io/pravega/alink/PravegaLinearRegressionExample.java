package io.pravega.alink;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;
import com.alibaba.alink.operator.batch.dataproc.JsonValueBatchOp;
import com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.batch.sink.PravegaSinkBatchOp;
import com.alibaba.alink.operator.batch.source.PravegaSourceBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp;
import com.alibaba.alink.pipeline.regression.LinearRegression;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.PravegaSourceStreamOp;
import com.alibaba.alink.pipeline.regression.LinearRegressionModel;
import io.pravega.alink.common.CommonParams;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class PravegaLinearRegressionExample {

    private static final Logger LOG = LoggerFactory.getLogger(PravegaLinearRegressionExample.class);
    public final String scope;
    public final String streamName;
    public final String controllerURI;


    public PravegaLinearRegressionExample(String scope, String streamName, String controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
    }

    public PravegaLinearRegressionExample PravegaStreamML(String scope, String streamName, String controllerUri) throws Exception {
        PravegaSourceStreamOp source =
                new PravegaSourceStreamOp()
                        .setPravegaControllerUri(controllerUri)
                        .setPravegaScope(scope)
                        .setPravegaStream(streamName);


        StreamOperator data = source
                .link (
                        new JsonValueStreamOp()
                                .setSelectedCol("event").setReservedCols(new String[] {})
                                .setOutputCols(
                                        new String[] {"DateTime", "PDP2GGERDJKS01000000_2", "PDP2GHDEDJKS01000017_2"}
                                )
                                .setJsonPath(new String[] {"$.DateTime", "$.PDP2GGERDJKS01000000_2", "$.PDP2GHDEDJKS01000017_2"})
                )
                .select("CAST(DateTime AS TIMESTAMP) AS DateTime" +
                        "CAST(PDP2GGERDJKS01000000_2 AS FLOAT) AS PDP2GGERDJKS01000000_2" +
                        "CAST(PDP2GHDEDJKS01000017_2 AS FLOAT) AS PDP2GHDEDJKS01000017_2"
                );

        //LinearRegPredictStreamOp LR = new LinearRegPredictStreamOp(data )
        System.out.print(data.getSchema());
        data.print();
        StreamOperator.execute();
        return this;
    }


    public PravegaLinearRegressionExample PravegaBatchmML(String scope, String streamName, String controllerUri) throws Exception {

        String labelColName = "PDP2GGERDJKS01000000_2";
        String[] selectedColNames = new String[] {"append_id"};


        //Pravega Source Batch Op
        System.out.println("Starting to read the dataset from Pravega........");
        PravegaSourceBatchOp source =
                new PravegaSourceBatchOp()
                        .setPravegaControllerUri(controllerUri)
                        .setPravegaScope(scope)
                        .setPravegaStream(streamName);


        BatchOperator data = source.link (
                new JsonValueBatchOp()
                        .setSelectedCol("event")
                        .setReservedCols(new String[] {})
                        .setOutputCols(
                                new String[] {"DateTime", "PDP2GGERDJKS01000000_2", "PDP2GHDEDJKS01000017_2"}
                        )
                        .setJsonPath(new String[] {"$.DateTime", "$.PDP2GGERDJKS01000000_2", "$.PDP2GHDEDJKS01000017_2"})
                )
                .select("CAST(DateTime AS TIMESTAMP) AS DateTime, " +
                "CAST(PDP2GGERDJKS01000000_2 AS FLOAT) AS PDP2GGERDJKS01000000_2, " +
                "CAST(PDP2GHDEDJKS01000017_2 AS FLOAT) AS PDP2GHDEDJKS01000017_2");

        System.out.print("Orignal Table Schema" + data.getSchema());
        data.print();


        BatchOperator pravegadata = data.link(new AppendIdBatchOp());

        System.out.print("pravegadata Table Schema" + pravegadata.getSchema());
        pravegadata.print();

        //Single Linear Regression Example
        System.out.println("Starting to run the Linear Regression on the original dataset........");
        LinearRegression lrop = new LinearRegression()
                .setLabelCol(labelColName)
                .setFeatureCols(selectedColNames)
                .setPredictionCol("pre");

        LinearRegTrainBatchOp LRTrain = new LinearRegTrainBatchOp()
                .setFeatureCols(selectedColNames)
                .setLabelCol(labelColName);


        List dataSet = lrop.fit(pravegadata).transform(pravegadata).collect();

        String[] a = dataSet.get(0).toString().split(",");
        String[] b = dataSet.get(dataSet.size()-1).toString().split(",");

        Double diff = ((Double.parseDouble(b[b.length-1]))-(Double.parseDouble(a[a.length-1])))/Double.parseDouble(a[a.length-1]);
        System.out.println(a[a.length-1]);
        System.out.println(b[b.length-1]);
        System.out.println(diff);

        // Linear Regression model sink to Pravega
        BatchOperator  LRModel = pravegadata.link(LRTrain).link (new PravegaSinkBatchOp()
                .setDataFormat("csv")
                .setPravegaControllerUri("tcp://localhost:9090")
                .setPravegaScope("workshop-samples")
                .setPravegaStream("stream"));
        BatchOperator.execute();

        PravegaSourceBatchOp newsource =
                new PravegaSourceBatchOp()
                        .setPravegaControllerUri("tcp://localhost:9090")
                        .setPravegaScope("workshop-samples")
                        .setPravegaStream("stream");
        System.out.print("Pravega Table Schema" + newsource.getSchema());
        newsource.print();



        return this;

    }

    public static void main(String[] args) throws Exception {
        LOG.info("########## FlinkSQLJOINReader START #############");
        CommonParams.init(args);
        String scope = CommonParams.getParam("pravega_scope");
        String streamName = CommonParams.getParam("stream_name");
        String controllerURI = CommonParams.getParam("pravega_controller_uri");

        LOG.info("#######################     SCOPE   ###################### " + scope);
        LOG.info("#######################     streamName   ###################### " + streamName);
        LOG.info("#######################     controllerURI   ###################### " + controllerURI);
        PravegaLinearRegressionExample PravegaLinearRegressionExample = new PravegaLinearRegressionExample (scope, streamName, controllerURI);
        PravegaLinearRegressionExample.PravegaBatchmML(scope, streamName, controllerURI);
    }
}

