package io.pravega.alink;


import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.JsonValueBatchOp;
import com.alibaba.alink.operator.batch.sink.PravegaSinkBatchOp;
import com.alibaba.alink.operator.batch.source.PravegaSourceBatchOp;


public class test_readWriteWithPravega {
    public static void main(String[] args) throws Exception {

        System.out.println("Starting to read the dataset from Pravega........");

        PravegaSourceBatchOp source =
                new PravegaSourceBatchOp()
                        .setPravegaControllerUri("tcp://localhost:9090")
                        .setPravegaScope("workshop-samples")
                        .setPravegaStream("workshop-stream");
        BatchOperator op = source.link (
                new JsonValueBatchOp()
                        .setSelectedCol("event")
                        .setReservedCols(new String[] {})
                        .setOutputCols(
                                new String[] {"DateTime", "PDP2GGERDJKS01000000_2", "PDP2GHDEDJKS01000017_2"}
                        )
                        .setJsonPath(new String[] {"$.DateTime", "$.PDP2GGERDJKS01000000_2", "$.PDP2GHDEDJKS01000017_2"})
        );
        System.out.print("Pravega Table Schema" + op.getSchema());

        PravegaSinkBatchOp sink = new PravegaSinkBatchOp()
                .setDataFormat("csv")
                .setPravegaControllerUri("tcp://localhost:9090")
                .setPravegaScope("workshop-samples")
                .setPravegaStream("stream");
        sink.linkFrom(op);
        BatchOperator.execute();

        PravegaSourceBatchOp newsource =
                new PravegaSourceBatchOp()
                        .setPravegaControllerUri("tcp://localhost:9090")
                        .setPravegaScope("workshop-samples")
                        .setPravegaStream("stream");
        System.out.print("Pravega Table Schema" + newsource.getSchema());
        newsource.print();

    }

}