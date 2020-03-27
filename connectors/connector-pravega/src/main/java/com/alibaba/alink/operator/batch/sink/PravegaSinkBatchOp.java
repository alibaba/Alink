package com.alibaba.alink.operator.batch.sink;


import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.io.pravega.PravegaCsvRowSerializationSchema;
import com.alibaba.alink.operator.common.io.pravega.PravegaJsonRowSerializationSchema;
import com.alibaba.alink.params.io.PravegaSinkParams;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.FlinkPravegaOutputFormat;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.net.URI;
import java.util.Collection;


@IoOpAnnotation(name = "pravega", ioType = IOType.SinkBatch)
public class PravegaSinkBatchOp extends BaseSinkBatchOp<PravegaSinkBatchOp>
        implements PravegaSinkParams<PravegaSinkBatchOp> {


    public PravegaSinkBatchOp() {
        this(new Params());
    }


    public PravegaSinkBatchOp(Params params) {
        super(AnnotationUtils.annotatedName(PravegaSinkBatchOp.class), params);
    }

    @Override
    protected PravegaSinkBatchOp sinkFrom(BatchOperator in) {
        final String pravegaControllerUri = getPravegaControllerUri();
        final String pravegaScope = getPravegaScope();
        final String pravegaStream = getPravegaStream();
        final Boolean PravegaStandalone = getPravegaStandalone();
        final String dataFormat = getDataFormat();
        final String[] colNames = in.getColNames();
        final String fieldDelimiter = CsvUtil.unEscape(getFieldDelimiter());

        System.out.println(in.collect().toString());

        PravegaEventRouter<Row> router = new PravegaEventRouter<Row>() {
            @Override
            public String getRoutingKey(Row eventType) {
                final String pravegaRoutingKey = getPravegaRoutingKey();
                return pravegaRoutingKey;
            }
        };

        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(pravegaControllerUri))
                .withDefaultScope(pravegaScope)
                //Enable it if with Nautilus
                //.withCredentials(credentials)
                .withHostnameValidation(false);


        //Create Pravega Scope and Stream
        Stream stream = pravegaConfig.resolve(pravegaStream);
        try (StreamManager streamManager = StreamManager.create(pravegaConfig.getClientConfig())) {
            // create the requested scope (if standalone)
            if (PravegaStandalone) {
                streamManager.createScope(pravegaScope);
            }
            // create the requested stream based on the given stream configuration
            streamManager.createStream(stream.getScope(), stream.getStreamName(), StreamConfiguration.builder().build());
        }

        SerializationSchema<Row> serializationSchema = null;
        //serializationSchema = new JsonRowSerializationSchema(colNames);
        if (dataFormat.equalsIgnoreCase("csv")) {
            serializationSchema = new PravegaCsvRowSerializationSchema(fieldDelimiter);
        } else if (dataFormat.equalsIgnoreCase("json")) {
            serializationSchema = new PravegaJsonRowSerializationSchema(colNames);
        }


        FlinkPravegaOutputFormat<Row> outputFormat = FlinkPravegaOutputFormat.<Row>builder()
                .forStream(pravegaStream)
                .withPravegaConfig(pravegaConfig)
                .withSerializationSchema(serializationSchema)
                .withEventRouter(router)
                .build();

        Collection<Row> inputData = in.collect();
        ExecutionEnvironment env = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment();

        env.fromCollection(inputData).output(outputFormat).name("pravega_" + pravegaScope + "/" + pravegaStream);
        return this;
    }
}
