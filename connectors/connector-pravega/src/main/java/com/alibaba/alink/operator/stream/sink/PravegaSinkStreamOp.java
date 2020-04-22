package com.alibaba.alink.operator.stream.sink;


import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.io.pravega.PravegaCsvRowSerializationSchema;
import com.alibaba.alink.operator.common.io.pravega.PravegaJsonRowSerializationSchema;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.PravegaSinkParams;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.*;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.net.URI;


/**
 * Data Sink for pravega.
 */
@IoOpAnnotation(name = "pravega", hasTimestamp = true, ioType = IOType.SinkStream)
public class PravegaSinkStreamOp extends BaseSinkStreamOp<PravegaSinkStreamOp>
        implements PravegaSinkParams<PravegaSinkStreamOp> {

    public PravegaSinkStreamOp() {
        this(new Params());
    }


    public PravegaSinkStreamOp(Params params) {
        super(AnnotationUtils.annotatedName(PravegaSinkStreamOp.class), params);
    }

    @Override
    protected PravegaSinkStreamOp sinkFrom(StreamOperator in) {
        final String pravegaControllerUri = getPravegaControllerUri();
        final String pravegaScope = getPravegaScope();
        final String pravegaStream = getPravegaStream();
        final Boolean PravegaStandalone = getPravegaStandalone();
        final String dataFormat = getDataFormat();
        final String[] colNames = in.getColNames();
        final String fieldDelimiter = CsvUtil.unEscape(getFieldDelimiter());

        System.out.println(in.getDataStream());

        //Fetch the Pravega writer mode
        PravegaWriterMode writerMode = null;
        final String pravegawritemode = getPravegaWriterMode();
        switch (pravegawritemode.toUpperCase()) {
            case ("ATLEAST_ONCE"):
                writerMode = PravegaWriterMode.ATLEAST_ONCE;
                break;
            case ("EXACTLY_ONCE"):
                writerMode = PravegaWriterMode.EXACTLY_ONCE;
                break;
            case ("BEST_EFFORT"):
                writerMode = PravegaWriterMode.BEST_EFFORT;
                break;
        }

        //Generate the routing key
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


        //Create Pravega Scope and Stream in case it does not exist.
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

        FlinkPravegaWriter<Row> pravegaSink = FlinkPravegaWriter.<Row>builder()
                .forStream(pravegaStream)
                .withPravegaConfig(pravegaConfig)
                .withSerializationSchema(serializationSchema)
                .withEventRouter(router)
                .withWriterMode(writerMode)
                .build();

        DataStream<Row> inputData = in.getDataStream();
        inputData.addSink(pravegaSink).name("pravega_" + pravegaScope + "/" + pravegaStream);

        return this;
    }
}
