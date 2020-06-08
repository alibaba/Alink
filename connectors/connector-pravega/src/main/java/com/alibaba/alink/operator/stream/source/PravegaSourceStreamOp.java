package com.alibaba.alink.operator.stream.source;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.io.pravega.PravegaRowDeserializationSchema;
import com.alibaba.alink.params.io.PravegaSourceParams;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.net.URI;
import java.util.Properties;

/**
 * Data source for pravega.
 */
@IoOpAnnotation(name = "pravega", hasTimestamp = true, ioType = IOType.SourceStream)
public class PravegaSourceStreamOp extends BaseSourceStreamOp<PravegaSourceStreamOp>
        implements PravegaSourceParams<PravegaSourceStreamOp> {

    public PravegaSourceStreamOp() {
        this(new Params());
    }


    public PravegaSourceStreamOp(Params params) {
        super(AnnotationUtils.annotatedName(PravegaSourceStreamOp.class), params);
    }

    @Override
    protected Table initializeDataSource() {
        String pravegaControllerUri = getPravegaControllerUri();
        String pravegascope = getPravegaScope();
        String pravegaStream = getPravegaStream();
        StreamCut pravegaStartStreamCut = null;
        if (getPravegaStartStreamCut().equals("UNBOUNDED"))
        {
            ClientConfig clientConfig= ClientConfig.builder().controllerURI(URI.create(pravegaControllerUri)).build();
            StreamManager streamManager = StreamManager.create(clientConfig);
            pravegaStartStreamCut = streamManager.getStreamInfo(pravegascope,pravegaStream).getTailStreamCut();
        } else {
            pravegaStartStreamCut = StreamCut.from(getPravegaStartStreamCut());
        }
        StreamCut pravegaEndStreamCut = StreamCut.from(getPravegaEndStreamCut());
        String schemaStr = "event String";
        final String[] colNames = CsvUtil.getColNames(schemaStr);
        final TypeInformation[] colTypes = CsvUtil.getColTypes(schemaStr);


        Properties props = new Properties();
        props.setProperty("pravegaControllerUri", pravegaControllerUri);
        props.setProperty("pravegascope", pravegascope);


        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(pravegaControllerUri))
                .withDefaultScope(pravegascope)
                //Enable it if with Nautilus
                //.withCredentials(credentials)
                .withHostnameValidation(false);

        /*Pravega pravega = new Pravega();
        pravega.tableSourceReaderBuilder()
                .forStream(pravegaStream)*/

        FlinkPravegaReader<Row> source = FlinkPravegaReader.<String>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(pravegaStream, pravegaStartStreamCut, pravegaEndStreamCut)
                .withDeserializationSchema(new PravegaRowDeserializationSchema(Row.class))
                .build();

        DataStream<Row> data = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamExecutionEnvironment()
                .addSource(source).name("Pravega StreamReader");
        return DataStreamConversionUtil.toTable(getMLEnvironmentId(), data, colNames, colTypes);
    }
}
