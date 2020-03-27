package com.alibaba.alink.operator.batch.source;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.io.pravega.PravegaRowDeserializationSchema;
import com.alibaba.alink.params.io.PravegaSourceParams;
import io.pravega.connectors.flink.FlinkPravegaInputFormat;
import io.pravega.connectors.flink.PravegaConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.net.URI;

@IoOpAnnotation(name = "pravega", ioType = IOType.SourceBatch)
public final class PravegaSourceBatchOp extends BaseSourceBatchOp<PravegaSourceBatchOp>
        implements PravegaSourceParams<PravegaSourceBatchOp> {

    public PravegaSourceBatchOp() {
        this(new Params());
    }


    public PravegaSourceBatchOp(Params params) {
        super(AnnotationUtils.annotatedName(PravegaSourceBatchOp.class), params);
    }

    @Override
    protected Table initializeDataSource() {
        String pravegaControllerUri = getPravegaControllerUri();
        String pravegascope = getPravegaScope();
        String pravegaStream = getPravegaStream();
        String schemaStr = "event String";
        //DeserializationSchema deserializationSchema = getPravegaDeserializer();
        final String[] colNames = CsvUtil.getColNames(schemaStr);
        final TypeInformation[] colTypes = CsvUtil.getColTypes(schemaStr);


        //Properties props = new Properties();
        //props.setProperty("pravegaControllerUri", pravegaControllerUri);
        //props.setProperty("pravegascope", pravegascope);


        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(pravegaControllerUri))
                .withDefaultScope(pravegascope)
                //Enable it if with Nautilus
                //.withCredentials(credentials)
                .withHostnameValidation(false);


        FlinkPravegaInputFormat<Row> source = FlinkPravegaInputFormat.<Row>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(pravegaStream)
                .withDeserializationSchema(new PravegaRowDeserializationSchema(Row.class))
                .build();

        ExecutionEnvironment execEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment();

        DataSource<Row> data = execEnv.createInput(source, TypeInformation.of(Row.class)).name("PravegaBatch");

        return DataSetConversionUtil.toTable(getMLEnvironmentId(), data, colNames, colTypes);
    }
}
