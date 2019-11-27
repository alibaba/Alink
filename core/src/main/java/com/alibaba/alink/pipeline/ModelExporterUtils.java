package com.alibaba.alink.pipeline;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.UnionAllBatchOp;
import com.alibaba.alink.operator.batch.utils.VectorSerializeBatchOp;
import com.alibaba.alink.operator.common.io.csv.CsvFormatter;
import com.alibaba.alink.operator.common.io.csv.CsvParser;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;
import com.jayway.jsonpath.JsonPath;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.pipeline.PipelineModel.PIPELINE_MODEL_SCHEMA;

/**
 * A utility class for exporting {@link PipelineModel}.
 */
class ModelExporterUtils {

    /**
     * Pack an array of transformers to a BatchOperator.
     */
    static BatchOperator packTransformersArray(TransformerBase[] transformers) {
        int numTransformers = transformers.length;
        String[] clazzNames = new String[numTransformers];
        String[] params = new String[numTransformers];
        String[] schemas = new String[numTransformers];
        for (int i = 0; i < numTransformers; i++) {
            clazzNames[i] = transformers[i].getClass().getCanonicalName();
            params[i] = transformers[i].getParams().toJson();
            schemas[i] = "";
            if (transformers[i] instanceof PipelineModel) {
                schemas[i] = CsvUtil.schema2SchemaStr(PIPELINE_MODEL_SCHEMA);
            } else if (transformers[i] instanceof ModelBase) {
                long envId = transformers[i].getMLEnvironmentId();
                BatchOperator data = BatchOperator.fromTable(((ModelBase) transformers[i]).getModelData());
                data.setMLEnvironmentId(envId);
                data = data.link(new VectorSerializeBatchOp().setMLEnvironmentId(envId));
                schemas[i] = CsvUtil.schema2SchemaStr(data.getSchema());
            }
        }
        Map<String, Object> config = new HashMap<>();
        config.put("clazz", clazzNames);
        config.put("param", params);
        config.put("schema", schemas);
        Row row = Row.of(-1L, JsonConverter.toJson(config));

        BatchOperator packed = new MemSourceBatchOp(Collections.singletonList(row), PIPELINE_MODEL_SCHEMA)
            .setMLEnvironmentId(transformers.length > 0 ? transformers[0].getMLEnvironmentId() :
                MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID);
        for (int i = 0; i < numTransformers; i++) {
            BatchOperator data = null;
            final long envId = transformers[i].getMLEnvironmentId();
            if (transformers[i] instanceof PipelineModel) {
                data = packTransformersArray(((PipelineModel) transformers[i]).transformers);
            } else if (transformers[i] instanceof ModelBase) {
                data = BatchOperator.fromTable(((ModelBase) transformers[i]).getModelData())
                    .setMLEnvironmentId(envId);
                data = data.link(new VectorSerializeBatchOp().setMLEnvironmentId(envId));
            }
            if (data != null) {
                packed = new UnionAllBatchOp().setMLEnvironmentId(envId).linkFrom(packed, packBatchOp(data, i));
            }
        }
        return packed;
    }

    /**
     * Unpack transformers array from a BatchOperator.
     */
    static TransformerBase[] unpackTransformersArray(BatchOperator batchOp) {
        String configStr;
        try {
            List<Row> rows = batchOp.as(new String[]{"f1", "f2"}).where("f1=-1").collect();
            Preconditions.checkArgument(rows.size() == 1, "Invalid model.");
            configStr = (String) rows.get(0).getField(1);
        } catch (Exception e) {
            throw new RuntimeException("Fail to collect model config.");
        }
        String[] clazzNames = JsonConverter.fromJson(JsonPath.read(configStr, "$.clazz").toString(), String[].class);
        String[] params = JsonConverter.fromJson(JsonPath.read(configStr, "$.param").toString(), String[].class);
        String[] schemas = JsonConverter.fromJson(JsonPath.read(configStr, "$.schema").toString(), String[].class);

        int numTransformers = clazzNames.length;
        TransformerBase[] transformers = new TransformerBase[numTransformers];
        for (int i = 0; i < numTransformers; i++) {
            try {
                Class clazz = Class.forName(clazzNames[i]);
                transformers[i] = (TransformerBase) clazz.getConstructor(Params.class).newInstance(
                    Params.fromJson(params[i])
                        .set(HasMLEnvironmentId.ML_ENVIRONMENT_ID, batchOp.getMLEnvironmentId()));
            } catch (Exception e) {
                throw new RuntimeException("Fail to re construct transformer.", e);
            }

            BatchOperator packed = batchOp.as(new String[]{"f1", "f2"}).where("f1=" + i);
            if (transformers[i] instanceof PipelineModel) {
                BatchOperator data = unpackBatchOp(packed, CsvUtil.schemaStr2Schema(schemas[i]));
                transformers[i] = new PipelineModel(unpackTransformersArray(data))
                    .setMLEnvironmentId(batchOp.getMLEnvironmentId());
            } else if (transformers[i] instanceof ModelBase) {
                BatchOperator data = unpackBatchOp(packed, CsvUtil.schemaStr2Schema(schemas[i]));
                ((ModelBase) transformers[i]).setModelData(data.getOutputTable());
            }
        }
        return transformers;
    }

    /**
     * Pack a BatchOperator with arbitrary schema into a BatchOperator that has two columns, one
     * id column of Long type, and one data column of String type.
     */
    private static BatchOperator packBatchOp(BatchOperator data, final long id) {
        DataSet<Row> rows = data.getDataSet();

        final TypeInformation[] types = data.getColTypes();

        rows = rows.map(new RichMapFunction<Row, Row>() {
            transient CsvFormatter formatter;

            @Override
            public void open(Configuration parameters) throws Exception {
                formatter = new CsvFormatter(types, "^", '\'');
            }

            @Override
            public Row map(Row value) throws Exception {
                return Row.of(id, formatter.format(value));
            }
        });

        return BatchOperator.fromTable(DataSetConversionUtil.toTable(data.getMLEnvironmentId(), rows,
            new String[]{"f1", "f2"}, new TypeInformation[]{Types.LONG, Types.STRING}))
            .setMLEnvironmentId(data.getMLEnvironmentId());
    }

    /**
     * Unpack a BatchOperator.
     */
    private static BatchOperator unpackBatchOp(BatchOperator data, TableSchema schema) {
        DataSet<Row> rows = data.getDataSet();
        final TypeInformation[] types = schema.getFieldTypes();

        rows = rows.map(new RichMapFunction<Row, Row>() {
            private transient CsvParser parser;

            @Override
            public void open(Configuration parameters) throws Exception {
                parser = new CsvParser(types, "^", '\'');
            }

            @Override
            public Row map(Row value) throws Exception {
                return parser.parse((String) value.getField(1));
            }
        });

        return BatchOperator.fromTable(DataSetConversionUtil.toTable(data.getMLEnvironmentId(), rows, schema))
            .setMLEnvironmentId(data.getMLEnvironmentId());
    }
}
