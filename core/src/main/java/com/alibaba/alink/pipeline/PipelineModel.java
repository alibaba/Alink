package com.alibaba.alink.pipeline;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * The model fitted by {@link Pipeline}.
 */
public class PipelineModel extends ModelBase<PipelineModel> implements LocalPredictable {

    TransformerBase[] transformers;

    public PipelineModel(Params params) {
        super(params);
    }

    public PipelineModel() {
        this(new Params());
    }

    public PipelineModel(TransformerBase[] transformers) {
        super(null);
        if (null == transformers) {
            this.transformers = new TransformerBase[]{};
        } else {
            List<TransformerBase> flattened = new ArrayList<>();
            flattenTransformers(transformers, flattened);
            this.transformers = flattened.toArray(new TransformerBase[0]);
        }
    }

    private static void flattenTransformers(TransformerBase[] transformers, List<TransformerBase> flattened) {
        for (TransformerBase transformer : transformers) {
            if (transformer instanceof PipelineModel) {
                flattenTransformers(((PipelineModel) transformer).transformers, flattened);
            } else {
                flattened.add(transformer);
            }
        }
    }

    @Override
    public BatchOperator<?> transform(BatchOperator input) {
        for (TransformerBase transformer : this.transformers) {
            input = transformer.transform(input);
        }
        return input;
    }

    @Override
    public StreamOperator transform(StreamOperator input) {
        for (TransformerBase transformer : this.transformers) {
            input = transformer.transform(input);
        }
        return input;
    }

    @Override
    public LocalPredictor getLocalPredictor(TableSchema inputSchema) throws Exception {
        if (null == transformers || transformers.length == 0) {
            throw new RuntimeException("PipelineModel is empty.");
        }

        List<BatchOperator> allModelData = new ArrayList<>();

        for (TransformerBase transformer : transformers) {
            if (!(transformer instanceof LocalPredictable)) {
                throw new RuntimeException(transformer.getClass().toString() + " not support local predict.");
            }
            if (transformer instanceof MapModel) {
                allModelData.add(BatchOperator
                    .fromTable(((MapModel) transformer).getModelData())
                    .setMLEnvironmentId(transformer.getMLEnvironmentId()));
            }
        }

        List<List<Row>> allModelDataRows = BatchOperator.collect(allModelData.toArray(new BatchOperator[0]));

        LocalPredictor predictor = null;
        TableSchema schema = inputSchema;
        int numMapperModel = 0;

        for (TransformerBase transformer : transformers) {
            LocalPredictor localPredictor;

            if (transformer instanceof MapModel) {
                MapModel<?> mapModel = (MapModel) transformer;
                ModelMapper mapper = mapModel
                    .mapperBuilder
                    .apply(mapModel.modelData.getSchema(), schema, mapModel.getParams());
                mapper.loadModel(allModelDataRows.get(numMapperModel++));
                localPredictor = new LocalPredictor(mapper);
            } else {
                localPredictor = ((LocalPredictable) transformer).getLocalPredictor(schema);
            }

            schema = localPredictor.getOutputSchema();
            if (predictor == null) {
                predictor = localPredictor;
            } else {
                predictor.merge(localPredictor);
            }
        }

        return predictor;
    }


    public static final TableSchema PIPELINE_MODEL_SCHEMA = new TableSchema(
        new String[]{"model_id", "model_data"}, new TypeInformation[]{Types.LONG, Types.STRING}
    );

    /**
     * Save the pipeline model to a path.
     */
    public void save(String path) {
        this.save().link(new CsvSinkBatchOp(path));
    }

    /**
     * Pack the pipeline model to a BatchOperator.
     */
    public BatchOperator save() {
        return ModelExporterUtils.packTransformersArray(transformers);
    }

    /**
     * Load the pipeline model from a path.
     */
    public static PipelineModel load(String path) {
        return load(new CsvSourceBatchOp(path, PIPELINE_MODEL_SCHEMA));
    }

    /**
     * Load the pipeline model from a BatchOperator.
     */
    public static PipelineModel load(BatchOperator batchOp) {
        return new PipelineModel(ModelExporterUtils.unpackTransformersArray(batchOp));
    }
}
