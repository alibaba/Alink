package com.alibaba.alink.pipeline;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.pipeline.recommendation.BaseRecommender;
import com.alibaba.alink.pipeline.recommendation.RecommenderUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The model fitted by {@link Pipeline}.
 */
public class PipelineModel extends ModelBase <PipelineModel> implements LocalPredictable {

	private static final long serialVersionUID = -7217216709192253383L;
	TransformerBase <?>[] transformers;

	public PipelineModel(Params params) {
		super(params);
	}

	public PipelineModel() {
		this(new Params());
	}

	public PipelineModel(TransformerBase <?>... transformers) {
		super(null);
		if (null == transformers) {
			this.transformers = new TransformerBase <?>[] {};
		} else {
			List <TransformerBase <?>> flattened = new ArrayList <>();
			flattenTransformers(transformers, flattened);
			this.transformers = flattened.toArray(new TransformerBase[0]);
		}
	}

	public TransformerBase <?>[] getTransformers() {
		return transformers;
	}

	private static void flattenTransformers(TransformerBase <?>[] transformers, List <TransformerBase <?>> flattened) {
		for (TransformerBase <?> transformer : transformers) {
			if (transformer instanceof PipelineModel) {
				flattenTransformers(((PipelineModel) transformer).transformers, flattened);
			} else {
				flattened.add(transformer);
			}
		}
	}

	@Override
	public BatchOperator <?> transform(BatchOperator <?> input) {
		for (TransformerBase <?> transformer : this.transformers) {
			input = transformer.transform(input);
		}
		return postProcessTransformResult(input);
	}

	@Override
	public StreamOperator <?> transform(StreamOperator <?> input) {
		for (TransformerBase <?> transformer : this.transformers) {
			input = transformer.transform(input);
		}
		return input;
	}

	@Override
	public LocalPredictor collectLocalPredictor(TableSchema inputSchema) throws Exception {
		if (null == transformers || transformers.length == 0) {
			throw new RuntimeException("PipelineModel is empty.");
		}

		List <BatchOperator <?>> allModelData = new ArrayList <>();

		for (TransformerBase <?> transformer : transformers) {
			if (!(transformer instanceof LocalPredictable)) {
				throw new RuntimeException(transformer.getClass().toString() + " not support local predict.");
			}
			if (transformer instanceof ModelBase) {
				allModelData.add(((ModelBase <?>) transformer).getModelData());
			}
		}

		List <List <Row>> allModelDataRows = BatchOperator.collect(allModelData.toArray(new BatchOperator <?>[0]));

		LocalPredictor predictor = null;
		TableSchema schema = inputSchema;
		int numMapperModel = 0;

		for (TransformerBase <?> transformer : transformers) {
			LocalPredictor localPredictor;

			if (transformer instanceof MapModel) {
				MapModel <?> mapModel = (MapModel <?>) transformer;
				ModelMapper mapper = mapModel
					.mapperBuilder
					.apply(mapModel.modelData.getSchema(), schema, mapModel.getParams());
				mapper.loadModel(allModelDataRows.get(numMapperModel++));
				localPredictor = new LocalPredictor(mapper);
			} else if (transformer instanceof BaseRecommender) {
				localPredictor = RecommenderUtil.createRecommLocalPredictor(
					(BaseRecommender <?>) transformer, ((BaseRecommender <?>) transformer).getModelData().getSchema(),
					inputSchema, allModelDataRows.get(numMapperModel++));
			} else {
				localPredictor = ((LocalPredictable) transformer).collectLocalPredictor(schema);
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

	/**
	 * Save the pipeline model to a path using ak file.
	 */
	public void save(String path) {
		save(path, false);
	}

	public void save(String path, boolean overwrite) {
		save(new FilePath(path), overwrite);
	}

	/**
	 * Save the pipeline model to a filepath using ak file.
	 */
	public void save(FilePath filePath) {
		save(filePath, false);
	}

	/**
	 * Save the pipeline model to a filepath using ak file.
	 */
	public void save(FilePath filePath, boolean overwrite) {
		save(filePath, overwrite, 1);
	}

	/**
	 * Save the pipeline model to a filepath using ak file.
	 */
	public void save(FilePath filePath, boolean overwrite, int numFiles) {
		save().link(
			new AkSinkBatchOp()
				.setMLEnvironmentId(getMLEnvironmentId())
				.setFilePath(filePath)
				.setOverwriteSink(overwrite)
				.setNumFiles(numFiles)
		);
	}

	/**
	 * Pack the pipeline model to a BatchOperator.
	 */
	public BatchOperator <?> save() {
		return ModelExporterUtils.serializePipelineStages(Arrays.asList(transformers));
	}

	/**
	 * Load the pipeline model from a path.
	 */
	public static PipelineModel load(String path) {
		return load(new FilePath(path));
	}

	public static PipelineModel load(FilePath filePath) {
		return load(filePath, MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID);
	}

	public static PipelineModel load(FilePath filePath, Long mlEnvId) {
		Tuple2 <TableSchema, Row> schemaAndMeta = ModelExporterUtils.loadMetaFromAkFile(filePath);

		return new PipelineModel(
			ModelExporterUtils. <TransformerBase <?>>fillPipelineStages(
				new AkSourceBatchOp()
					.setFilePath(filePath)
					.setMLEnvironmentId(mlEnvId),
				ModelExporterUtils.deserializePipelineStagesFromMeta(schemaAndMeta.f1, schemaAndMeta.f0),
				schemaAndMeta.f0
			).toArray(new TransformerBase <?>[0])
		);
	}

	public static PipelineModel collectLoad(BatchOperator <?> batchOp) {
		return new PipelineModel(
			ModelExporterUtils. <TransformerBase <?>>fillPipelineStages(
				batchOp,
				ModelExporterUtils.collectMetaFromOp(batchOp),
				batchOp.getSchema()
			).toArray(new TransformerBase <?>[0]));
	}

	public static PipelineModel load(BatchOperator <?> batchOp, Row metaRow) {
		return new PipelineModel(
			ModelExporterUtils. <TransformerBase <?>>fillPipelineStages(
				batchOp,
				ModelExporterUtils.deserializePipelineStagesFromMeta(metaRow, batchOp.getSchema()),
				batchOp.getSchema()
			).toArray(new TransformerBase <?>[0])
		);
	}
}
