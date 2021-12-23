package com.alibaba.alink.pipeline;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.PipelineModelMapper;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.ModelStreamFileSourceStreamOp;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.pipeline.recommendation.BaseRecommender;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The model fitted by {@link Pipeline}.
 */
public final class PipelineModel extends ModelBase <PipelineModel> implements LocalPredictable {

	private static final long serialVersionUID = -7217216709192253383L;
	TransformerBase <?>[] transformers;
	private FilePath modelStreamFilePath = null;
	private int modelStreamScanInterval = 10;
	private String modelStreamStartTime = null;

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
		List <PipelineModel> pipelineModels = splitPipelineModel(true);
		int maxNumThread = getMaxNumThread(this);
		for (PipelineModel model : pipelineModels) {
			if (model.transformers.length == 1) {
				input = model.transformers[0].transform(input);
			} else {
				int maxCurModelNumThread = getMaxNumThread(model);
				if (0 >= maxCurModelNumThread) {
					maxCurModelNumThread = maxNumThread;
				}
				if (0 >= maxCurModelNumThread) {
					maxCurModelNumThread = MapperParams.NUM_THREADS.getDefaultValue();
				}

				BatchOperator <?> pipelineExpandModel = model.save();
				TableSchema outSchema = getOutSchema(model, input.getSchema());
				input = new PipelinePredictBatchOp()
					.setMLEnvironmentId(input.getMLEnvironmentId())
					.setNumThreads(maxCurModelNumThread)
					.set(PipelineModelMapper.PIPELINE_TRANSFORM_OUT_COL_NAMES, outSchema.getFieldNames())
					.set(PipelineModelMapper.PIPELINE_TRANSFORM_OUT_COL_TYPES,
						FlinkTypeConverter.getTypeString(outSchema.getFieldTypes()))
					.linkFrom(pipelineExpandModel, input);

				TransformerBase <?> transformer = model.transformers[model.transformers.length - 1];

				if (transformer.params.get(LAZY_PRINT_TRANSFORM_DATA_ENABLED)) {
					input.lazyPrint(transformer.params.get(LAZY_PRINT_TRANSFORM_DATA_NUM),
						transformer.params.get(LAZY_PRINT_TRANSFORM_DATA_TITLE));
				}

				if (transformer.params.get(LAZY_PRINT_TRANSFORM_STAT_ENABLED)) {
					input.lazyPrintStatistics(transformer.params.get(LAZY_PRINT_TRANSFORM_STAT_TITLE));
				}
			}
		}
		return postProcessTransformResult(input);
	}

	@Override
	public StreamOperator <?> transform(StreamOperator <?> input) {
		List <PipelineModel> pipelineModels = splitPipelineModel(false);
		int maxNumThread = getMaxNumThread(this);
		for (PipelineModel model : pipelineModels) {
			if (model.transformers.length == 1) {
				input = model.transformers[0].transform(input);
			} else {
				int maxCurModelNumThread = getMaxNumThread(model);
				if (0 >= maxCurModelNumThread) {
					maxCurModelNumThread = maxNumThread;
				}
				if (0 >= maxCurModelNumThread) {
					maxCurModelNumThread = MapperParams.NUM_THREADS.getDefaultValue();
				}

				BatchOperator <?> pipelineExpandModel = model.save();
				TableSchema outSchema = getOutSchema(model, input.getSchema());
				PipelinePredictStreamOp pipePredictOp = new PipelinePredictStreamOp(pipelineExpandModel)
					.setMLEnvironmentId(input.getMLEnvironmentId())
					.setNumThreads(maxCurModelNumThread)
					.set(PipelineModelMapper.PIPELINE_TRANSFORM_OUT_COL_NAMES, outSchema.getFieldNames())
					.set(PipelineModelMapper.PIPELINE_TRANSFORM_OUT_COL_TYPES,
						FlinkTypeConverter.getTypeString(outSchema.getFieldTypes()));
				input = (modelStreamFilePath == null) ? pipePredictOp.linkFrom(input) :
					pipePredictOp.linkFrom(input, new ModelStreamFileSourceStreamOp()
						.setFilePath(modelStreamFilePath)
						.setScanInterval(modelStreamScanInterval)
						.setStartTime(modelStreamStartTime)
						.setSchemaStr(CsvUtil.schema2SchemaStr(pipelineExpandModel.getSchema())));
			}
		}
		return input;
	}

	public PipelineModel setModelStreamFilePath(FilePath filePath) {
		this.modelStreamFilePath = filePath;
		for (TransformerBase <?> t : transformers) {
			if (t.params.contains(ModelMapperParams.MODEL_STREAM_FILE_PATH)) {
				t.params.remove(ModelMapperParams.MODEL_STREAM_FILE_PATH);
			}
			if (t.params.contains(ModelMapperParams.MODEL_STREAM_SCAN_INTERVAL)) {
				t.params.remove(ModelMapperParams.MODEL_STREAM_SCAN_INTERVAL);
			}
			if (t.params.contains(ModelMapperParams.MODEL_STREAM_START_TIME)) {
				t.params.remove(ModelMapperParams.MODEL_STREAM_START_TIME);
			}
		}
		return this;
	}

	public PipelineModel setModelStreamScanInterval(int scanInterval) {
		this.modelStreamScanInterval = scanInterval;
		return this;
	}

	public PipelineModel setModelStreamStartTime(String modelStreamStartTime) {
		this.modelStreamStartTime = modelStreamStartTime;
		return this;
	}

	private static int getMaxNumThread(PipelineModel model) {
		int maxNumThread = -1;
		for (TransformerBase <?> transformer : model.transformers) {
			maxNumThread = Math.max(getNumThreadDefaultNeg1(transformer), maxNumThread);
		}
		return maxNumThread;
	}

	private static int getNumThreadDefaultNeg1(TransformerBase <?> transformer) {
		if (transformer.params.contains(MapperParams.NUM_THREADS)) {
			return -1;
		} else {
			return transformer.get(MapperParams.NUM_THREADS);
		}
	}

	/**
	 * split by step
	 * 1. if not MapModel,MapTransformer,BaseRecommender, it is split point.
	 * 2. if batch transform, and lazy(print or stat), next transformer is split point.
	 * 3. if num thread of transformer is set, will go to next split point.
	 */
	List <PipelineModel> splitPipelineModel(boolean isBatch) {
		List <PipelineModel> models = new ArrayList <>();
		List <TransformerBase <?>> curTransformers = new ArrayList <>();
		int latestNumThread = -1;
		TransformerBase <?> latestTransformer = null;

		for (TransformerBase <?> transformer : this.transformers) {
			boolean isSplitPoint = false;
			int curNumThread = transformer.params.contains(MapperParams.NUM_THREADS) ?
				transformer.params.get(MapperParams.NUM_THREADS) : -1;

			if (!(transformer instanceof MapModel
				|| transformer instanceof MapTransformer
				|| transformer instanceof BaseRecommender)) {
				isSplitPoint = true;
			} else if (isBatch
				&& latestTransformer != null
				&& (latestTransformer.params.get(LAZY_PRINT_TRANSFORM_DATA_ENABLED)
				|| latestTransformer.params.get(LAZY_PRINT_TRANSFORM_STAT_ENABLED))) {
				isSplitPoint = true;
				latestTransformer = transformer;

			} else if (-1 == curNumThread) {
				latestTransformer = transformer;
			} else if (-1 == latestNumThread) {
				latestTransformer = transformer;
				latestNumThread = curNumThread;
			} else if (curNumThread != latestNumThread) {
				isSplitPoint = true;
				latestNumThread = curNumThread;
				latestTransformer = transformer;
			}

			if (isSplitPoint && curTransformers.size() > 0) {
				models.add(new PipelineModel(curTransformers.toArray(new TransformerBase[0])));
				curTransformers.clear();
				curTransformers.add(transformer);
				latestNumThread = curNumThread;
			} else {
				curTransformers.add(transformer);
			}
		}
		if (curTransformers.size() > 0) {
			models.add(new PipelineModel(curTransformers.toArray(new TransformerBase[0])));
		}
		return models;
	}

	@Override
	public LocalPredictor collectLocalPredictor(TableSchema inputSchema) throws Exception {
		if (modelStreamFilePath != null) {
			BatchOperator <?> modelSave = ModelExporterUtils.serializePipelineStages(Arrays.asList(transformers));
			TableSchema extendSchema = getOutSchema(this, inputSchema);
			BatchOperator <?> model = new TableSourceBatchOp(DataSetConversionUtil
				.toTable(modelSave.getMLEnvironmentId(),
					modelSave.getDataSet()
						.map(new PipelineModelMapper
							.ExtendPipelineModelRow(extendSchema.getFieldNames().length + 1)),
					PipelineModelMapper.getExtendModelSchema(modelSave.getSchema(),
						extendSchema.getFieldNames(),
						extendSchema.getFieldTypes())));

			List <Row> modelRows = model.collect();
			Params params = new Params()
				.set(ModelMapperParams.MODEL_STREAM_FILE_PATH, modelStreamFilePath.serialize())
				.set(ModelMapperParams.MODEL_STREAM_START_TIME, modelStreamStartTime)
				.set(ModelMapperParams.MODEL_STREAM_SCAN_INTERVAL, modelStreamScanInterval);
			ModelMapper mapper = new PipelineModelMapper(model.getSchema(), inputSchema, params);
			mapper.loadModel(modelRows);
			return new LocalPredictor(mapper);
		}
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

		List <List <Row>> allModelDataRows;
		if (!allModelData.isEmpty()) {
			allModelDataRows = BatchOperator.collect(allModelData.toArray(new BatchOperator <?>[0]));
		} else {
			allModelDataRows = new ArrayList <>();
		}

		TableSchema schema = inputSchema;
		int numMapperModel = 0;

		List <Mapper> mappers = new ArrayList <>();
		for (TransformerBase <?> transformer : transformers) {
			Mapper mapper;
			if (transformer instanceof MapModel) {
				mapper = ModelExporterUtils.createMapperFromStage(transformer,
					((MapModel <?>) transformer).modelData.getSchema(),
					schema, allModelDataRows.get(numMapperModel));
				numMapperModel++;
			} else if (transformer instanceof BaseRecommender) {
				mapper = ModelExporterUtils.createMapperFromStage(transformer,
					((BaseRecommender <?>) transformer).modelData.getSchema(),
					schema, allModelDataRows.get(numMapperModel));
				numMapperModel++;
			} else {
				mapper = ModelExporterUtils.createMapperFromStage(transformer, null, schema, null);
			}
			mappers.add(mapper);
			schema = mapper.getOutputSchema();
		}

		return new LocalPredictor(mappers.toArray(new Mapper[0]));
	}

	@Override
	public BatchOperator <?> getModelData() {
		throw new UnsupportedOperationException("Unsupported getModelData in Pipeline model");
	}

	@Override
	public PipelineModel setModelData(BatchOperator <?> modelData) {
		throw new UnsupportedOperationException("Unsupported setModelData in Pipeline model");
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

	private TableSchema getOutSchema(PipelineModel pipelineModel, TableSchema inputSchema) {
		TableSchema outSchema = inputSchema;
		for (TransformerBase <?> transformer : pipelineModel.transformers) {
			TableSchema modelSchema = null;
			if (transformer instanceof MapModel) {
				modelSchema = ((MapModel <?>) transformer).modelData.getSchema();
			} else if (transformer instanceof BaseRecommender) {
				modelSchema = ((BaseRecommender <?>) transformer).modelData.getSchema();
			}
			Mapper mapper = ModelExporterUtils.createMapperFromStage(transformer, modelSchema, outSchema, null);
			outSchema = mapper.getOutputSchema();
		}

		return outSchema;
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

	public static PipelineModel collectLoad(BatchOperator <?> batchOp) {
		return new PipelineModel(
			ModelExporterUtils. <TransformerBase <?>>fillPipelineStages(
				batchOp,
				ModelExporterUtils.collectMetaFromOp(batchOp),
				batchOp.getSchema()
			).toArray(new TransformerBase <?>[0]));
	}

	@Deprecated
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

	static class PipelinePredictBatchOp extends ModelMapBatchOp <PipelinePredictBatchOp>
		implements MapperParams <PipelinePredictBatchOp> {

		PipelinePredictBatchOp() {
			this(new Params());
		}

		PipelinePredictBatchOp(Params params) {
			super(PipelineModelMapper::new, params);
		}
	}

	static class PipelinePredictStreamOp extends ModelMapStreamOp <PipelinePredictStreamOp>
		implements MapperParams <PipelinePredictStreamOp> {

		PipelinePredictStreamOp(BatchOperator <?> model) {
			this(model, new Params());
		}

		PipelinePredictStreamOp(BatchOperator <?> model, Params params) {
			super(model, PipelineModelMapper::new, params);
		}
	}
}
