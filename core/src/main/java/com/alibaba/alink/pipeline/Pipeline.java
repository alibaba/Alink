package com.alibaba.alink.pipeline;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;

import java.util.ArrayList;
import java.util.Arrays;

import static com.alibaba.alink.common.lazy.HasLazyPrintTransformInfo.LAZY_PRINT_TRANSFORM_DATA_ENABLED;
import static com.alibaba.alink.common.lazy.HasLazyPrintTransformInfo.LAZY_PRINT_TRANSFORM_STAT_ENABLED;

/**
 * A pipeline is a linear workflow which chains {@link EstimatorBase}s and {@link TransformerBase}s to execute an
 * algorithm.
 */
public class Pipeline extends EstimatorBase <Pipeline, PipelineModel> {

	private static final long serialVersionUID = 1562871813230757217L;
	ArrayList <PipelineStageBase <?>> stages = new ArrayList <>();

	public Pipeline() {
		this(new Params());
	}

	public Pipeline(Params params) {
		super(params);
	}

	public Pipeline(PipelineStageBase <?>... stages) {
		super(null);
		if (null != stages) {
			this.stages.addAll(Arrays.asList(stages));
		}
	}

	@Override
	public Pipeline clone() throws CloneNotSupportedException {
		Pipeline pipeline = new Pipeline();
		for (PipelineStageBase <?> stage : this.stages) {
			pipeline.add(stage.clone());
		}
		return pipeline;
	}

	/**
	 * Appends the specified stage to the end of this pipeline.
	 *
	 * @param stage pipelineStage to be appended to this pipeline
	 * @return this pipeline
	 */
	public Pipeline add(PipelineStageBase <?> stage) {
		this.stages.add(stage);
		return this;
	}

	/**
	 * Inserts the specified stage at the specified position in this pipeline. Shifts the stage currently at that
	 * position (if any) and any subsequent stages to the right (adds one to their indices).
	 *
	 * @param index index at which the specified stage is to be inserted
	 * @param stage pipelineStage to be inserted
	 * @return this pipeline
	 * @throws IndexOutOfBoundsException
	 */
	@Deprecated
	public Pipeline add(int index, PipelineStageBase <?> stage) {
		this.stages.add(index, stage);
		return this;
	}

	/**
	 * Removes the stage at the specified position in this pipeline. Shifts any subsequent stages to the left
	 * (subtracts
	 * one from their indices).
	 *
	 * @param index the index of the stage to be removed
	 * @return the pipeline after remove operation
	 * @throws IndexOutOfBoundsException
	 */
	@Deprecated
	public Pipeline remove(int index) {
		this.stages.remove(index);
		return this;
	}

	/**
	 * Returns the stage at the specified position in this pipeline.
	 *
	 * @param index index of the stage to return
	 * @return the stage at the specified position in this pipeline
	 * @throws IndexOutOfBoundsException
	 */
	public PipelineStageBase <?> get(int index) {
		return this.stages.get(index);
	}

	/**
	 * Returns the number of stages in this pipeline.
	 *
	 * @return the number of stages in this pipeline
	 */
	public int size() {
		return this.stages.size();
	}

	/**
	 * Train the pipeline with batch data.
	 *
	 * @param input input data
	 * @return pipeline model
	 */
	@Override
	public PipelineModel fit(BatchOperator <?> input) {
		return new PipelineModel(fit(input, false).f0).setMLEnvironmentId(input.getMLEnvironmentId());
	}

	@Override
	public BatchOperator <?> fitAndTransform(BatchOperator <?> input) {
		return fit(input, true).f1;
	}

	private Tuple2 <TransformerBase <?>[], BatchOperator <?>> fit(BatchOperator <?> input, boolean withTransform) {
		int lastEstimatorIdx = getIndexOfLastEstimator();
		TransformerBase <?>[] transformers = new TransformerBase <?>[stages.size()];
		for (int i = 0; i < stages.size(); i++) {
			PipelineStageBase <?> stage = stages.get(i);
			if (i <= lastEstimatorIdx) {
				if (stage instanceof EstimatorBase) {
					transformers[i] = ((EstimatorBase <?, ?>) stage).fit(input);
				} else if (stage instanceof TransformerBase) {
					transformers[i] = (TransformerBase <?>) stage;
				}
				if (i < lastEstimatorIdx) {
					// temporarily disable lazy print transform results
					Boolean lazyPrintTransformDataEnabled = (Boolean) transformers[i].get(
						LAZY_PRINT_TRANSFORM_DATA_ENABLED);
					Boolean lazyPrintTransformStatEnabled = (Boolean) transformers[i].get(
						LAZY_PRINT_TRANSFORM_STAT_ENABLED);
					transformers[i].set(LAZY_PRINT_TRANSFORM_DATA_ENABLED, false);
					transformers[i].set(LAZY_PRINT_TRANSFORM_STAT_ENABLED, false);

					input = transformers[i].transform(input);

					transformers[i].set(LAZY_PRINT_TRANSFORM_DATA_ENABLED, lazyPrintTransformDataEnabled);
					transformers[i].set(LAZY_PRINT_TRANSFORM_STAT_ENABLED, lazyPrintTransformStatEnabled);
				}
			} else {
				// After lastEstimatorIdx, there're only Transformer stages, so it's safe to do type cast.
				transformers[i] = (TransformerBase <?>) stage;

				if(withTransform){
					// temporarily disable lazy print transform results
					Boolean lazyPrintTransformDataEnabled = (Boolean) transformers[i].get(
						LAZY_PRINT_TRANSFORM_DATA_ENABLED);
					Boolean lazyPrintTransformStatEnabled = (Boolean) transformers[i].get(
						LAZY_PRINT_TRANSFORM_STAT_ENABLED);
					transformers[i].set(LAZY_PRINT_TRANSFORM_DATA_ENABLED, false);
					transformers[i].set(LAZY_PRINT_TRANSFORM_STAT_ENABLED, false);

					input = transformers[i].transform(input);

					transformers[i].set(LAZY_PRINT_TRANSFORM_DATA_ENABLED, lazyPrintTransformDataEnabled);
					transformers[i].set(LAZY_PRINT_TRANSFORM_STAT_ENABLED, lazyPrintTransformStatEnabled);
				}
			}
		}
		return new Tuple2 <>(transformers, input);
	}

	/**
	 * Train the pipeline with stream data.
	 *
	 * @param input input data
	 * @return pipeline model
	 */
	@Override
	public PipelineModel fit(StreamOperator <?> input) {
		int lastEstimatorIdx = getIndexOfLastEstimator();
		TransformerBase <?>[] transformers = new TransformerBase <?>[stages.size()];
		for (int i = 0; i < stages.size(); i++) {
			PipelineStageBase <?> stage = stages.get(i);
			if (i <= lastEstimatorIdx) {
				if (stage instanceof EstimatorBase) {
					transformers[i] = ((EstimatorBase <?, ?>) stage).fit(input);
				} else if (stage instanceof TransformerBase) {
					transformers[i] = (TransformerBase <?>) stage;
				}
				if (i < lastEstimatorIdx) {
					input = transformers[i].transform(input);
				}
			} else {
				// After lastEstimatorIdx, there're only Transformer stages, so it's safe to do type cast.
				transformers[i] = (TransformerBase <?>) stage;
			}
		}
		return new PipelineModel(transformers).setMLEnvironmentId(input.getMLEnvironmentId());
	}

	/**
	 * Get the index of the last estimator stage. If no estimator found, -1 is returned.
	 *
	 * @return index of the last estimator.
	 */
	private int getIndexOfLastEstimator() {
		int index = -1;
		for (int i = 0; i < stages.size(); i++) {
			if (stages.get(i) instanceof EstimatorBase) {
				index = i;
			}
		}
		return index;
	}

	/**
	 * Save the pipeline to a path using ak file.
	 */
	public void save(String path) {
		save(path, false);
	}

	public void save(String path, boolean overwrite) {
		save(new FilePath(path), overwrite);
	}

	/**
	 * Save the pipeline to a filepath using ak file.
	 */
	public void save(FilePath filePath) {
		save(filePath, false);
	}

	/**
	 * Save the pipeline to a filepath using ak file.
	 */
	public void save(FilePath filePath, boolean overwrite) {
		save(filePath, overwrite, 1);
	}

	/**
	 * Save the pipeline to a filepath using ak file.
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
	 * Pack the pipeline to a BatchOperator.
	 */
	public BatchOperator <?> save() {
		return ModelExporterUtils.serializePipelineStages(stages);
	}

	/**
	 * Load the pipeline from a path.
	 */
	public static Pipeline load(String path) {
		return load(new FilePath(path));
	}

	public static Pipeline load(FilePath filePath) {
		return load(filePath, MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID);
	}

	public static Pipeline load(FilePath filePath, Long mlEnvId) {
		Tuple2 <TableSchema, Row> schemaAndMeta = ModelExporterUtils.loadMetaFromAkFile(filePath);

		return new Pipeline(
			ModelExporterUtils.fillPipelineStages(
				new AkSourceBatchOp()
					.setFilePath(filePath)
					.setMLEnvironmentId(mlEnvId),
				ModelExporterUtils.deserializePipelineStagesFromMeta(
					schemaAndMeta.f1, schemaAndMeta.f0
				),
				schemaAndMeta.f0
			).toArray(new PipelineStageBase[0])
		);
	}

	public static Pipeline collectLoad(BatchOperator <?> batchOp) {
		return new Pipeline(
			ModelExporterUtils.fillPipelineStages(
				batchOp,
				ModelExporterUtils.collectMetaFromOp(batchOp),
				batchOp.getSchema()
			).toArray(new PipelineStageBase[0]));
	}

	public static Pipeline load(BatchOperator <?> batchOp, Row metaRow) {
		return new Pipeline(
			ModelExporterUtils. <TransformerBase <?>>fillPipelineStages(
				batchOp,
				ModelExporterUtils.deserializePipelineStagesFromMeta(metaRow, batchOp.getSchema()),
				batchOp.getSchema()
			).toArray(new PipelineStageBase <?>[0])
		);
	}
}
