package com.alibaba.alink.pipeline;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * A pipeline is a linear workflow which chains {@link EstimatorBase}s and {@link TransformerBase}s to
 * execute an algorithm.
 */
public class Pipeline extends EstimatorBase<Pipeline, PipelineModel> {

	private ArrayList<PipelineStageBase> stages = new ArrayList<>();

	public Pipeline() {
		this(new Params());
	}

	public Pipeline(Params params) {
		super(params);
	}

	public Pipeline(PipelineStageBase<?>... stages) {
		super(null);
		if (null != stages) {
			this.stages.addAll(Arrays.asList(stages));
		}
	}

	@Override
	public Pipeline clone() throws CloneNotSupportedException {
		Pipeline pipeline = new Pipeline();
		for (PipelineStageBase stage : this.stages) {
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
	public Pipeline add(PipelineStageBase stage) {
		this.stages.add(stage);
		return this;
	}

	/**
	 * Inserts the specified stage at the specified position in this
	 * pipeline. Shifts the stage currently at that position (if any) and
	 * any subsequent stages to the right (adds one to their indices).
	 *
	 * @param index index at which the specified stage is to be inserted
	 * @param stage pipelineStage to be inserted
	 * @return this pipeline
	 * @throws IndexOutOfBoundsException
	 */
	public Pipeline add(int index, PipelineStageBase stage) {
		this.stages.add(index, stage);
		return this;
	}

	/**
	 * Removes the stage at the specified position in this pipeline.
	 * Shifts any subsequent stages to the left (subtracts one from their
	 * indices).
	 *
	 * @param index the index of the stage to be removed
	 * @return the pipeline after remove operation
	 * @throws IndexOutOfBoundsException
	 */
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
	public PipelineStageBase get(int index) {
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
	public PipelineModel fit(BatchOperator input) {
		int lastEstimatorIdx = getIndexOfLastEstimator();
		TransformerBase[] transformers = new TransformerBase[stages.size()];
		for (int i = 0; i < stages.size(); i++) {
			PipelineStageBase stage = stages.get(i);
			if (i <= lastEstimatorIdx) {
				if (stage instanceof EstimatorBase) {
					transformers[i] = ((EstimatorBase) stage).fit(input);
				} else if (stage instanceof TransformerBase) {
					transformers[i] = (TransformerBase) stage;
				}
				if (i < lastEstimatorIdx) {
					input = transformers[i].transform(input);
				}
			} else {
				// After lastEstimatorIdx, there're only Transformer stages, so it's safe to do type cast.
				transformers[i] = (TransformerBase) stage;
			}
		}
		return new PipelineModel(transformers).setMLEnvironmentId(input.getMLEnvironmentId());

	}

	/**
	 * Train the pipeline with stream data.
	 *
	 * @param input input data
	 * @return pipeline model
	 */
	@Override
	public PipelineModel fit(StreamOperator input) {
		int lastEstimatorIdx = getIndexOfLastEstimator();
		TransformerBase[] transformers = new TransformerBase[stages.size()];
		for (int i = 0; i < stages.size(); i++) {
			PipelineStageBase stage = stages.get(i);
			if (i <= lastEstimatorIdx) {
				if (stage instanceof EstimatorBase) {
					transformers[i] = ((EstimatorBase) stage).fit(input);
				} else if (stage instanceof TransformerBase) {
					transformers[i] = (TransformerBase) stage;
				}
				if (i < lastEstimatorIdx) {
					input = transformers[i].transform(input);
				}
			} else {
				// After lastEstimatorIdx, there're only Transformer stages, so it's safe to do type cast.
				transformers[i] = (TransformerBase) stage;
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
}
