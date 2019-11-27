package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.pipeline.EstimatorBase;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineStageBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * PipelineCandidatesBase.
 */
public abstract class PipelineCandidatesBase {
	protected final Pipeline pipeline;

	public PipelineCandidatesBase(EstimatorBase estimator) {
		if (estimator instanceof Pipeline) {
			this.pipeline = (Pipeline) estimator;
		} else {
			this.pipeline = new Pipeline().add(estimator);
		}
	}

	private static List <PipelineStageBase> listAllStages(Pipeline pipeline) {
		ArrayList <PipelineStageBase> allStages = new ArrayList <>();
		for (int i = 0; i < pipeline.size(); i++) {
			PipelineStageBase stage = pipeline.get(i);
			if (stage instanceof Pipeline) {
				allStages.addAll(listAllStages((Pipeline) stage));
			} else {
				allStages.add(stage);
			}
		}
		return allStages;
	}

	protected static void updatePipelineParams(Pipeline pipeline,
											   List <Tuple3 <Integer, ParamInfo, Object>> paramList) {
		List <PipelineStageBase> allStages = listAllStages(pipeline);
		for (Tuple3 <Integer, ParamInfo, Object> t3 : paramList) {
			allStages.get(t3.f0).set(t3.f1, t3.f2);
		}
	}

	protected static int[] findStageIndex(Pipeline pipeline, PipelineStageBase[] stages) {
		List <PipelineStageBase> allStages = listAllStages(pipeline);
		HashMap <PipelineStageBase, Integer> stageMap = new HashMap <>();
		for (int i = 0; i < allStages.size(); i++) {
			stageMap.put(allStages.get(i), i);
		}
		int n = stages.length;
		int[] stageIndex = new int[n];
		for (int i = 0; i < n; i++) {
			Preconditions.checkArgument(!(stages[i] instanceof Pipeline),
				"CAN NOT directly SET parameter to the pipeline object!");
			Integer index = stageMap.get(stages[i]);
			Preconditions.checkState(index != null, "The pipeline NOT contains the stage!");
			stageIndex[i] = index;
		}
		return stageIndex;
	}

	public abstract int size();

	public abstract Tuple2<Pipeline, List<Tuple3<Integer, ParamInfo, Object>>>
	get(int index, List <Double> experienceScores) throws CloneNotSupportedException;
}
