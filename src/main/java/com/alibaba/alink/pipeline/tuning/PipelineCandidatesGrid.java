package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;

import com.alibaba.alink.pipeline.EstimatorBase;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineStageBase;

import java.util.ArrayList;
import java.util.List;

/**
 * PipelineCandidatesGrid.
 */
public class PipelineCandidatesGrid extends PipelineCandidatesBase {
	private final ArrayList <Tuple3 <Integer, ParamInfo, Object[]>> items;
	private final int dim;
	private final int[] counts;

	public PipelineCandidatesGrid(EstimatorBase estimator,
								  ParamGrid paramGrid) {
		super(estimator);

		List <Tuple3 <PipelineStageBase, ParamInfo, Object[]>> grid = paramGrid.getItems();

		this.dim = grid.size();

		PipelineStageBase[] stagesGrid = new PipelineStageBase[this.dim];
		for (int i = 0; i < this.dim; i++) {
			stagesGrid[i] = grid.get(i).f0;
		}
		int[] indices = findStageIndex(this.pipeline, stagesGrid);

		this.items = new ArrayList <>();
		for (int i = 0; i < this.dim; i++) {
			Tuple3 <PipelineStageBase, ParamInfo, Object[]> t3 = grid.get(i);
			this.items.add(new Tuple3 <>(indices[i], t3.f1, t3.f2));
		}

		this.counts = new int[this.dim + 1];
		this.counts[0] = 1;
		for (int i = 0; i < dim; i++) {
			this.counts[i + 1] = this.counts[i] * this.items.get(i).f2.length;
		}
	}

	@Override
	public int size() {
		return this.counts[this.dim];
	}

	@Override
	public Tuple2<Pipeline, List<Tuple3<Integer, ParamInfo, Object>>> get(
		int index, List <Double> experienceScores) throws CloneNotSupportedException {
		ArrayList <Tuple3 <Integer, ParamInfo, Object>> paramList = new ArrayList <>();
		for (int i = this.dim - 1; i >= 0; i--) {
			int k = index / this.counts[i];
			index = index % this.counts[i];
			Tuple3 <Integer, ParamInfo, Object[]> t3 = this.items.get(i);
			paramList.add(new Tuple3 <>(t3.f0, t3.f1, t3.f2[k]));
		}
		Pipeline pipelineClone = this.pipeline.clone();
		updatePipelineParams(pipelineClone, paramList);
		return Tuple2.of(pipelineClone, paramList);
	}

}
