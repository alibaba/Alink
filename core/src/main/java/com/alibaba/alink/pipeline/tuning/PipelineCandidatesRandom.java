package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;

import com.alibaba.alink.pipeline.EstimatorBase;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineStageBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PipelineCandidatesRandom extends PipelineCandidatesBase {
	final ArrayList <Tuple3 <Integer, ParamInfo, ValueDist>> items;
	final long seed;
	final int nIter;
	Random rand = new Random();

	public PipelineCandidatesRandom(EstimatorBase estimator,
									ParamDist paramDist, long seed, int nIter) {
		super(estimator);

		this.seed = seed;
		this.nIter = nIter;

		List <Tuple3 <PipelineStageBase, ParamInfo, ValueDist>> grid = paramDist.getItems();

		int dim = grid.size();

		PipelineStageBase[] stagesGrid = new PipelineStageBase[dim];
		for (int i = 0; i < dim; i++) {
			stagesGrid[i] = grid.get(i).f0;
		}
		int[] indices = findStageIndex(this.pipeline, stagesGrid);

		this.items = new ArrayList <>();
		for (int i = 0; i < dim; i++) {
			Tuple3 <PipelineStageBase, ParamInfo, ValueDist> t3 = grid.get(i);
			this.items.add(new Tuple3 <>(indices[i], t3.f1, t3.f2));
		}
	}

	@Override
	public int size() {
		return this.nIter;
	}

	@Override
	public Tuple2 <Pipeline, List <Tuple3 <Integer, ParamInfo, Object>>>
	get(int index, List <Double> experienceScores) throws CloneNotSupportedException {
		ArrayList <Tuple3 <Integer, ParamInfo, Object>> paramList = new ArrayList <>();
		rand.setSeed(this.seed + index * 100000);
		for (Tuple3 <Integer, ParamInfo, ValueDist> t3 : this.items) {
			paramList.add(new Tuple3 <>(t3.f0, t3.f1, t3.f2.get(rand.nextDouble())));
		}
		Pipeline pipelineClone = this.pipeline.clone();
		updatePipelineParams(pipelineClone, paramList);
		return Tuple2.of(pipelineClone, paramList);
	}

}
