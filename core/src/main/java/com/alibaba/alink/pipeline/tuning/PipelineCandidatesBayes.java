package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.params.tuning.BayesTuningParams.BayesStrategy;
import com.alibaba.alink.pipeline.EstimatorBase;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineStageBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PipelineCandidatesBayes extends PipelineCandidatesBase {
	transient ArrayList <Tuple3 <Integer, ParamInfo, ValueDist>> items;
	final ParamDist paramDist;
	final long seed;
	final int warmUpJobs;
	final int nIter;
	final int sampleNum;
	final int linearForgetting;
	final int dim;
	final BayesStrategy strategy;
	final boolean isLargerBetter;
	Random rand = new Random();
	private final ArrayList <Object[]> records;
	private final ArrayList <DenseVector> recordVectors;

	public PipelineCandidatesBayes(
		EstimatorBase estimator,
		ParamDist paramDist,
		long seed,
		int warmUpJobs,
		int nIter,
		int sampleNum,
		int linearForgetting,
		BayesStrategy strategy,
		boolean isLargerBetter) {
		super(estimator);

		this.paramDist = paramDist;
		this.seed = seed;
		this.warmUpJobs = warmUpJobs;
		this.nIter = nIter;
		this.sampleNum = sampleNum;
		this.linearForgetting = linearForgetting;
		this.strategy = strategy;
		this.isLargerBetter = isLargerBetter;

		this.records = new ArrayList <>(this.nIter);
		this.recordVectors = new ArrayList <>(this.nIter);

		setItems(paramDist);
		List <Tuple3 <PipelineStageBase, ParamInfo, ValueDist>> grid = paramDist.getItems();
		this.dim = grid.size();
	}

	private void setItems(ParamDist paramDist) {
		PipelineStageBase[] stagesGrid = new PipelineStageBase[dim];
		List <Tuple3 <PipelineStageBase, ParamInfo, ValueDist>> grid = paramDist.getItems();
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

	private static List <Double> tpeProcessExperienceScores(List <Double> experienceScores, boolean isLargerBetter) {
		List <Double> processedExperienceScores = new ArrayList <>(experienceScores.size());
		for (double score : experienceScores) {
			if (isLargerBetter) {
				processedExperienceScores.add(-score);
			} else {
				processedExperienceScores.add(score);
			}
		}
		return processedExperienceScores;
	}

	@Override
	public int size() {
		return this.nIter;
	}

	private void fillDoubleArray(Object[] values, double[] doubles) {
		for (int i = 0; i < values.length; i++) {
			doubles[i] = ((Number) values[i]).doubleValue();
		}
	}

	@Override
	public Tuple2 <Pipeline, List <Tuple3 <Integer, ParamInfo, Object>>> get(int index, List <Double> experienceScores)
		throws CloneNotSupportedException {

		ArrayList <Tuple3 <Integer, ParamInfo, Object>> paramList = new ArrayList <>();

		if (items == null || items.size() == 0) {
			setItems(this.paramDist);
		}

		if (index < this.records.size()) {
			Object[] values = this.records.get(index);
			for (int i = 0; i < this.dim; i++) {
				Tuple3 <Integer, ParamInfo, ValueDist> t3 = this.items.get(i);
				paramList.add(new Tuple3 <>(t3.f0, t3.f1, values[i]));
			}
		} else {
			rand.setSeed(this.seed + index * 100000);
			Object[] bestValues = new Object[this.dim];
			double[] doubles = new double[this.dim];

			if (experienceScores.size() < warmUpJobs) {
				for (int i = 0; i < this.dim; i++) {
					bestValues[i] = this.items.get(i).f2.get(rand.nextDouble());
				}
			} else if (strategy.equals(BayesStrategy.GP)) {
				GaussianProcessRegression gpr = new GaussianProcessRegression(this.recordVectors, experienceScores);
				double maxReg = Double.NEGATIVE_INFINITY;
				Object[] values = new Object[this.dim];
				for (int k = 0; k < sampleNum; k++) {
					for (int i = 0; i < this.dim; i++) {
						values[i] = this.items.get(i).f2.get(rand.nextDouble());
					}

					fillDoubleArray(values, doubles);
					double r = 0;
					r = gpr.calc(new DenseVector(doubles)).f0.doubleValue();
					r = isLargerBetter ? r : -r;

					if (r > maxReg) {
						maxReg = r;
						Object[] t = bestValues;
						bestValues = values;
						values = t;
					}
				}
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.printf("GP estimates maximum regression: %f, hyper-parameters values: \n", maxReg);
					for (int i = 0; i < bestValues.length; i++) {
						System.out.printf("%f ", ((Number) bestValues[i]).doubleValue());
					}
					System.out.println();
				}
			} else if (strategy.equals(BayesStrategy.TPE)) {
				List <Double> processedExperienceScores = tpeProcessExperienceScores(experienceScores, isLargerBetter);

				TreeParzenEstimator tpe = new TreeParzenEstimator(this.recordVectors, processedExperienceScores);

				for (int i = 0; i < this.dim; i++) {
					Object[] samplesObject = new Object[sampleNum];
					double[] samples = new double[sampleNum];

					for (int j = 0; j < sampleNum; j++) {
						samplesObject[j] = this.items.get(i).f2.get(rand.nextDouble());
					}

					fillDoubleArray(samplesObject, samples);

					// tuple of mean, sd, lower, upper and interval
					Tuple5 <Double, Double, Double, Double, Integer> summary = ValueDistUtils.getValueDistSummary(
						this.items.get(i).f2);

					bestValues[i] = samplesObject[tpe.calc(new DenseVector(samples), i, summary.f0, summary.f1,
						summary.f2, summary.f3, summary.f4, linearForgetting)];
				}

			}

			this.records.add(bestValues);
			fillDoubleArray(bestValues, doubles);
			this.recordVectors.add(new DenseVector(doubles));

			for (int i = 0; i < this.dim; i++) {
				Tuple3 <Integer, ParamInfo, ValueDist> t3 = this.items.get(i);
				paramList.add(new Tuple3 <>(t3.f0, t3.f1, bestValues[i]));
			}
		}

		Pipeline pipelineClone = new Pipeline();
		for (int i = 0; i < pipeline.size(); i++) {
			try {
				pipelineClone.add(
					pipeline.get(i).getClass().getConstructor(Params.class).newInstance(pipelineAllStageParams.get(i)));
			} catch (Exception e) {}
		}
		updatePipelineParams(pipelineClone, paramList);
		return Tuple2.of(pipelineClone, paramList);
	}

	public void checkParamsSameValueOrNullValue(int index, List <Tuple3 <Integer, ParamInfo, Object>> params) {
		if (items == null || items.size() == 0) {
			setItems(this.paramDist);
		}

		if (index < this.records.size() && this.records.get(index).length > 0) {
			Object[] values = this.records.get(index);
			for (int i = 0; i < values.length; i++) {
				double v1 = ((Number) values[i]).doubleValue();
				double v2 = ((Number) params.get(i).f2).doubleValue();
				if (Math.abs(v1 - v2) > 1e-9) {
					throw new RuntimeException("checkParamsSameValueOrNullValue failed.");
				}
			}
		} else {
			Object[] values = new Object[params.size()];
			for (int i = 0; i < params.size(); i++) {
				values[i] = params.get(i).f2;
			}

			while (records.size() <= index) {
				records.add(new Object[] {});
			}

			records.set(index, values);
		}
	}

}
