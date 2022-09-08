package com.alibaba.alink.operator.batch.huge.line;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.common.io.directreader.DistributedInfo;
import com.alibaba.alink.operator.common.aps.ApsContext;
import com.alibaba.alink.operator.common.aps.ApsFuncTrain;
import com.alibaba.alink.params.graph.LineParams;
import com.alibaba.alink.params.nlp.HasNegative;
import com.alibaba.alink.params.shared.HasVectorSizeDefaultAs100;

import java.util.List;
import java.util.Map;

public class ApsFuncTrainLine extends ApsFuncTrain <Number[], float[][]> {
	private static final long serialVersionUID = -2210479949975637732L;
	private final Params params;

	ApsFuncTrainLine(Params params) {
		this.params = params;
	}

	@Override
	protected List <Tuple2 <Long, float[][]>> train(List <Tuple2 <Long, float[][]>> relatedFeatures,
													Map <Long, Integer> mapFeatureId2Local,
													List <Number[]> trainData) throws Exception {
		if (null != this.contextParams) {
			Long[] nsPool = this.contextParams.getLongArray("negBound");
			int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
			Long[] seeds = this.contextParams.get(ApsContext.SEEDS);
			int seed = seeds[subTaskId].intValue();
			int negaTime = params.get(HasNegative.NEGATIVE);
			int threadNum = params.getIntegerOrDefault("threadNum", 8);
			double sampleRatioPerPartition = params.get(LineParams.SAMPLE_RATIO_PER_PARTITION);
			double leaningRate = params.get(LineParams.RHO);
			int vectorSize = params.get(HasVectorSizeDefaultAs100.VECTOR_SIZE);
			int order = params.get(LineParams.ORDER).getValue();
			double minRhoRate = params.get(LineParams.MIN_RHO_RATE);
			int modelLength = relatedFeatures.size();
			float[] valueBuffer = new float[modelLength * vectorSize];
			//just initialize.
			float[] contextBuffer = new float[0];
			if (order == 1) {
				for (int i = 0; i < modelLength; i++) {
					System.arraycopy(relatedFeatures.get(i).f1[0], 0,
						valueBuffer, i * vectorSize, vectorSize);
				}
			} else {
				contextBuffer = new float[modelLength * vectorSize];
				for (int i = 0; i < modelLength; i++) {
					System.arraycopy(relatedFeatures.get(i).f1[0], 0,
						valueBuffer, i * vectorSize, vectorSize);
					System.arraycopy(relatedFeatures.get(i).f1[1], 0,
						contextBuffer, i * vectorSize, vectorSize);
				}
			}
			TrainSubSet trainSubSet = new TrainSubSet(nsPool, negaTime, sampleRatioPerPartition, leaningRate,
				minRhoRate, vectorSize, threadNum);
			trainSubSet.train(seed, trainData, order, valueBuffer, contextBuffer,
				mapFeatureId2Local, getIterationRuntimeContext().getSuperstepNumber());
			if (order == 1) {
				for (int i = 0; i < modelLength; i++) {
					float[] valueOrigin = relatedFeatures.get(i).f1[0];
					for (int j = 0; j < vectorSize; j++) {
						valueOrigin[j] = valueBuffer[i * vectorSize + j] - valueOrigin[j];
					}
				}
			} else {
				for (int i = 0; i < modelLength; i++) {
					float[] valueOrigin = relatedFeatures.get(i).f1[0];
					float[] contextOrigin = relatedFeatures.get(i).f1[1];
					for (int j = 0; j < vectorSize; j++) {
						valueOrigin[j] = valueBuffer[i * vectorSize + j] - valueOrigin[j];
						contextOrigin[j] = contextBuffer[i * vectorSize + j] - contextOrigin[j];
					}
				}
			}
			return relatedFeatures;
		} else {
			throw new AkUnclassifiedErrorException("Aps server meets RuntimeException when training");
		}
	}

	public static class TrainSubSet {
		private final Long[] nsPool;
		int negaTime;
		double sampleRatioPerPartition;
		double leaningRate;
		double minRhoRate;
		int vectorSize;
		int threadNum;

		TrainSubSet(Long[] nsPool, int negaTime, double sampleRatioPerPartition,
					double leaningRate, double minRhoRate, int vectorSize, int threadNum) {
			this.nsPool = nsPool;
			this.negaTime = negaTime;
			this.sampleRatioPerPartition = sampleRatioPerPartition;
			this.leaningRate = leaningRate;
			this.minRhoRate = minRhoRate;
			this.vectorSize = vectorSize;
			this.threadNum = threadNum;
		}

		public void train(int seed, List <Number[]> trainData, int order,
						  float[] buffer, float[] contextBuffer,
						  Map <Long, Integer> mapFeatureId2Local, int iterNum) throws InterruptedException {
			Thread[] thread = new Thread[threadNum];
			DistributedInfo distributedInfo = new DefaultDistributedInfo();
			for (int i = 0; i < threadNum; ++i) {
				int start = (int) distributedInfo.startPos(i, threadNum, trainData.size());
				int end = (int) distributedInfo.localRowCnt(i, threadNum, trainData.size()) + start;
				thread[i] = new TrainRunner(nsPool, seed + i, vectorSize, order,
					negaTime, sampleRatioPerPartition, leaningRate, minRhoRate,
					buffer, contextBuffer, mapFeatureId2Local, trainData.subList(start, end));
				thread[i].start();
			}

			for (int i = 0; i < threadNum; ++i) {
				thread[i].join();
			}
		}
	}

	private static class TrainRunner extends Thread {
		private final Long[] nsPool;
		List <Number[]> edges;
		int seed;
		int negaTime;
		double sampleRatioPerPartition;
		double leaningRate;
		double minRhoRate;
		int vectorSize;
		boolean isOrderOne;
		float[] valueBuffer;
		float[] contextBuffer;
		Map <Long, Integer> modelMapper;

		TrainRunner(Long[] nsPool, int seed, int vectorSize, int order,
					int negaTime, double sampleRatioPerPartition, double leaningRate, double minRhoRate,
					float[] valueBuffer, float[] contextBuffer,
					Map <Long, Integer> mapFeatureId2Local,
					List <Number[]> trainData) {
			this.nsPool = nsPool;
			this.edges = trainData;
			this.seed = seed;
			this.negaTime = negaTime;
			this.sampleRatioPerPartition = sampleRatioPerPartition;
			this.leaningRate = leaningRate;
			this.minRhoRate = minRhoRate;
			this.vectorSize = vectorSize;
			this.valueBuffer = valueBuffer;
			this.contextBuffer = contextBuffer;
			this.modelMapper = mapFeatureId2Local;
			this.isOrderOne = order == 1;
		}

		@Override
		public void run() {
			LinePullAndTrainOperation trainOperation = new LinePullAndTrainOperation(negaTime, nsPool);
			trainOperation.train(seed, leaningRate, minRhoRate, isOrderOne, vectorSize, sampleRatioPerPartition,
				valueBuffer, contextBuffer, modelMapper, edges);
		}
	}

}
