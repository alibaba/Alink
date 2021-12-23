package com.alibaba.alink.operator.batch.huge.line;

import org.apache.flink.api.java.tuple.Tuple2;
import com.alibaba.alink.common.utils.ExpTableArray;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class LinePullAndTrainOperation {
	private final int negaTime;
	private final Long[] nsPool;

	public LinePullAndTrainOperation(int negaTime, Long[] pool) {
		this.negaTime = negaTime;
		this.nsPool = pool;
	}

	public static long negativeSampling(Random random, Long[] bound) {
		int start = 0;
		int end = bound.length - 1;
		double multi = random.nextDouble() * end;
		double floor = Math.floor(multi);
		double minus = multi - floor;
		int pos = start + (int) floor;
		return ((Math.round((bound[pos + 1] - bound[pos]) * minus) + bound[pos]));
	}

	public void getIndexes(int seed, double sampleRatioPerPartition, List <Number[]> content, Set <Long> output) {
		Random random = new Random();
		random.setSeed(seed);
		int edgeSize = content.size();
		long[][] edges = new long[edgeSize][2];
		double[] weights = new double[edgeSize];
		int count = 0;
		for (Number[] item : content) {
			long[] edge = new long[2];
			edge[0] = (long) item[0];
			edge[1] = (long) item[1];
			edges[count] = edge;
			weights[count] = Float.valueOf((float) item[2]).doubleValue();
			++count;
		}
		AliasSampling edgeAliasSampling = new AliasSampling(weights, seed);
		int runTime = (int) Math.round(sampleRatioPerPartition * edgeSize);
		for (int i = 0; i < runTime; i++) {
			int index = edgeAliasSampling.sampling();
			long source = edges[index][0];
			long target = edges[index][1];
			output.add(source);
			output.add(target);
			count = 0;
			while (count < negaTime) {
				target = negativeSampling(random, nsPool);
				output.add(target);
				++count;
			}
		}
	}

	public void train(int seed, double learningRate, double minRhoRate, boolean isOrderOne,
					  int vectorSize, double sampleRatioPerPartition,
					  float[] valueBuffer, float[] contextBuffer,
					  Map <Long, Integer> modelMapper, List <Number[]> trainData) {
		Random random = new Random();
		random.setSeed(seed);
		int edgeSize = trainData.size();
		long[][] edges = new long[edgeSize][2];
		double[] weights = new double[edgeSize];
		int count = 0;
		for (Number[] item : trainData) {
			long[] edge = new long[2];
			edge[0] = (long) item[0];
			edge[1] = (long) item[1];
			edges[count] = edge;
			weights[count] = item[2].doubleValue();
			++count;
		}
		AliasSampling aliasEdge = new AliasSampling(weights, seed);
		float[] vectorError = new float[vectorSize];
		int runTime = (int) Math.round(sampleRatioPerPartition * edgeSize);
		double[] loss = new double[] {0};
		if (isOrderOne) {
			for (int i = 0; i < runTime; ++i) {
				double curRate = learningRate * Math.max((1 - i * 1.0 / runTime), minRhoRate);
				Arrays.fill(vectorError, 0);
				//sample due to the weight of edge.
				int index = aliasEdge.sampling();
				long source = edges[index][0];
				float[] sourceVector = getVec(valueBuffer, modelMapper, source, vectorSize);
				long target = edges[index][1];
				float[] targetVector = getVec(valueBuffer, modelMapper, target, vectorSize);
				update(sourceVector, targetVector, vectorError, 1, curRate, loss);
				setVec(valueBuffer, targetVector, modelMapper.get(target), vectorSize);
				count = 0;
				while (count < negaTime) {
					target = negativeSampling(random, nsPool);
					targetVector = getVec(valueBuffer, modelMapper, target, vectorSize);
					update(sourceVector, targetVector, vectorError, 0, curRate, loss);
					setVec(valueBuffer, targetVector, modelMapper.get(target), vectorSize);
					count++;
				}
				for (int j = 0; j < vectorSize; ++j) {
					sourceVector[j] += vectorError[j];
				}
				setVec(valueBuffer, sourceVector, modelMapper.get(source), vectorSize);
			}
		} else {
			for (int i = 0; i < runTime; ++i) {
				double curRate = learningRate * Math.max((1 - i * 1.0 / runTime), minRhoRate);
				Arrays.fill(vectorError, 0);
				int index = aliasEdge.sampling();
				long source = edges[index][0];
				float[] sourceVector = getVec(valueBuffer, modelMapper, source, vectorSize);
				long target = edges[index][1];
				float[] targetContextVector = getVec(contextBuffer, modelMapper, target, vectorSize);
				update(sourceVector, targetContextVector, vectorError, 1, curRate, loss);
				setVec(contextBuffer, targetContextVector, modelMapper.get(target), vectorSize);
				count = 0;
				while (count < negaTime) {
					target = negativeSampling(random, nsPool);
					targetContextVector = getVec(contextBuffer, modelMapper, target, vectorSize);
					update(sourceVector, targetContextVector, vectorError, 0, curRate, loss);
					setVec(contextBuffer, targetContextVector, modelMapper.get(target), vectorSize);
					count++;
				}
				for (int j = 0; j < vectorSize; ++j) {
					sourceVector[j] += vectorError[j];
				}
				setVec(valueBuffer, sourceVector, modelMapper.get(source), vectorSize);
			}
		}
	}

	private static float[] getVec(float[] buffer, Map <Long, Integer> modelMapper, long vertex, int modelVectorSize) {
		int index = modelMapper.get(vertex);
		float[] res = new float[modelVectorSize];
		System.arraycopy(buffer, index * modelVectorSize, res, 0, modelVectorSize);
		return res;
	}

	private static void setVec(float[] buffer, float[] vector, int index, int modelVectorSize) {
		System.arraycopy(vector, 0, buffer, index * modelVectorSize, modelVectorSize);
	}

	protected static void update(float[] vecU, float[] vecV, float[] vecError, int label,
								 double rho, double[] loss) {
		float x = floatDot(vecU, vecV);
		float prob = ExpTableArray.sigmoid(x);
		if (label == 1) {
			loss[0] -= ExpTableArray.log(prob);
		} else {
			loss[0] -= ExpTableArray.log(1 - prob);
		}
		float g = (float) ((label - prob) * rho);
		floatAxpy(g, vecV, vecError);
		floatAxpy(g, vecU, vecV);
	}

	//for float
	public static float floatDot(float[] x, float[] y) {
		float s = 0;
		for (int i = 0; i < x.length; ++i) {
			s += x[i] * y[i];
		}
		return s;
	}

	public static void floatAxpy(float a, float[] x, float[] y) {
		int length = x.length;
		for (int i = 0; i < length; ++i) {
			y[i] += a * x[i];
		}
	}

	public static class AliasSampling {

		private final int[] alias;
		private final double[] prob;
		private final Random rand;
		private final int weightSize;

		public AliasSampling(double[] weights, int seed) {
			Tuple2 <int[], double[]> res = initAlias(weights);
			alias = res.f0;
			prob = res.f1;
			rand = new Random(seed);
			weightSize = weights.length;
		}

		static Tuple2 <int[], double[]> initAlias(double[] weights) {
			int num = weights.length;
			int[] alias = new int[num];
			double[] prob = new double[num];
			double[] normProb = new double[num];
			int[] largeBlock = new int[num];
			int[] smallBlock = new int[num];
			double sum = 0;
			for (double weight : weights) {
				sum += weight;
			}
			int numLargeBlock = 0;
			int numSmallBlock = 0;
			int curLargeBlock;
			int curSmallBlock;
			for (int i = 0; i < num; i++) {
				normProb[i] = weights[i] * num / sum;
			}
			for (int k = num - 1; k >= 0; k--) {
				if (normProb[k] < 1) {
					smallBlock[numSmallBlock++] = k;
				} else {
					largeBlock[numLargeBlock++] = k;
				}
			}
			while (numSmallBlock > 0 && numLargeBlock > 0) {
				curSmallBlock = smallBlock[--numSmallBlock];
				curLargeBlock = largeBlock[--numLargeBlock];
				prob[curSmallBlock] = normProb[curSmallBlock];
				alias[curSmallBlock] = curLargeBlock;
				normProb[curLargeBlock] = normProb[curLargeBlock] + normProb[curSmallBlock] - 1;
				if (normProb[curLargeBlock] < 1) {
					smallBlock[numSmallBlock++] = curLargeBlock;
				} else {
					largeBlock[numLargeBlock++] = curLargeBlock;
				}
			}
			while (numLargeBlock != 0) { prob[largeBlock[--numLargeBlock]] = 1;}
			while (numSmallBlock != 0) {prob[smallBlock[--numSmallBlock]] = 1;}
			return Tuple2.of(alias, prob);
		}

		public int sampling() {
			int k = (int) Math.floor(weightSize * rand.nextDouble());
			return rand.nextDouble() < prob[k] ? k : alias[k];
		}
	}
}
