package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.operator.common.tree.Criteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

public class CalBestSplit extends ComputeFunction {

	private static final Logger LOG = LoggerFactory.getLogger(CalBestSplit.class);
	private int binNum;
	private int featureColNum;
	private int minSamplesPerLeaf;
	private double minSumHessianPerLeaf;
	private double minInfoGain;
	private double learningRate;
	private int[] categoryDetailHashMap;
	private boolean[] featureCut;
	private int[] leftSubset;
	private float[] newBinForCategory;
	private Integer[] compareIndex;
	private float[] compareValue;
	private float[] reducedBin;

	public CalBestSplit(int binNum, int featureColNum, int minSamplesPerLeaf, double minSumHessianPerLeaf,
						double minInfoGain, double learningRate) {
		this.binNum = binNum;
		this.featureColNum = featureColNum;
		this.minSamplesPerLeaf = minSamplesPerLeaf;
		this.minSumHessianPerLeaf = minSumHessianPerLeaf;
		this.minInfoGain = minInfoGain;
		this.learningRate = learningRate;

		categoryDetailHashMap = new int[featureColNum + 1];
		featureCut = new boolean[featureColNum];
		leftSubset = new int[binNum];
		newBinForCategory = new float[binNum * 4];
		compareIndex = new Integer[binNum];
		compareValue = new float[binNum];
		reducedBin = new float[0];
	}

	@Override
	public void calc(ComContext context) {
		LOG.info(Thread.currentThread().getName(), "open");

		if (context.getStepNo() == 1) {
			//store category detail
			//only 1 variable
			for (int feature_index = 0; feature_index < featureColNum; feature_index++) {
				categoryDetailHashMap[feature_index] = 0;
			}

			Iterable<Tuple2<Long, int[][]>> categoryDetail = context.getObj("categoryDetail");
			Iterator<Tuple2<Long, int[][]>> categoryDetailIter = categoryDetail.iterator();
			if (categoryDetailIter.hasNext()) {
				int[][] curPara = categoryDetailIter.next().f1;

				for (int index = 0; index < curPara.length; index++) {
					// <0 means dummy, no need to insert
					if (curPara[index][0] >= 0) {
						categoryDetailHashMap[curPara[index][0]] = curPara[index][1];
					}
				}
			}
		}

		//input data
		//1 worker, 1 line of data

		double[] doubleReducedBin = context.getObj("gbdtBin");
		if (reducedBin.length < doubleReducedBin.length) {
			reducedBin = new float[doubleReducedBin.length];
		}
		for (int index = 0; index < doubleReducedBin.length; index++) {
			reducedBin[index] = ((Double) doubleReducedBin[index]).floatValue();
		}

		for (int featureIndex = 0; featureIndex < featureColNum; featureIndex++) {
			featureCut[featureIndex] = true;
		}

		int maximumNumNode = reducedBin.length / featureColNum / binNum / 4;

		SplitResult[] allResult;
		if (context.getStepNo() == 1) {
			allResult = new SplitResult[maximumNumNode];
			context.putObj("splitResult", allResult);
		} else {
			allResult = context.getObj("splitResult");
		}

		int validMaximumNodeIndex = 0;
		for (int nodeIndex = 0; nodeIndex < maximumNumNode; nodeIndex++) {
			Criteria.MSE totalCounter = null;
			int splitFeature = -2;
			int splitPoint = -3;
			double bestGain = minInfoGain;
			Criteria.MSE bestLeftCounter = new Criteria.MSE(0, 0, 0, 0);
			Criteria.MSE bestRightCounter = new Criteria.MSE(0, 0, 0, 0);
			int numInstanceCount = 0;
			for (int index = 0; index < binNum; index++) {
				leftSubset[index] = -1;
			}

			for (int featureIndex = 0; featureIndex < featureColNum; featureIndex++) {
				int startCounter = nodeIndex * featureColNum * binNum + featureIndex * binNum;

				// 1. check if category feature
				int categorySize = 0;
				if (categoryDetailHashMap[featureIndex] > 0) {
					categorySize = categoryDetailHashMap[featureIndex];
				}

				// 2. if category feature, sort feature value
				if (categorySize > 0) {
					//get g/h
					for (int binIndex = 0; binIndex < categorySize; binIndex++) {
						// "+1"==sum of gradient, "+2"==sum of hessian
						compareValue[binIndex] =
							reducedBin[(startCounter + binIndex) * 4 + 1]
								/ reducedBin[(startCounter + binIndex) * 4 + 2];
						if (reducedBin[(startCounter + binIndex) * 4 + 2] < 1e-6) {
							compareValue[binIndex] = -1;
						}

						compareIndex[binIndex] = binIndex;
					}

					//sort
					Arrays.sort(compareIndex, 0, categorySize, new CategoryFeatureComparator(compareValue));

					for (int binIndex = 0; binIndex < categorySize; binIndex++) {
						int position = (startCounter + compareIndex[binIndex]) * 4;
						newBinForCategory[4 * binIndex] = reducedBin[position];
						newBinForCategory[4 * binIndex + 1] = reducedBin[position + 1];
						newBinForCategory[4 * binIndex + 2] = reducedBin[position + 2];
						newBinForCategory[4 * binIndex + 3] = reducedBin[position + 3];
					}

					for (int binIndex = categorySize; binIndex < binNum; binIndex++) {
						newBinForCategory[4 * binIndex] = 0;
						newBinForCategory[4 * binIndex + 1] = 0;
						newBinForCategory[4 * binIndex + 2] = 0;
						newBinForCategory[4 * binIndex + 3] = 0;
					}
				}

				//3. total sum
				double totalGradient = 0;
				double totalHessian = 0;
				int totalWeight = 0;
				totalCounter = null;
				for (int bin_index = 0; bin_index < binNum; bin_index++) {
					Criteria.MSE cur;
					if (categorySize <= 0) {
						cur = new Criteria.MSE(
							reducedBin[(startCounter + bin_index) * 4 + 2],
							(int) reducedBin[(startCounter + bin_index) * 4 + 3],
							reducedBin[(startCounter + bin_index) * 4 + 1],
							reducedBin[(startCounter + bin_index) * 4]
						);
					} else {
						cur = new Criteria.MSE(
							newBinForCategory[bin_index * 4 + 2],
							(int) newBinForCategory[bin_index * 4 + 3],
							newBinForCategory[bin_index * 4 + 1],
							newBinForCategory[bin_index * 4]
						);
					}
					totalGradient += cur.getSum();
					totalHessian += cur.getWeightSum();

					numInstanceCount += cur.getNumInstances();
					totalWeight += cur.getNumInstances();

					if (totalCounter == null) {
						totalCounter = (Criteria.MSE) cur.clone();
					} else {
						totalCounter.add(cur);
					}
				}

				//update default left counter
				if (bestLeftCounter.getNumInstances() == 0) {
					bestLeftCounter = totalCounter;
				}

				//no data, no need for split
				if (totalHessian < 1e-6) {
					continue;
				}

				//4. iterator for best split
				double leftGradient = 0;
				double leftHessian = 0;
				int leftWeight = 0;
				Criteria.MSE leftCounter = new Criteria.MSE(0, 0, 0, 0);
				Criteria.MSE rightCounter = (Criteria.MSE) totalCounter.clone();

				for (int binIndex = 0; binIndex < binNum; binIndex++) {
					Criteria.MSE cur;
					if (categorySize <= 0) {
						cur = new Criteria.MSE(
							reducedBin[(startCounter + binIndex) * 4 + 2],
							(int) reducedBin[(startCounter + binIndex) * 4 + 3],
							reducedBin[(startCounter + binIndex) * 4 + 1],
							reducedBin[(startCounter + binIndex) * 4]);
					} else {
						cur = new Criteria.MSE(
							newBinForCategory[binIndex * 4 + 2],
							(int) newBinForCategory[binIndex * 4 + 3],
							newBinForCategory[binIndex * 4 + 1],
							newBinForCategory[binIndex * 4]
						);
					}
					leftGradient += cur.getSum();
					leftHessian += cur.getWeightSum();
					leftWeight += cur.getNumInstances();

					leftCounter.add(cur);
					rightCounter.subtract(cur);

					//deal with corner case
					if (leftHessian / totalHessian < 1e-7
						|| leftHessian / totalHessian > (1.0 - 1e-7)) {
						continue;
					}

					//min samples per leaf
					if (leftWeight < minSamplesPerLeaf || totalWeight - leftWeight < minSamplesPerLeaf
						|| leftHessian < minSumHessianPerLeaf || totalHessian - leftHessian < minSumHessianPerLeaf) {
						continue;
					}

					double rightGradient = totalGradient - leftGradient;
					double rightHessian = totalHessian - leftHessian;

					//split formula here
					double gain;

					//special case
					if (leftHessian == 0 || rightHessian == 0) {
						gain = 0.0;
					} else {
						gain = Math.abs(
							leftGradient * leftGradient / leftHessian
								+ rightGradient * rightGradient / rightHessian
								- totalGradient * totalGradient / totalHessian);
					}

					//new best, update result
					if (gain > bestGain + 1e-6) {
						bestGain = gain;
						splitFeature = featureIndex;
						splitPoint = binIndex;
						bestLeftCounter = (Criteria.MSE) leftCounter.clone();
						bestRightCounter = (Criteria.MSE) rightCounter.clone();

						//left subset
						if (categorySize > 0) {
							for (int index = 0; index < binNum; index++) {
								leftSubset[index] = 0;
							}
							for (int leftIndex = 0; leftIndex <= binIndex; leftIndex++) {
								leftSubset[compareIndex[leftIndex]] = 1;
							}
						} else {
							for (int index = 0; index < binNum; index++) {
								leftSubset[index] = -1;
							}
						}

						//special case
						if (leftHessian == 0) {
							splitPoint = binNum - 1;
							bestLeftCounter = (Criteria.MSE) totalCounter.clone();
							bestRightCounter = new Criteria.MSE(0, 0, 0, 0);
						}
					}
				}
			}

			//set result split
			SplitResult splitResult = new SplitResult();
			splitResult.setSplitFeature(splitFeature);
			splitResult.setSplitPoint(splitPoint);
			if (splitFeature >= 0 && leftSubset[0] >= 0) {
				boolean[] realLeftSubset = new boolean[categoryDetailHashMap[splitFeature]];
				for (int index = 0; index < realLeftSubset.length; index++) {
					realLeftSubset[index] = (leftSubset[index] != 0);
				}
				splitResult.setLeftSubset(realLeftSubset);
			} else {
				splitResult.setLeftSubset(new boolean[0]);
			}

			LOG.info("tree_index " + nodeIndex + " numInstanceCount " + (numInstanceCount / featureColNum) +
				" best_split_feature " + splitFeature + " best_split_point_right " + splitPoint + " best_loss " +
				bestGain);
			LOG.info("total counter: sum " + totalCounter.getSum()
				+ " weight sum " + totalCounter.getWeightSum() + " number sum " + totalCounter.getNumInstances());
			LOG.info("left counter: sum " + bestLeftCounter.getSum()
				+ " weight sum " + bestLeftCounter.getWeightSum() + " number sum " + bestLeftCounter.getNumInstances());
			LOG.info("right counter: sum " + bestRightCounter.getSum()
				+ " weight sum " + bestRightCounter.getWeightSum() + " number sum " + bestRightCounter.getNumInstances());

			//learning rate
			bestLeftCounter.setSum(-bestLeftCounter.getSum() * learningRate);
			bestRightCounter.setSum(-bestRightCounter.getSum() * learningRate);

			splitResult.setLeftCounter(bestLeftCounter);
			splitResult.setRightCounter(bestRightCounter);
			allResult[nodeIndex] = splitResult;

			//max_tree_id
			if (numInstanceCount > 0) {
				validMaximumNodeIndex = nodeIndex;
			}

		}
		//re-calculate max_tree_id (in case of incomplete binary tree)
		if (validMaximumNodeIndex > 0) {
			validMaximumNodeIndex = (1 << (TreeParaContainer.TreeParas.depth(validMaximumNodeIndex - 1))) - 1;
		}

		for (int nodeIndex = 0; nodeIndex < maximumNumNode; nodeIndex++) {
			allResult[nodeIndex].setMaxTreeId(validMaximumNodeIndex);
			allResult[nodeIndex].setFeatureCut(featureCut);
		}

		LOG.info(Thread.currentThread().getName(), "close");
	}

	public static class CategoryFeatureComparator implements Comparator<Integer> {
		float[] value;

		CategoryFeatureComparator(float[] value) {
			this.value = value;
		}

		@Override
		public int compare(Integer a, Integer b) {
			//float value comparison
			if (value[a] < value[b] - 1e-6) {
				return -1;
			}
			if (value[a] < value[b] + 1e-6) {
				return 0;
			}
			return 1;
		}
	}

}

