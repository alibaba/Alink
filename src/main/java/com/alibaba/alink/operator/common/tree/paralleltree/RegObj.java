package com.alibaba.alink.operator.common.tree.paralleltree;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.LabelCounter;
import com.alibaba.alink.operator.common.tree.Node;

import java.util.Arrays;

public class RegObj extends TreeObj<double[]> {
	private double[] labels;

	public RegObj(
		Params params,
		QuantileDiscretizerModelDataConverter qModel,
		FeatureMeta[] featureMetas,
		FeatureMeta lableMeta) {
		super(params, qModel, featureMetas, lableMeta);

		initBuffer();
	}

	@Override
	public void stat(int loop, int start, int end, int bufStart, int bufEnd) {
		int f0, f1, h, p;
		double l;

		Arrays.fill(hist, bufStart, bufEnd, 0.);

		if (baggingFeatureCount() != nFeatureCol) {
			int[] baggingFeature = loopBuffer[loop].baggingFeatures;
			for (int i = 0; i < baggingFeatureCount(); ++i) {
				f0 = baggingFeature[i] * numLocalRow;
				f1 = i * 3 * nBin + bufStart;
				for (int j = start; j < end; ++j) {
					p = partitions[j];
					h = f1 + features[f0 + p] * 3;
					l = labels[p];
					hist[h] += l;
					hist[h + 1] += l * l;
					hist[h + 2] += 1.;
				}
			}
		} else {
			for (int i = 0; i < nFeatureCol; ++i) {
				f0 = i * numLocalRow;
				f1 = i * 3 * nBin + bufStart;
				for (int j = start; j < end; ++j) {
					p = partitions[j];
					h = f1 + features[f0 + p] * 3;
					l = labels[p];
					hist[h] += l;
					hist[h + 1] += l * l;
					hist[h + 2] += 1.;
				}
			}
		}
	}

	@Override
	public final int lenPerStat() {
		if (baggingFeatureCount() != nFeatureCol) {
			return baggingFeatureCount() * 3 * nBin;
		} else {
			return nFeatureCol * 3 * nBin;
		}
	}

	@Override
	public void setLabels(double[] values) {
		labels = values;
	}

	public final Tuple2<int[], Double> bestSplitCategorical(
		int featureStart,
		double mse,
		double totalSquareSum,
		double totalSum,
		double totalWeight,
		int categoricalSize) {

		double bestGain = 0.;
		int[] bestSplit = new int[categoricalSize];
		int[] splitBuf = new int[categoricalSize];

		double leftSum;
		double leftSquareSum;
		double leftWeight;

		double rightSum;
		double rightSquareSum;
		double rightWeight;

		int n = 1 << (categoricalSize - 1);
		for (int i = 1; i < n; ++i) {
			leftSum = 0.;
			leftSquareSum = 0.;
			leftWeight = 0.;

			rightSum = 0.;
			rightSquareSum = 0.;
			rightWeight = 0.;

			for (int j = 0; j < categoricalSize; ++j) {
				splitBuf[j] = -1;

				if ((i & (1 << j)) != 0) {
					splitBuf[j] = 0;
					int binStart = featureStart + j * 3;
					leftSum += minusHist[binStart];
					leftSquareSum += minusHist[binStart + 1];
					leftWeight += minusHist[binStart + 2];
				} else {
					splitBuf[j] = 1;
					int binStart = featureStart + j * 3;
					rightSum += minusHist[binStart];
					rightSquareSum += minusHist[binStart + 1];
					rightWeight += minusHist[binStart + 2];
				}
			}

			if (minSamplesPerLeaf > leftWeight
				|| minSamplesPerLeaf > rightWeight) {
				continue;
			}

			double leftMean = leftSum / leftWeight;
			double leftMse = leftSquareSum / leftWeight - leftMean * leftMean;

			double rightMean = rightSum / rightWeight;
			double rightMse = rightSquareSum / rightWeight - rightMean * rightMean;

			double curGain = mse - leftWeight / totalWeight * leftMse - rightWeight / totalWeight * rightMse;
			if (curGain > bestGain) {
				bestGain = curGain;
				System.arraycopy(splitBuf, 0, bestSplit, 0, categoricalSize);
			}
		}

		return Tuple2.of(bestSplit, bestGain);
	}

	public final Tuple2<Integer, Double> bestSplitNumerical(
		int fStart,
		double mse,
		double totalSquareSum,
		double totalSum,
		double totalWeight) {

		double bestGain = 0.;
		int bestSplit = 0;

		double leftSum = 0.;
		double leftSquareSum = 0.;
		double leftWeight = 0.;

		double rightSum = totalSum;
		double rightSquareSum = totalSquareSum;
		double rightWeight = totalWeight;

		for (int z = 0; z < nBin - 1; ++z) {
			int binStart = fStart + z * 3;
			leftSum += minusHist[binStart];
			leftSquareSum += minusHist[binStart + 1];
			leftWeight += minusHist[binStart + 2];

			rightSum -= minusHist[binStart];
			rightSquareSum -= minusHist[binStart + 1];
			rightWeight -= minusHist[binStart + 2];

			if (minSamplesPerLeaf > leftWeight
				|| minSamplesPerLeaf > rightWeight) {
				continue;
			}

			double leftMean = leftSum / leftWeight;
			double leftMse = leftSquareSum / leftWeight - leftMean * leftMean;

			double rightMean = rightSum / rightWeight;
			double rightMse = rightSquareSum / rightWeight - rightMean * rightMean;

			double curGain = mse - leftWeight / totalWeight * leftMse - rightWeight / totalWeight * rightMse;

			if (curGain > bestGain) {
				bestGain = curGain;
				bestSplit = z;
			}
		}

		return Tuple2.of(bestSplit, bestGain);
	}

	@Override
	public final void bestSplit(Node node, int minusId, NodeInfoPair pair) {
		int start = minusId * lenPerStat();

		double gBestGain = 0.;
		int gBestSplit = 0;
		int[] gBestSplitCategorical = null;
		int gBestFeature = -1;

		double sum = 0.;
		double squareSum = 0.;
		double weight = 0.;

		// stat the first feature
		for (int z = 0; z < nBin; ++z) {
			int zStart = start + z * 3;
			sum += minusHist[zStart];
			squareSum += minusHist[zStart + 1];
			weight += minusHist[zStart + 2];
		}

		node.setCounter(new LabelCounter(weight, 0, new double[]{sum, squareSum}));

		if (maxDepth < pair.depth
			|| minSamplesPerLeaf > weight) {
			node.makeLeaf();
			return;
		}

		double mean = sum / weight;
		double mse = squareSum / weight - mean * mean;

		if (baggingFeatureCount() != nFeatureCol) {
			int baggingFeatureCount = baggingFeatureCount();
			for (int j = 0; j < baggingFeatureCount; ++j) {
				int fStart = start + j * nBin * 3;
				if (featureMetas[pair.baggingFeatures[j]].getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
					Tuple2<Integer, Double> gain = bestSplitNumerical(fStart, mse, squareSum, sum, weight);
					if (gain.f1 > gBestGain) {
						gBestGain = gain.f1;
						gBestSplit = gain.f0;
						gBestFeature = pair.baggingFeatures[j];
					}
				} else {
					Tuple2<int[], Double> gain = bestSplitCategorical(fStart, mse, squareSum, sum, weight,
						featureMetas[pair.baggingFeatures[j]].getNumCategorical());
					if (gain.f1 > gBestGain) {
						gBestGain = gain.f1;
						gBestSplitCategorical = gain.f0;
						gBestFeature = pair.baggingFeatures[j];
					}
				}
			}
		} else {
			for (int j = 0; j < nFeatureCol; ++j) {
				int fStart = start + j * nBin * 3;
				if (featureMetas[j].getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
					Tuple2<Integer, Double> gain = bestSplitNumerical(fStart, mse, squareSum, sum, weight);
					if (gain.f1 > gBestGain) {
						gBestGain = gain.f1;
						gBestSplit = gain.f0;
						gBestFeature = j;
					}
				} else {
					Tuple2<int[], Double> gain = bestSplitCategorical(fStart, mse, squareSum, sum, weight,
						featureMetas[j].getNumCategorical());
					if (gain.f1 > gBestGain) {
						gBestGain = gain.f1;
						gBestSplitCategorical = gain.f0;
						gBestFeature = j;
					}
				}
			}
		}

		if (gBestGain > 0.) {
			node.setFeatureIndex(gBestFeature);
			if (featureMetas[gBestFeature].getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
				node.setContinuousSplit(gBestSplit);
			} else {
				node.setCategoricalSplit(gBestSplitCategorical);
			}
			node.setGain(gBestGain);
		} else {
			node.makeLeaf();
		}
	}
}
