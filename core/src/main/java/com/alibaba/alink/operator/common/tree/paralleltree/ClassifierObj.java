package com.alibaba.alink.operator.common.tree.paralleltree;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class ClassifierObj extends TreeObj<int[]> {
	private static final Logger LOG = LoggerFactory.getLogger(ClassifierObj.class);

	private Criteria.Gini total;
	private int[] labels;
	private int nLabels;

	public ClassifierObj(
		Params params,
		QuantileDiscretizerModelDataConverter qModel,
		FeatureMeta[] featureMetas,
		FeatureMeta labelMeta) {
		super(params, qModel, featureMetas, labelMeta);

		nLabels = this.labelMeta.getNumCategorical();

		total = new Criteria.Gini(0, 0, new double[nLabels]);

		initBuffer();
	}

	@Override
	public int lenPerStat() {
		if (baggingFeatureCount() != nFeatureCol) {
			return nLabels * baggingFeatureCount() * nBin;
		} else {
			return nLabels * nFeatureCol * nBin;
		}
	}

	@Override
	public void setLabels(int[] values) {
		labels = values;
	}

	@Override
	public void stat(int loop, int start, int end, int bufStart, int bufEnd) {
		int f0, f1, h, p;

		Arrays.fill(hist, bufStart, bufEnd, 0.);

		if (baggingFeatureCount() != nFeatureCol) {
			int[] baggingFeature = loopBuffer[loop].baggingFeatures;
			for (int i = 0; i < baggingFeatureCount(); ++i) {
				f0 = baggingFeature[i] * numLocalRow;
				f1 = i * nLabels * nBin + bufStart;
				for (int j = start; j < end; ++j) {
					p = partitions[j];
					h = f1 + features[f0 + p] * nLabels;
					hist[h + labels[p]] += 1.;
				}
			}
		} else {
			for (int i = 0; i < nFeatureCol; ++i) {
				f0 = i * numLocalRow;
				f1 = i * nLabels * nBin + bufStart;
				for (int j = start; j < end; ++j) {
					p = partitions[j];
					h = f1 + features[f0 + p] * nLabels;
					hist[h + labels[p]] += 1.;
				}
			}
		}
	}

	public final Tuple2<int[], Double> bestSplitCategorical(
		int featureStart, int categoricalSize) {
		double bestGain = 0.;
		int[] bestSplit = new int[categoricalSize];
		int[] splitBuf = new int[categoricalSize];

		int n = 1 << (categoricalSize - 1);
		for (int i = 1; i < n; ++i) {
			Criteria.Gini left = new Criteria.Gini(0, 0, new double[nLabels]);
			Criteria.Gini right = new Criteria.Gini(0, 0, new double[nLabels]);

			for (int j = 0; j < categoricalSize; ++j) {
				splitBuf[j] = -1;

				if ((i & (1 << j)) != 0) {
					splitBuf[j] = 0;
					int binStart = featureStart + j * nLabels;
					for (int z = 0; z < nLabels; ++z) {
						left.add(z, minusHist[binStart + z]);
					}
				} else {
					splitBuf[j] = 1;
					int binStart = featureStart + j * nLabels;
					for (int z = 0; z < nLabels; ++z) {
						right.add(z, minusHist[binStart + z]);
					}
				}
			}

			if (minSamplesPerLeaf > left.getNumInstances()
				|| minSamplesPerLeaf > right.getNumInstances()) {
				continue;
			}

			double curGain = total.gain(left, right);

			if (curGain > bestGain) {
				bestGain = curGain;
				System.arraycopy(splitBuf, 0, bestSplit, 0, categoricalSize);
			}
		}

		return Tuple2.of(bestSplit, bestGain);
	}

	public final Tuple2<Integer, Double> bestSplitNumerical(int fStart) throws Exception {
		double bestGain = 0.;
		int bestSplit = 0;
		Criteria.Gini left = new Criteria.Gini(0, 0, new double[nLabels]);
		Criteria.Gini right = (Criteria.Gini) total.clone();

		for (int z = 0; z < nBin - 1; ++z) {
			int binStart = fStart + z * nLabels;
			for (int i = 0; i < nLabels; ++i) {
				left.add(i, minusHist[binStart + i]);
				right.subtract(i, minusHist[binStart + i]);
			}

			if (minSamplesPerLeaf > left.getNumInstances()
				|| minSamplesPerLeaf > right.getNumInstances()) {
				continue;
			}

			double curGain = total.gain(left, right);

			if (curGain > bestGain) {
				bestGain = curGain;
				bestSplit = z;
			}
		}

		return Tuple2.of(bestSplit, bestGain);
	}

	@Override
	public final void bestSplit(Node node, int minusId, NodeInfoPair pair) throws Exception {
		int start = minusId * lenPerStat();

		double gBestGain = 0.;
		int gBestSplit = 0;
		int[] gBestSplitCategorical = null;
		int gBestFeature = -1;

		total = new Criteria.Gini(0, 0, new double[nLabels]);
		// stat the first feature
		for (int z = 0; z < nBin; ++z) {
			int zStart = start + z * nLabels;
			for (int i = 0; i < nLabels; ++i) {
				total.add(i, minusHist[zStart + i]);
			}
		}

		node.setCounter(total.toLabelCounter());

		if (total.getWeightSum() < 0) {
			LOG.info("total: {}", JsonConverter.gson.toJson(total));
		}

		if (maxDepth < pair.depth
			|| minSamplesPerLeaf * 2 >= total.getNumInstances()) {
			node.makeLeaf();
			return;
		}

		if (baggingFeatureCount() != nFeatureCol) {
			int baggingFeatureCount = baggingFeatureCount();
			for (int j = 0; j < baggingFeatureCount; ++j) {
				int fStart = start + j * nBin * nLabels;
				if (featureMetas[pair.baggingFeatures[j]].getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
					Tuple2<Integer, Double> gain = bestSplitNumerical(fStart);
					if (gain.f1 > gBestGain) {
						gBestGain = gain.f1;
						gBestSplit = gain.f0;
						gBestFeature = pair.baggingFeatures[j];
					}
				} else {
					Tuple2<int[], Double> gain = bestSplitCategorical(fStart,
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
				int fStart = start + j * nBin * nLabels;
				if (featureMetas[j].getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
					Tuple2<Integer, Double> gain = bestSplitNumerical(fStart);
					if (gain.f1 > gBestGain) {
						gBestGain = gain.f1;
						gBestSplit = gain.f0;
						gBestFeature = j;
					}
				} else {
					Tuple2<int[], Double> gain = bestSplitCategorical(fStart,
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
