package com.alibaba.alink.operator.common.tree.parallelcart.data;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.operator.common.tree.parallelcart.BaseGbdtTrainBatchOp;
import com.alibaba.alink.operator.common.tree.parallelcart.EpsilonApproQuantile;
import com.alibaba.alink.operator.common.tree.parallelcart.criteria.PaiCriteria;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossUtils;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public final class DenseData implements Data {
	FeatureMeta[] featureMetas;

	int m;
	int n;

	int[] values;

	IndexedValue[] sortedValues;
	Integer[] orderedIndices;

	double[] labels;
	double[] weights;

	boolean isRanking;
	int[] queryIdOffset;
	int maxQuerySize;

	boolean useMissing;
	boolean zeroAsMissing;

	public DenseData(Params params, FeatureMeta[] featureMetas, int m, int n, boolean useSort) {
		this.featureMetas = featureMetas;
		this.m = m;
		this.n = n;

		if (useSort) {
			sortedValues = new IndexedValue[m * n];
			orderedIndices = new Integer[m * n];
		} else {
			values = new int[m * n];
		}

		labels = new double[m];
		weights = new double[m];

		Arrays.fill(weights, 1.0);

		isRanking = LossUtils.isRanking(params.get(LossUtils.LOSS_TYPE));

		useMissing = params.get(BaseGbdtTrainBatchOp.USE_MISSING);
		zeroAsMissing = params.get(Preprocessing.ZERO_AS_MISSING);
	}

	@Override
	public int getM() {
		return m;
	}

	@Override
	public int getN() {
		return n;
	}

	@Override
	public double[] getLabels() {
		return labels;
	}

	@Override
	public double[] getWeights() {
		return weights;
	}

	@Override
	public boolean isRanking() {
		return isRanking;
	}

	@Override
	public int[] getQueryIdOffset() {
		return queryIdOffset;
	}

	@Override
	public int getMaxQuerySize() {
		return maxQuerySize;
	}

	@Override
	public FeatureMeta[] getFeatureMetas() {
		return featureMetas;
	}

	@Override
	public void loadFromRow(List <Row> rawData) {
		if (rawData == null) {
			return;
		}

		if (isRanking) {
			rawData.sort((o1, o2) -> ((Comparable) o1.getField(0)).compareTo(o2.getField(0)));
			int querySize = 0;
			long latestQueryId = 0;
			for (Row row : rawData) {
				long queryId = ((Number) row.getField(0)).longValue();

				if (querySize == 0 || latestQueryId != queryId) {
					latestQueryId = queryId;
					querySize++;
				}
			}

			int queryIdOffsetIndex = 0;

			queryIdOffset = new int[querySize + 1];

			int localCnt = 0;

			for (Row row : rawData) {
				long queryId = ((Number) row.getField(0)).longValue();

				if (queryIdOffsetIndex == 0 || latestQueryId != queryId) {
					queryIdOffset[queryIdOffsetIndex] = localCnt;
					queryIdOffsetIndex++;
					if (maxQuerySize < latestQueryId - queryId) {
						maxQuerySize = (int) (latestQueryId - queryId);
					}
					latestQueryId = queryId;
				}

				final int lastIndex = row.getArity() - 1;
				for (int i = 1; i < lastIndex; ++i) {
					Object val = row.getField(i);

					boolean isMissing = Preprocessing.isMissing(val, featureMetas[i - 1].getMissingIndex());

					if (!useMissing && isMissing) {
						throw new IllegalArgumentException(
							"Find the missing value in data. " +
								"Maybe you could open the useMissing to deal with the missing value"
						);
					}

					if (isMissing) {
						values[(i - 1) * m + localCnt] = featureMetas[i - 1].getMissingIndex();
					} else {
						values[(i - 1) * m + localCnt] = (int) val;
					}
				}

				labels[localCnt] = ((Number) row.getField(row.getArity() - 1)).doubleValue();
				localCnt++;
			}

			queryIdOffset[querySize] = localCnt;

		} else {
			int localCnt = 0;

			for (Row row : rawData) {
				final int lastIndex = row.getArity() - 1;
				for (int i = 0; i < lastIndex; ++i) {
					Object val = row.getField(i);

					boolean isMissing = Preprocessing.isMissing(val, featureMetas[i].getMissingIndex());

					if (!useMissing && isMissing) {
						throw new IllegalArgumentException(
							"Find the missing value in data. " +
								"Maybe you could open the useMissing to deal with the missing value"
						);
					}

					if (isMissing) {
						values[i * m + localCnt] = featureMetas[i].getMissingIndex();
					} else {
						values[i * m + localCnt] = (int) val;
					}
				}

				labels[localCnt] = ((Number) row.getField(lastIndex)).doubleValue();
				localCnt++;
			}
		}
	}

	@Override
	public void loadFromRowWithContinues(List <Row> rawData) {
		if (rawData == null) {
			return;
		}

		if (isRanking) {
			rawData.sort((o1, o2) -> ((Comparable) o1.getField(0)).compareTo(o2.getField(0)));
			int querySize = 0;
			long latestQueryId = 0;
			for (Row row : rawData) {
				long queryId = ((Number) row.getField(0)).longValue();

				if (querySize == 0 || latestQueryId != queryId) {
					latestQueryId = queryId;
					querySize++;
				}
			}

			int queryIdOffsetIndex = 0;

			queryIdOffset = new int[querySize + 1];

			int localCnt = 0;

			for (Row row : rawData) {
				long queryId = ((Number) row.getField(0)).longValue();

				if (queryIdOffsetIndex == 0 || latestQueryId != queryId) {
					queryIdOffset[queryIdOffsetIndex] = localCnt;
					queryIdOffsetIndex++;
					if (maxQuerySize < latestQueryId - queryId) {
						maxQuerySize = (int) (latestQueryId - queryId);
					}
					latestQueryId = queryId;
				}

				final int lastIndex = row.getArity() - 1;
				for (int i = 1; i < lastIndex; ++i) {
					Object val = row.getField(i);

					boolean isMissing = Preprocessing.isMissing(val, featureMetas[i - 1], zeroAsMissing);

					if (!useMissing && isMissing) {
						throw new IllegalArgumentException(
							"Find the missing value in data. " +
								"Maybe you could open the useMissing to deal with the missing value"
						);
					}

					if (isMissing) {
						sortedValues[(i - 1) * m + localCnt] = new IndexedValue(
							localCnt,
							featureMetas[i - 1].getType().equals(FeatureMeta.FeatureType.CONTINUOUS) ?
								Double.NaN : featureMetas[i - 1].getMissingIndex()
						);
					} else {
						sortedValues[(i - 1) * m + localCnt] = new IndexedValue(localCnt, ((Number) val).doubleValue
							());
					}
				}

				labels[localCnt] = ((Number) row.getField(row.getArity() - 1)).doubleValue();
				localCnt++;
			}

			queryIdOffset[querySize] = localCnt;

		} else {
			int localCnt = 0;

			for (Row row : rawData) {
				final int lastIndex = row.getArity() - 1;
				for (int i = 0; i < lastIndex; ++i) {
					Object val = row.getField(i);

					boolean isMissing = Preprocessing.isMissing(val, featureMetas[i], zeroAsMissing);

					if (!useMissing && isMissing) {
						throw new IllegalArgumentException(
							"Find the missing value in data. " +
								"Maybe you could open the useMissing to deal with the missing value"
						);
					}

					if (isMissing) {
						sortedValues[i * m + localCnt] = new IndexedValue(localCnt,
							featureMetas[i].getType().equals(FeatureMeta.FeatureType.CONTINUOUS) ?
								Double.NaN : featureMetas[i].getMissingIndex());
					} else {
						sortedValues[i * m + localCnt] = new IndexedValue(localCnt, ((Number) val).doubleValue());
					}
				}

				labels[localCnt] = ((Number) row.getField(lastIndex)).doubleValue();
				localCnt++;
			}
		}
	}

	@Override
	public int splitInstances(
		Node node,
		EpsilonApproQuantile.WQSummary summary,
		int[] indices,
		Slice slice) {

		if (sortedValues != null) {
			if (featureMetas[node.getFeatureIndex()].getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
				int featureOffset = node.getFeatureIndex() * getM();

				int lstart = slice.start;
				int lend = slice.end;
				lend--;
				while (lstart <= lend) {
					while (lstart <= lend &&
						DataUtil.leftUseSummary(
							sortedValues[orderedIndices[featureOffset + indices[lstart]]].val,
							node, summary,
							featureMetas[node.getFeatureIndex()],
							zeroAsMissing)) {
						lstart++;
					}

					while (lstart <= lend &&
						!DataUtil.leftUseSummary(
							sortedValues[orderedIndices[featureOffset + indices[lend]]].val,
							node, summary,
							featureMetas[node.getFeatureIndex()],
							zeroAsMissing)) {
						lend--;
					}

					if (lstart < lend) {
						int tmp = indices[lstart];
						indices[lstart] = indices[lend];
						indices[lend] = tmp;
					}
				}

				return lstart;
			} else {
				int featureOffset = node.getFeatureIndex() * getM();

				int lstart = slice.start;
				int lend = slice.end;
				lend--;
				while (lstart <= lend) {
					while (lstart <= lend &&
						DataUtil.left((int) sortedValues[featureOffset + indices[lstart]].val, node,
							featureMetas[node.getFeatureIndex()])) {
						lstart++;
					}

					while (lstart <= lend &&
						!DataUtil.left((int) sortedValues[featureOffset + indices[lend]].val, node,
							featureMetas[node.getFeatureIndex()])) {
						lend--;
					}

					if (lstart < lend) {
						int tmp = indices[lstart];
						indices[lstart] = indices[lend];
						indices[lend] = tmp;
					}
				}

				return lstart;
			}
		} else {

			int featureOffset = node.getFeatureIndex() * getM();

			int lstart = slice.start;
			int lend = slice.end;
			lend--;
			while (lstart <= lend) {
				while (lstart <= lend &&
					DataUtil.left(values[featureOffset + indices[lstart]], node,
						featureMetas[node.getFeatureIndex()])) {
					lstart++;
				}

				while (lstart <= lend &&
					!DataUtil.left(values[featureOffset + indices[lend]], node, featureMetas[node.getFeatureIndex()
						])) {
					lend--;
				}

				if (lstart < lend) {
					int tmp = indices[lstart];
					indices[lstart] = indices[lend];
					indices[lend] = tmp;
				}
			}

			return lstart;
		}
	}

	@Override
	public void sort() {
		for (int i = 0; i < n; ++i) {
			final FeatureMeta featureMeta = featureMetas[i];
			if (featureMeta.getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
				Arrays.sort(sortedValues, featureMeta.getIndex() * m, (featureMeta.getIndex() + 1) * m, (o1, o2) -> {
					boolean isMissing1 = Preprocessing.isMissing(o1.val, featureMeta, zeroAsMissing);
					boolean isMissing2 = Preprocessing.isMissing(o2.val, featureMeta, zeroAsMissing);

					if (isMissing1 && isMissing2) {
						return 0;
					} else if (isMissing1) {
						return 1;
					} else if (isMissing2) {
						return -1;
					} else {
						return Double.compare(o1.val, o2.val);
					}
				});

				for (int j = 0; j < m; ++j) {
					orderedIndices[i * m + j] = i * m + j;
				}

				Arrays.sort(orderedIndices, i * m, (i + 1) * m, Comparator.comparingInt(o -> sortedValues[o].index));
			}
		}
	}

	@Override
	public EpsilonApproQuantile.SketchEntry[] createWQSummary(
		int maxSize, double eps, EpsilonApproQuantile.SketchEntry[] buffer,
		double[] dynamicWeights,
		BitSet validFlags) {
		for (int i = 0, index = 0; i < n; ++i) {
			final FeatureMeta featureMeta = featureMetas[i];
			if (featureMeta.getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
				buffer[index].sumTotal = 0.0;

				int featureOffSet = i * m;

				for (int j = 0; j < m; ++j) {
					IndexedValue v = sortedValues[featureOffSet + j];

					if (validFlags.get(v.index) && !Preprocessing.isMissing(v.val, featureMeta, zeroAsMissing)) {
						buffer[index].sumTotal += dynamicWeights[v.index];
					}
				}

				index++;
			}
		}

		for (int i = 0, index = 0; i < n; ++i) {
			final FeatureMeta featureMeta = featureMetas[i];
			if (featureMeta.getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
				int start = 0;
				int end = m;

				EpsilonApproQuantile.SketchEntry entry = buffer[index];

				if (start == end || entry.sumTotal == 0.0) {
					//empty or all elements are null.
					index++;
					continue;
				}

				entry.init(maxSize);

				int featureOffSet = i * m;

				for (int j = start; j < end; ++j) {
					IndexedValue v = sortedValues[featureOffSet + j];

					if (validFlags.get(v.index) && !Preprocessing.isMissing(v.val, featureMeta, zeroAsMissing)) {
						entry.push(v.val, dynamicWeights[v.index], maxSize);
					}
				}

				entry.finalize(maxSize);
				index++;
			}
		}

		return buffer;
	}

	@Override
	public void constructHistogramWithWQSummary(
		boolean useInstanceCount, int nodeSize,
		BitSet featureValid, int[] nodeIdCache, int[] validFeatureOffset,
		double[] gradients, double[] hessions, double[] weights,
		EpsilonApproQuantile.WQSummary[] summaries, ExecutorService executorService,
		Future <?>[] futures, double[] featureSplitHistogram) {

		final int step = 4;
		for (int i = 0, index = 0; i < getN(); ++i) {
			final FeatureMeta featureMeta = featureMetas[i];
			boolean isContinuous = featureMeta.getType().equals(FeatureMeta.FeatureType.CONTINUOUS);

			futures[i] = null;
			if (!featureValid.get(i)) {
				if (isContinuous) {
					index++;
				}

				continue;
			}

			if (isContinuous) {
				EpsilonApproQuantile.WQSummary summary = summaries[index];

				final int dataOffset = getM() * i;
				final int featureSize = DataUtil.getFeatureCategoricalSize(featureMetas[i], useMissing);
				final int histogramOffset = validFeatureOffset[i] * nodeSize * step;
				final int nextHistogramOffset = histogramOffset + featureSize * nodeSize * step;

				if (useInstanceCount) {
					futures[i] = executorService.submit(() -> {
						int cursor = 0;
						Arrays.fill(featureSplitHistogram, histogramOffset, nextHistogramOffset, 0.0);
						for (int j = 0; j < m; ++j) {
							final int localRowIndex = sortedValues[dataOffset + j].index;

							if (nodeIdCache[localRowIndex] < 0) {
								continue;
							}

							while ((cursor < summary.entries.size()
								&& summary.entries.get(cursor).value < sortedValues[dataOffset + j].val)) {
								cursor++;
							}

							if (Preprocessing.isMissing(sortedValues[dataOffset + j].val, featureMeta,
								zeroAsMissing)) {
								cursor = summary.entries.size();
							}

							final int localValue = cursor;

							final int node = nodeIdCache[localRowIndex];
							final int counterIndex = (node * featureSize + localValue) * step + histogramOffset;
							featureSplitHistogram[counterIndex] += gradients[localRowIndex];
							featureSplitHistogram[counterIndex + 1] += hessions[localRowIndex];
							featureSplitHistogram[counterIndex + 2] += weights[localRowIndex];
							if (weights[localRowIndex] > PaiCriteria.PAI_EPS) {
								featureSplitHistogram[counterIndex + 3] += 1.0;
							}
						}
					});
				} else {
					futures[i] = executorService.submit(() -> {
						int cursor = 0;
						Arrays.fill(featureSplitHistogram, histogramOffset, nextHistogramOffset, 0.0);
						for (int j = 0; j < m; ++j) {
							final int localRowIndex = sortedValues[dataOffset + j].index;

							if (nodeIdCache[localRowIndex] < 0) {
								continue;
							}

							while ((cursor < summary.entries.size()
								&& summary.entries.get(cursor).value < sortedValues[dataOffset + j].val)) {
								cursor++;
							}

							if (Preprocessing.isMissing(sortedValues[dataOffset + j].val, featureMeta,
								zeroAsMissing)) {
								cursor = summary.entries.size();
							}

							final int localValue = cursor;

							final int node = nodeIdCache[localRowIndex];
							final int counterIndex = (node * featureSize + localValue) * step + histogramOffset;
							featureSplitHistogram[counterIndex] += gradients[localRowIndex];
							featureSplitHistogram[counterIndex + 1] += hessions[localRowIndex];
							featureSplitHistogram[counterIndex + 2] += weights[localRowIndex];
							featureSplitHistogram[counterIndex + 3] += 1.0;
						}
					});
				}

				index++;
			} else {
				final int dataOffset = getM() * i;
				final int featureSize = DataUtil.getFeatureCategoricalSize(featureMetas[i], useMissing);
				final int histogramOffset = validFeatureOffset[i] * nodeSize * step;
				final int nextHistogramOffset = histogramOffset + featureSize * nodeSize * step;

				if (useInstanceCount) {
					futures[i] = executorService.submit(() -> {
						Arrays.fill(featureSplitHistogram, histogramOffset, nextHistogramOffset, 0.0);
						for (int j = 0; j < m; ++j) {
							final int localRowIndex = sortedValues[dataOffset + j].index;

							if (nodeIdCache[localRowIndex] < 0) {
								continue;
							}

							final int localValue = (int) sortedValues[dataOffset + j].val;
							final int node = nodeIdCache[localRowIndex];
							final int counterIndex = (node * featureSize + localValue) * step + histogramOffset;
							featureSplitHistogram[counterIndex] += gradients[localRowIndex];
							featureSplitHistogram[counterIndex + 1] += hessions[localRowIndex];
							featureSplitHistogram[counterIndex + 2] += weights[localRowIndex];
							if (weights[localRowIndex] > PaiCriteria.PAI_EPS) {
								featureSplitHistogram[counterIndex + 3] += 1.0;
							}
						}
					});
				} else {
					futures[i] = executorService.submit(() -> {
						Arrays.fill(featureSplitHistogram, histogramOffset, nextHistogramOffset, 0.0);
						for (int j = 0; j < m; ++j) {
							final int localRowIndex = sortedValues[dataOffset + j].index;

							if (nodeIdCache[localRowIndex] < 0) {
								continue;
							}

							final int localValue = (int) sortedValues[dataOffset + j].val;
							final int node = nodeIdCache[localRowIndex];
							final int counterIndex = (node * featureSize + localValue) * step + histogramOffset;
							featureSplitHistogram[counterIndex] += gradients[localRowIndex];
							featureSplitHistogram[counterIndex + 1] += hessions[localRowIndex];
							featureSplitHistogram[counterIndex + 2] += weights[localRowIndex];
							featureSplitHistogram[counterIndex + 3] += 1.0;
						}
					});
				}
			}
		}

		for (Future <?> future : futures) {
			if (future == null) {
				continue;
			}

			try {
				future.get();
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
	}

	@Override
	public void constructHistogram(
		final boolean useInstanceCount,
		final int nodeSize,
		final int validInstanceCount,
		BitSet featureValid,
		int[] nodeIdCache,
		int[] validFeatureOffset,
		int[] aligned,
		double[] gradients,
		double[] hessions,
		double[] weights,
		ExecutorService executorService,
		Future <?>[] futures,
		double[] featureSplitHistogram) {

		final int step = 4;
		for (int i = 0; i < getN(); ++i) {
			futures[i] = null;
			if (!featureValid.get(i)) {
				continue;
			}

			final int dataOffset = getM() * i;
			final int featureSize = DataUtil.getFeatureCategoricalSize(featureMetas[i], useMissing);
			final int histogramOffset = validFeatureOffset[i] * nodeSize * step;
			final int nextHistogramOffset = histogramOffset + featureSize * nodeSize * step;

			if (useInstanceCount) {
				futures[i] = executorService.submit(() -> {
					Arrays.fill(featureSplitHistogram, histogramOffset, nextHistogramOffset, 0.0);
					for (int j = 0; j < validInstanceCount; ++j) {
						final int val = values[dataOffset + aligned[j]];
						final int node = nodeIdCache[aligned[j]];
						final int counterIndex = (node * featureSize + val) * step + histogramOffset;
						featureSplitHistogram[counterIndex] += gradients[aligned[j]];
						featureSplitHistogram[counterIndex + 1] += hessions[aligned[j]];
						featureSplitHistogram[counterIndex + 2] += weights[aligned[j]];
						if (weights[aligned[j]] > PaiCriteria.PAI_EPS) {
							featureSplitHistogram[counterIndex + 3] += 1.0;
						}
					}
				});
			} else {
				futures[i] = executorService.submit(() -> {
					Arrays.fill(featureSplitHistogram, histogramOffset, nextHistogramOffset, 0.0);
					for (int j = 0; j < validInstanceCount; ++j) {
						final int val = values[dataOffset + aligned[j]];
						final int node = nodeIdCache[aligned[j]];
						final int counterIndex = (node * featureSize + val) * step + histogramOffset;
						featureSplitHistogram[counterIndex] += gradients[aligned[j]];
						featureSplitHistogram[counterIndex + 1] += hessions[aligned[j]];
						featureSplitHistogram[counterIndex + 2] += weights[aligned[j]];
						featureSplitHistogram[counterIndex + 3] += 1.0;
					}
				});
			}
		}

		for (Future <?> future : futures) {
			if (future == null) {
				continue;
			}

			try {
				future.get();
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
	}
}
