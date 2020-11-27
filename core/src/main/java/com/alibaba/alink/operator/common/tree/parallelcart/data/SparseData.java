package com.alibaba.alink.operator.common.tree.parallelcart.data;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
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

/**
 * Compressed sparse column(CSC) implement.
 * <p>see: <a href="https://en.wikipedia.org/wiki/Sparse_matrix">Sparse_matrix</a>
 */
public final class SparseData implements Data {
	FeatureMeta[] featureMetas;

	int m;
	int n;
	int nnz;

	int[] col;
	int[] row;
	int[] val;

	IndexedValue[] values;
	Integer[] orderedIndices;

	double[] labels;
	double[] weights;

	boolean isRanking;
	int[] queryIdOffset;
	int maxQuerySize;

	boolean useMissing;
	boolean zeroAsMissing;

	public SparseData(Params params, FeatureMeta[] featureMetas, int m, int n) {
		this.featureMetas = featureMetas;
		this.m = m;
		this.n = n;

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

		col = new int[n + 1];
		nnz = 0;

		if (isRanking) {
			rawData.sort((o1, o2) -> ((Comparable) o1.getField(0)).compareTo(o2.getField(0)));
			int[] rowAcc = new int[n];
			int querySize = 0;
			long latestQueryId = 0;
			int localCnt = 0;
			for (Row localRow : rawData) {
				long queryId = ((Number) localRow.getField(0)).longValue();

				if (querySize == 0 || latestQueryId != queryId) {
					latestQueryId = queryId;
					querySize++;
				}
				Vector vector = VectorUtil.getVector(localRow.getField(1));

				if (vector instanceof SparseVector) {
					SparseVector sparseVector = (SparseVector) vector;
					int[] indices = sparseVector.getIndices();

					for (int index : indices) {
						rowAcc[index] += 1;
					}

					nnz += sparseVector.getValues().length;
				} else {
					DenseVector denseVector = (DenseVector) vector;

					double[] vals = denseVector.getData();
					for (int i = 0; i < vals.length; ++i) {
						rowAcc[i] += 1;
					}

					nnz += vals.length;
				}

				labels[localCnt] = ((Number) localRow.getField(localRow.getArity() - 1)).doubleValue();
				localCnt++;
			}

			for (int i = 0; i < n; i++) {
				col[i + 1] = rowAcc[i] + col[i];
			}

			int queryIdOffsetIndex = 0;

			queryIdOffset = new int[querySize + 1];

			row = new int[nnz];
			val = new int[nnz];

			Arrays.fill(rowAcc, 0);
			localCnt = 0;
			for (Row localRow : rawData) {
				long queryId = ((Number) localRow.getField(0)).longValue();

				if (queryIdOffsetIndex == 0 || latestQueryId != queryId) {
					queryIdOffset[queryIdOffsetIndex] = localCnt;
					queryIdOffsetIndex++;
					if (maxQuerySize < latestQueryId - queryId) {
						maxQuerySize = (int) (latestQueryId - queryId);
					}
					latestQueryId = queryId;
				}
				Vector vector = VectorUtil.getVector(localRow.getField(1));
				if (vector instanceof SparseVector) {
					SparseVector sparseVector = (SparseVector) vector;
					int[] indices = sparseVector.getIndices();
					double[] values = sparseVector.getValues();

					for (int j = 0; j < indices.length; ++j) {
						int localCol = indices[j];
						int localVal = (int) values[j];

						boolean isMissing = Preprocessing.isMissing(localVal, featureMetas[localCol].getMissingIndex
							());

						if (!useMissing && isMissing) {
							throw new IllegalArgumentException(
								"Find the missing value in data. " +
									"Maybe you could open the useMissing to deal with the missing value"
							);
						}

						if (isMissing) {
							localVal = featureMetas[localCol].getMissingIndex();
						}

						row[col[localCol] + rowAcc[localCol]] = localCnt;
						val[col[localCol] + rowAcc[localCol]] = localVal;
						rowAcc[localCol]++;
					}
				} else {
					DenseVector denseVector = (DenseVector) vector;
					double[] values = denseVector.getData();

					for (int j = 0; j < values.length; ++j) {
						int localVal = (int) values[j];

						boolean isMissing = Preprocessing.isMissing(localVal, featureMetas[j].getMissingIndex());

						if (!useMissing && isMissing) {
							throw new IllegalArgumentException(
								"Find the missing value in data. " +
									"Maybe you could open the useMissing to deal with the missing value"
							);
						}

						if (isMissing) {
							localVal = featureMetas[j].getMissingIndex();
						}

						row[col[j] + rowAcc[j]] = localCnt;
						val[col[j] + rowAcc[j]] = localVal;
						rowAcc[j]++;
					}
				}

				localCnt++;
			}

			queryIdOffset[querySize] = localCnt;

		} else {
			int localCnt = 0;
			int[] rowAcc = new int[n];

			for (Row localRow : rawData) {
				Vector vector = VectorUtil.getVector(localRow.getField(0));

				if (vector instanceof SparseVector) {
					SparseVector sparseVector = (SparseVector) vector;
					int[] indices = sparseVector.getIndices();

					for (int index : indices) {
						rowAcc[index] += 1;
					}

					nnz += sparseVector.getValues().length;
				} else {
					DenseVector denseVector = (DenseVector) vector;

					double[] vals = denseVector.getData();
					for (int i = 0; i < vals.length; ++i) {
						rowAcc[i] += 1;
					}

					nnz += vals.length;
				}

				labels[localCnt] = ((Number) localRow.getField(localRow.getArity() - 1)).doubleValue();
				localCnt++;
			}

			for (int i = 0; i < n; i++) {
				col[i + 1] = rowAcc[i] + col[i];
			}

			row = new int[nnz];
			val = new int[nnz];

			Arrays.fill(rowAcc, 0);

			localCnt = 0;
			for (Row localRow : rawData) {
				Vector vector = VectorUtil.getVector(localRow.getField(0));
				if (vector instanceof SparseVector) {
					SparseVector sparseVector = (SparseVector) vector;
					int[] indices = sparseVector.getIndices();
					double[] values = sparseVector.getValues();

					for (int j = 0; j < indices.length; ++j) {
						int localCol = indices[j];
						int localVal = (int) values[j];

						boolean isMissing = Preprocessing.isMissing(localVal, featureMetas[localCol].getMissingIndex
							());

						if (!useMissing && isMissing) {
							throw new IllegalArgumentException(
								"Find the missing value in data. " +
									"Maybe you could open the useMissing to deal with the missing value"
							);
						}

						if (isMissing) {
							localVal = featureMetas[localCol].getMissingIndex();
						}

						row[col[localCol] + rowAcc[localCol]] = localCnt;
						val[col[localCol] + rowAcc[localCol]] = localVal;
						rowAcc[localCol]++;
					}

				} else {
					DenseVector denseVector = (DenseVector) vector;
					double[] values = denseVector.getData();

					for (int j = 0; j < values.length; ++j) {
						int localVal = (int) values[j];

						boolean isMissing = Preprocessing.isMissing(localVal, featureMetas[j].getMissingIndex());

						if (!useMissing && isMissing) {
							throw new IllegalArgumentException(
								"Find the missing value in data. " +
									"Maybe you could open the useMissing to deal with the missing value"
							);
						}

						if (isMissing) {
							localVal = featureMetas[j].getMissingIndex();
						}

						row[col[j] + rowAcc[j]] = localCnt;
						val[col[j] + rowAcc[j]] = localVal;
						rowAcc[j]++;
					}
				}

				localCnt++;
			}
		}
	}

	@Override
	public void loadFromRowWithContinues(List <Row> rawData) {
		if (rawData == null) {
			return;
		}

		col = new int[n + 1];
		nnz = 0;

		if (isRanking) {
			rawData.sort((o1, o2) -> ((Comparable) o1.getField(0)).compareTo(o2.getField(0)));
			int[] rowAcc = new int[n];
			int querySize = 0;
			long latestQueryId = 0;
			int localCnt = 0;
			for (Row localRow : rawData) {
				long queryId = ((Number) localRow.getField(0)).longValue();

				if (querySize == 0 || latestQueryId != queryId) {
					latestQueryId = queryId;
					querySize++;
				}
				Vector vector = VectorUtil.getVector(localRow.getField(1));

				if (vector instanceof SparseVector) {
					SparseVector sparseVector = (SparseVector) vector;
					int[] indices = sparseVector.getIndices();

					for (int index : indices) {
						rowAcc[index] += 1;
					}

					nnz += sparseVector.getValues().length;
				} else {
					DenseVector denseVector = (DenseVector) vector;

					double[] vals = denseVector.getData();
					for (int i = 0; i < vals.length; ++i) {
						rowAcc[i] += 1;
					}

					nnz += vals.length;
				}

				labels[localCnt] = ((Number) localRow.getField(localRow.getArity() - 1)).doubleValue();
				localCnt++;
			}

			for (int i = 0; i < n; i++) {
				col[i + 1] = rowAcc[i] + col[i];
			}

			int queryIdOffsetIndex = 0;

			queryIdOffset = new int[querySize + 1];

			values = new IndexedValue[nnz];
			orderedIndices = new Integer[nnz];

			Arrays.fill(rowAcc, 0);
			localCnt = 0;
			for (Row localRow : rawData) {
				long queryId = ((Number) localRow.getField(0)).longValue();

				if (queryIdOffsetIndex == 0 || latestQueryId != queryId) {
					queryIdOffset[queryIdOffsetIndex] = localCnt;
					queryIdOffsetIndex++;
					if (maxQuerySize < latestQueryId - queryId) {
						maxQuerySize = (int) (latestQueryId - queryId);
					}
					latestQueryId = queryId;
				}
				Vector vector = VectorUtil.getVector(localRow.getField(1));
				if (vector instanceof SparseVector) {
					SparseVector sparseVector = (SparseVector) vector;
					int[] indices = sparseVector.getIndices();
					double[] values = sparseVector.getValues();

					for (int j = 0; j < indices.length; ++j) {
						int localCol = indices[j];
						double localVal = values[j];

						boolean isMissing = Preprocessing.isMissing(localVal, featureMetas[localCol], zeroAsMissing);

						if (!useMissing && isMissing) {
							throw new IllegalArgumentException(
								"Find the missing value in data. " +
									"Maybe you could open the useMissing to deal with the missing value"
							);
						}

						if (isMissing && featureMetas[j].getType().equals(FeatureMeta.FeatureType.CATEGORICAL)) {
							localVal = featureMetas[localCol].getMissingIndex();
						}

						this.values[col[localCol] + rowAcc[localCol]] = new IndexedValue(localCnt, localVal);

						rowAcc[localCol]++;
					}
				} else {
					DenseVector denseVector = (DenseVector) vector;
					double[] values = denseVector.getData();

					for (int j = 0; j < values.length; ++j) {
						double localVal = values[j];

						boolean isMissing = Preprocessing.isMissing(localVal, featureMetas[j], zeroAsMissing);

						if (!useMissing && isMissing) {
							throw new IllegalArgumentException(
								"Find the missing value in data. " +
									"Maybe you could open the useMissing to deal with the missing value"
							);
						}

						if (isMissing && featureMetas[j].getType().equals(FeatureMeta.FeatureType.CATEGORICAL)) {
							localVal = featureMetas[j].getMissingIndex();
						}

						this.values[col[j] + rowAcc[j]] = new IndexedValue(localCnt, localVal);
						rowAcc[j]++;
					}
				}

				localCnt++;
			}

			queryIdOffset[querySize] = localCnt;

		} else {
			int localCnt = 0;
			int[] rowAcc = new int[n];

			for (Row localRow : rawData) {
				Vector vector = VectorUtil.getVector(localRow.getField(0));

				if (vector instanceof SparseVector) {
					SparseVector sparseVector = (SparseVector) vector;
					int[] indices = sparseVector.getIndices();

					for (int index : indices) {
						rowAcc[index] += 1;
					}

					nnz += sparseVector.getValues().length;
				} else {
					DenseVector denseVector = (DenseVector) vector;

					double[] vals = denseVector.getData();
					for (int i = 0; i < vals.length; ++i) {
						rowAcc[i] += 1;
					}

					nnz += vals.length;
				}

				labels[localCnt] = ((Number) localRow.getField(localRow.getArity() - 1)).doubleValue();
				localCnt++;
			}

			for (int i = 0; i < n; i++) {
				col[i + 1] = rowAcc[i] + col[i];
			}

			values = new IndexedValue[nnz];
			orderedIndices = new Integer[nnz];

			Arrays.fill(rowAcc, 0);

			localCnt = 0;
			for (Row localRow : rawData) {
				Vector vector = VectorUtil.getVector(localRow.getField(0));
				if (vector instanceof SparseVector) {
					SparseVector sparseVector = (SparseVector) vector;
					int[] indices = sparseVector.getIndices();
					double[] values = sparseVector.getValues();

					for (int j = 0; j < indices.length; ++j) {
						int localCol = indices[j];
						double localVal = values[j];

						boolean isMissing = Preprocessing.isMissing(localVal, featureMetas[j], zeroAsMissing);

						if (!useMissing && isMissing) {
							throw new IllegalArgumentException(
								"Find the missing value in data. " +
									"Maybe you could open the useMissing to deal with the missing value"
							);
						}

						if (isMissing && featureMetas[j].getType().equals(FeatureMeta.FeatureType.CATEGORICAL)) {
							localVal = featureMetas[localCol].getMissingIndex();
						}

						this.values[col[localCol] + rowAcc[localCol]] = new IndexedValue(localCnt, localVal);
						rowAcc[localCol]++;
					}

				} else {
					DenseVector denseVector = (DenseVector) vector;
					double[] values = denseVector.getData();

					for (int j = 0; j < values.length; ++j) {
						double localVal = values[j];

						boolean isMissing = Preprocessing.isMissing(localVal, featureMetas[j], zeroAsMissing);

						if (!useMissing && isMissing) {
							throw new IllegalArgumentException(
								"Find the missing value in data. " +
									"Maybe you could open the useMissing to deal with the missing value"
							);
						}

						if (isMissing && featureMetas[j].getType().equals(FeatureMeta.FeatureType.CATEGORICAL)) {
							localVal = featureMetas[j].getMissingIndex();
						}

						this.values[col[j] + rowAcc[j]] = new IndexedValue(localCnt, localVal);
						rowAcc[j]++;
					}
				}

				localCnt++;
			}
		}
	}

	private double val(int colIndex, int rowIndex) {
		int hit = Arrays.binarySearch(row, col[colIndex], col[colIndex + 1], rowIndex);

		return hit < 0 ? 0.0 : val[hit];
	}

	private double valValues(int colIndex, int rowIndex) {
		int hit = Arrays.binarySearch(
			orderedIndices, col[colIndex], col[colIndex + 1], rowIndex,
			(o1, o2) -> Integer.compare(values[o1].index, o2)
		);

		return hit < 0 ? 0.0 : values[orderedIndices[hit]].val;
	}

	@Override
	public int splitInstances(
		Node node, EpsilonApproQuantile.WQSummary summary, int[] indices, Slice slice) {
		if (values != null) {
			if (featureMetas[node.getFeatureIndex()].getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
				int featureIndex = node.getFeatureIndex();

				int lstart = slice.start;
				int lend = slice.end;
				lend--;
				while (lstart <= lend) {
					while (lstart <= lend &&
						DataUtil.leftUseSummary(
							valValues(featureIndex, indices[lstart]),
							node, summary,
							featureMetas[node.getFeatureIndex()],
							zeroAsMissing)) {

						lstart++;
					}

					while (lstart <= lend &&
						!DataUtil.leftUseSummary(
							valValues(featureIndex, indices[lend]),
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
				int featureIndex = node.getFeatureIndex();

				int lstart = slice.start;
				int lend = slice.end;
				lend--;
				while (lstart <= lend) {
					while (lstart <= lend &&
						DataUtil.left(
							(int) valValues(featureIndex, indices[lstart]),
							node, featureMetas[node.getFeatureIndex()])) {

						lstart++;
					}

					while (lstart <= lend &&
						!DataUtil.left(
							(int) valValues(featureIndex, indices[lend]),
							node, featureMetas[node.getFeatureIndex()])) {

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
			int featureIndex = node.getFeatureIndex();

			int lstart = slice.start;
			int lend = slice.end;
			lend--;
			while (lstart <= lend) {
				while (lstart <= lend &&
					DataUtil.left((int) val(featureIndex, indices[lstart]), node,
						featureMetas[node.getFeatureIndex()])) {
					lstart++;
				}

				while (lstart <= lend &&
					!DataUtil.left((int) val(featureIndex, indices[lend]), node,
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
	}

	@Override
	public void sort() {
		for (int i = 0; i < n; ++i) {
			if (featureMetas[i].getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
				Arrays.sort(
					values,
					col[featureMetas[i].getIndex()],
					col[featureMetas[i].getIndex() + 1],
					Comparator.comparingDouble(o -> o.val)
				);

				int start = col[featureMetas[i].getIndex()];
				int end = col[featureMetas[i].getIndex() + 1];

				for (int j = start; j < end; ++j) {
					orderedIndices[j] = j;
				}

				Arrays.sort(orderedIndices, start, end, Comparator.comparingInt(o -> values[o].index));
			}
		}
	}

	@Override
	public EpsilonApproQuantile.SketchEntry[] createWQSummary(
		int maxSize, double eps, EpsilonApproQuantile.SketchEntry[] buffer, double[] dynamicWeights,
		BitSet validFlags) {

		for (int i = 0, index = 0; i < n; ++i) {
			if (featureMetas[i].getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
				int featureIndex = featureMetas[i].getIndex();

				buffer[index].sumTotal = 0.0;

				for (int j = col[featureIndex]; j < col[featureIndex + 1]; ++j) {
					IndexedValue v = values[j];

					if (validFlags.get(v.index)) {
						buffer[index].sumTotal += dynamicWeights[v.index];
					}
				}

				index++;
			}
		}

		for (int i = 0, index = 0; i < n; ++i) {
			if (featureMetas[i].getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
				int featureIndex = featureMetas[i].getIndex();
				int start = col[featureIndex];
				int end = col[featureIndex + 1];

				EpsilonApproQuantile.SketchEntry entry = buffer[index];

				if (start == end) {
					index++;
					//empty
					continue;
				}

				if (values[start].val == values[end - 1].val) {
					// one value
					entry.push(values[start].val, entry.sumTotal, maxSize);
				} else {
					entry.init(maxSize);

					for (int j = start; j < end; ++j) {
						IndexedValue v = values[j];

						if (validFlags.get(v.index)) {
							entry.push(v.val, dynamicWeights[v.index], maxSize);
						}
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

		double[] total = new double[nodeSize * STEP];

		if (useInstanceCount) {
			for (int i = 0; i < m; ++i) {
				if (nodeIdCache[i] < 0) {
					continue;
				}

				final int node = nodeIdCache[i];
				final int counterIndex = node * STEP;

				total[counterIndex] += gradients[i];
				total[counterIndex + 1] += hessions[i];
				total[counterIndex + 2] += weights[i];
				if (weights[i] > PaiCriteria.PAI_EPS) {
					total[counterIndex + 3] += 1.0;
				}
			}
		} else {
			for (int i = 0; i < m; ++i) {
				if (nodeIdCache[i] < 0) {
					continue;
				}

				final int node = nodeIdCache[i];
				final int counterIndex = node * STEP;

				total[counterIndex] += gradients[i];
				total[counterIndex + 1] += hessions[i];
				total[counterIndex + 2] += weights[i];
				total[counterIndex + 3] += 1.0;
			}
		}

		int continuousFeatureIndex = -1;

		for (int i = 0; i < getN(); ++i) {
			boolean isContinuous = featureMetas[i].getType()
				.equals(FeatureMeta.FeatureType.CONTINUOUS);

			if (isContinuous) {
				continuousFeatureIndex++;
			}

			futures[i] = null;
			if (!featureValid.get(i)) {
				continue;
			}

			if (isContinuous) {
				EpsilonApproQuantile.WQSummary summary = summaries[continuousFeatureIndex];

				final int featureSize = DataUtil.getFeatureCategoricalSize(featureMetas[i], useMissing);
				final int sparseZeroIndex = featureMetas[i].getSparseZeroIndex();
				final int featureOffset = validFeatureOffset[i] * nodeSize * STEP;
				final int nextFeatureOffset = featureOffset + featureSize * nodeSize * STEP;
				final int colIndex = i;

				if (useInstanceCount) {
					futures[i] = executorService.submit(() -> {
						int cursor = 0;
						Arrays.fill(featureSplitHistogram, featureOffset, nextFeatureOffset, 0.0);
						for (int j = col[colIndex]; j < col[colIndex + 1]; ++j) {
							final int localRowIndex = values[j].index;

							if (nodeIdCache[localRowIndex] < 0) {
								continue;
							}

							while (cursor < summary.entries.size() && summary.entries.get(cursor).value
								< values[j].val) {
								cursor++;
							}

							final int localValue = cursor;

							final int node = nodeIdCache[localRowIndex];
							final int counterIndex = (node * featureSize + localValue) * STEP + featureOffset;
							featureSplitHistogram[counterIndex] += gradients[localRowIndex];
							featureSplitHistogram[counterIndex + 1] += hessions[localRowIndex];
							featureSplitHistogram[counterIndex + 2] += weights[localRowIndex];
							if (weights[localRowIndex] > PaiCriteria.PAI_EPS) {
								featureSplitHistogram[counterIndex + 3] += 1.0;
							}
						}

						double[] others = new double[nodeSize * STEP];
						Arrays.fill(others, 0.0);

						for (int j = 0; j < nodeSize; ++j) {
							final int nodeStart = j * STEP;

							for (int z = 0; z < featureSize; ++z) {
								final int counterIndex = featureOffset + (j * featureSize + z) * STEP;

								others[nodeStart] += featureSplitHistogram[counterIndex];
								others[nodeStart + 1] += featureSplitHistogram[counterIndex + 1];
								others[nodeStart + 2] += featureSplitHistogram[counterIndex + 2];
								others[nodeStart + 3] += featureSplitHistogram[counterIndex + 3];
							}

							final int counterIndex = featureOffset + (j * featureSize + sparseZeroIndex) * STEP;

							featureSplitHistogram[counterIndex] += total[nodeStart] - others[nodeStart];
							featureSplitHistogram[counterIndex + 1] += total[nodeStart + 1] - others[nodeStart + 1];
							featureSplitHistogram[counterIndex + 2] += total[nodeStart + 2] - others[nodeStart + 2];
							featureSplitHistogram[counterIndex + 3] += total[nodeStart + 3] - others[nodeStart + 3];
						}
					});
				} else {
					futures[i] = executorService.submit(() -> {
						int cursor = 0;
						Arrays.fill(featureSplitHistogram, featureOffset, nextFeatureOffset, 0.0);
						for (int j = col[colIndex]; j < col[colIndex + 1]; ++j) {
							final int localRowIndex = values[j].index;

							if (nodeIdCache[localRowIndex] < 0) {
								continue;
							}

							while (cursor < summary.entries.size() && summary.entries.get(cursor).value
								< values[j].val) {
								cursor++;
							}

							final int localValue = cursor;

							final int node = nodeIdCache[localRowIndex];
							final int counterIndex = (node * featureSize + localValue) * STEP + featureOffset;
							featureSplitHistogram[counterIndex] += gradients[localRowIndex];
							featureSplitHistogram[counterIndex + 1] += hessions[localRowIndex];
							featureSplitHistogram[counterIndex + 2] += weights[localRowIndex];
							featureSplitHistogram[counterIndex + 3] += 1.0;
						}

						double[] others = new double[nodeSize * STEP];
						Arrays.fill(others, 0.0);

						for (int j = 0; j < nodeSize; ++j) {
							final int nodeStart = j * STEP;

							for (int z = 0; z < featureSize; ++z) {
								final int counterIndex = featureOffset + (j * featureSize + z) * STEP;

								others[nodeStart] += featureSplitHistogram[counterIndex];
								others[nodeStart + 1] += featureSplitHistogram[counterIndex + 1];
								others[nodeStart + 2] += featureSplitHistogram[counterIndex + 2];
								others[nodeStart + 3] += featureSplitHistogram[counterIndex + 3];
							}

							final int counterIndex = featureOffset + (j * featureSize + sparseZeroIndex) * STEP;

							featureSplitHistogram[counterIndex] += total[nodeStart] - others[nodeStart];
							featureSplitHistogram[counterIndex + 1] += total[nodeStart + 1] - others[nodeStart + 1];
							featureSplitHistogram[counterIndex + 2] += total[nodeStart + 2] - others[nodeStart + 2];
							featureSplitHistogram[counterIndex + 3] += total[nodeStart + 3] - others[nodeStart + 3];
						}
					});
				}
			} else {
				throw new IllegalArgumentException("Unsupported categorical now.");
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

	final static int STEP = 4;

	private static class RowIndexRange {
		int cursor;
		int[] aligned;
		int validInstanceCount;

		RowIndexRange(int[] aligned, int validInstanceCount) {
			this.cursor = 0;
			this.aligned = aligned;
			this.validInstanceCount = validInstanceCount;
		}

		boolean invalid(int rowIndex) {
			while (cursor < validInstanceCount
				&& rowIndex < aligned[cursor]) {
				cursor++;
			}

			if (cursor < validInstanceCount
				&& rowIndex > aligned[cursor]) {
				while (cursor < validInstanceCount
					&& rowIndex > aligned[cursor]) {
					cursor++;
				}
			}

			return cursor >= validInstanceCount || rowIndex != aligned[cursor++];
		}
	}

	@Override
	public void constructHistogram(
		boolean useInstanceCount,
		int nodeSize,
		int validInstanceCount,
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

		double[] total = new double[nodeSize * STEP];

		if (useInstanceCount) {
			for (int i = 0; i < validInstanceCount; ++i) {
				final int node = nodeIdCache[aligned[i]];
				final int counterIndex = node * STEP;

				total[counterIndex] += gradients[aligned[i]];
				total[counterIndex + 1] += hessions[aligned[i]];
				total[counterIndex + 2] += weights[aligned[i]];
				if (weights[aligned[i]] > PaiCriteria.PAI_EPS) {
					total[counterIndex + 3] += 1.0;
				}
			}
		} else {
			for (int i = 0; i < validInstanceCount; ++i) {
				final int node = nodeIdCache[aligned[i]];
				final int counterIndex = node * STEP;

				total[counterIndex] += gradients[aligned[i]];
				total[counterIndex + 1] += hessions[aligned[i]];
				total[counterIndex + 2] += weights[aligned[i]];
				total[counterIndex + 3] += 1.0;
			}
		}

		for (int i = 0; i < getN(); ++i) {
			futures[i] = null;
			if (!featureValid.get(i)) {
				continue;
			}

			final int featureSize = DataUtil.getFeatureCategoricalSize(featureMetas[i], useMissing);
			final int sparseZeroIndex = featureMetas[i].getSparseZeroIndex();
			final int featureOffset = validFeatureOffset[i] * nodeSize * STEP;
			final int nextFeatureOffset = featureOffset + featureSize * nodeSize * STEP;
			final int colIndex = i;

			if (useInstanceCount) {
				futures[i] = executorService.submit(() -> {
					RowIndexRange range = new RowIndexRange(aligned, validInstanceCount);

					Arrays.fill(featureSplitHistogram, featureOffset, nextFeatureOffset, 0.0);
					for (int j = col[colIndex]; j < col[colIndex + 1]; ++j) {
						final int localRowIndex = row[j];
						final int localValue = val[j];

						if (range.invalid(localRowIndex)) {
							continue;
						}

						final int node = nodeIdCache[localRowIndex];
						final int counterIndex = (node * featureSize + localValue) * STEP + featureOffset;
						featureSplitHistogram[counterIndex] += gradients[localRowIndex];
						featureSplitHistogram[counterIndex + 1] += hessions[localRowIndex];
						featureSplitHistogram[counterIndex + 2] += weights[localRowIndex];
						if (weights[localRowIndex] > PaiCriteria.PAI_EPS) {
							featureSplitHistogram[counterIndex + 3] += 1.0;
						}
					}

					double[] others = new double[nodeSize * STEP];
					Arrays.fill(others, 0.0);

					for (int j = 0; j < nodeSize; ++j) {
						final int nodeStart = j * STEP;

						for (int z = 0; z < featureSize; ++z) {
							final int counterIndex = featureOffset + (j * featureSize + z) * STEP;

							others[nodeStart] += featureSplitHistogram[counterIndex];
							others[nodeStart + 1] += featureSplitHistogram[counterIndex + 1];
							others[nodeStart + 2] += featureSplitHistogram[counterIndex + 2];
							others[nodeStart + 3] += featureSplitHistogram[counterIndex + 3];
						}

						final int counterIndex = featureOffset + (j * featureSize + sparseZeroIndex) * STEP;

						featureSplitHistogram[counterIndex] += total[nodeStart] - others[nodeStart];
						featureSplitHistogram[counterIndex + 1] += total[nodeStart + 1] - others[nodeStart + 1];
						featureSplitHistogram[counterIndex + 2] += total[nodeStart + 2] - others[nodeStart + 2];
						featureSplitHistogram[counterIndex + 3] += total[nodeStart + 3] - others[nodeStart + 3];
					}
				});
			} else {
				futures[i] = executorService.submit(() -> {
					RowIndexRange range = new RowIndexRange(aligned, validInstanceCount);
					Arrays.fill(featureSplitHistogram, featureOffset, nextFeatureOffset, 0.0);
					for (int j = col[colIndex]; j < col[colIndex + 1]; ++j) {
						final int localRowIndex = row[j];
						final int localValue = val[j];

						if (range.invalid(localRowIndex)) {
							continue;
						}

						final int node = nodeIdCache[localRowIndex];
						final int counterIndex = (node * featureSize + localValue) * STEP + featureOffset;
						featureSplitHistogram[counterIndex] += gradients[localRowIndex];
						featureSplitHistogram[counterIndex + 1] += hessions[localRowIndex];
						featureSplitHistogram[counterIndex + 2] += weights[localRowIndex];
						featureSplitHistogram[counterIndex + 3] += 1.0;
					}

					double[] others = new double[nodeSize * STEP];
					Arrays.fill(others, 0.0);

					for (int j = 0; j < nodeSize; ++j) {
						final int nodeStart = j * STEP;

						for (int z = 0; z < featureSize; ++z) {
							final int counterIndex = featureOffset + (j * featureSize + z) * STEP;

							others[nodeStart] += featureSplitHistogram[counterIndex];
							others[nodeStart + 1] += featureSplitHistogram[counterIndex + 1];
							others[nodeStart + 2] += featureSplitHistogram[counterIndex + 2];
							others[nodeStart + 3] += featureSplitHistogram[counterIndex + 3];
						}

						final int counterIndex = featureOffset + (j * featureSize + sparseZeroIndex) * STEP;

						featureSplitHistogram[counterIndex] += total[nodeStart] - others[nodeStart];
						featureSplitHistogram[counterIndex + 1] += total[nodeStart + 1] - others[nodeStart + 1];
						featureSplitHistogram[counterIndex + 2] += total[nodeStart + 2] - others[nodeStart + 2];
						featureSplitHistogram[counterIndex + 3] += total[nodeStart + 3] - others[nodeStart + 3];
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
