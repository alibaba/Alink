package com.alibaba.alink.operator.local.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.feature.ExclusiveFeatureBundleModelDataConverter;
import com.alibaba.alink.operator.common.feature.FeatureBundles;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.feature.ExclusiveFeatureBundlePredictParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

@NameCn("互斥特征捆绑模型训练")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.ExclusiveFeatureBundle")
public class ExclusiveFeatureBundleTrainLocalOp extends LocalOperator <ExclusiveFeatureBundleTrainLocalOp>
	implements ExclusiveFeatureBundlePredictParams <ExclusiveFeatureBundleTrainLocalOp> {
	public ExclusiveFeatureBundleTrainLocalOp() {this(new Params());}

	public ExclusiveFeatureBundleTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	public ExclusiveFeatureBundleTrainLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);

		final int indexSparseVector = TableUtil.findColIndexWithAssertAndHint(in.getSchema(), getSparseVectorCol());
		MTable mt = in.getOutputTable();
		int nRows = mt.getNumRow();
		SparseVector[] vectors = new SparseVector[nRows];
		for (int i = 0; i < nRows; i++) {
			vectors[i] = VectorUtil.getSparseVector(mt.getEntry(i, indexSparseVector));
		}

		FeatureBundles bundles = extract(vectors);

		ExclusiveFeatureBundleModelDataConverter converter = new ExclusiveFeatureBundleModelDataConverter();
		TableSchema schema = TableUtil.schemaStr2Schema(bundles.getSchemaStr());
		converter.efbColNames = schema.getFieldNames();
		converter.efbColTypes = schema.getFieldTypes();

		RowCollector rowCollector = new RowCollector();
		converter.save(bundles, rowCollector);

		this.setOutputTable(new MTable(rowCollector.getRows(), converter.getModelSchema()));
		return this;
	}

	public static FeatureBundles extract(SparseVector[] vectors) {
		List <int[]> bundles = null;

		//for the col with non-one values, itself will be a bundle.
		Set <Integer> valueNeqOne = new TreeSet <>();
		for (SparseVector vector : vectors) {
			int[] indices = vector.getIndices();
			double[] values = vector.getValues();
			for (int i = 0; i < values.length; i++) {
				if (values[i] != 1) {
					valueNeqOne.add(indices[i]);
				}
			}
		}

		int dim = vectors[0].size();

		bundles = quickCheck(vectors, valueNeqOne, dim);

		if (null == bundles) {
			final int numVectors = vectors.length;

			ArrayList <int[]> indicesList = new ArrayList <>();
			for (SparseVector vector : vectors) {
				indicesList.add(vector.getIndices());
			}

			int[] numUsed = new int[dim];
			for (int[] indices : indicesList) {
				for (int d : indices) {
					numUsed[d] += 1;
				}
			}

			//here we change it's numUsed value, which will be precessed in the next step.
			for (Integer k : valueNeqOne) {
				numUsed[k] = numVectors;
			}

			bundles = new ArrayList <>();
			//将满连接的索引值，每个值作为一个bundle
			for (int d = 0; d < dim; d++) {
				if (numVectors == numUsed[d]) {
					bundles.add(new int[] {d});
				}
			}

			for (int[] bundle : bundles) {
				for (int d : bundle) {
					numUsed[d] = 0;
				}
			}

			int sumLeft = sumNumUsed(numUsed);
			while (0 < sumLeft) {
				List <Integer> newBundle = findNewBundle(indicesList, Arrays.copyOf(numUsed, dim), dim);
				int[] bundle = new int[newBundle.size()];
				for (int i = 0; i < bundle.length; i++) {
					bundle[i] = newBundle.get(i);
				}
				bundles.add(bundle);
				for (int d : bundle) {
					numUsed[d] = 0;
				}
				sumLeft = sumNumUsed(numUsed);
			}
		}

		return new FeatureBundles(dim, bundles);

	}

	static List <int[]> quickCheck(SparseVector[] vectors, Set <Integer> valueNeqOne, int dim) {
		final int m = vectors[0].numberOfValues();
		for (SparseVector vector : vectors) {
			if (m != vector.numberOfValues()) {
				return null;
			}
		}

		int[] rangeMin = Arrays.copyOf(vectors[0].getIndices(), m);
		int[] rangeMax = Arrays.copyOf(vectors[0].getIndices(), m);
		for (SparseVector vector : vectors) {
			int[] indices = vector.getIndices();
			for (int i = 0; i < m; i++) {
				rangeMin[i] = Math.min(rangeMin[i], indices[i]);
				rangeMax[i] = Math.max(rangeMax[i], indices[i]);
			}
		}

		for (int i = 0; i < m - 1; i++) {
			if (rangeMax[i] >= rangeMin[i + 1]) {
				return null;
			}
		}

		ArrayList <int[]> bundles = new ArrayList <>();
		for (int i = 0; i < m - 1; i++) {
			int n = rangeMin[i + 1] - rangeMin[i];
			int[] bundle = new int[n];
			for (int k = 0; k < n; k++) {
				bundle[k] = rangeMin[i] + k;
			}
			bundles.add(bundle);
		}
		for (int i = m - 1; i < m; i++) {
			int n = dim - rangeMin[i];
			int[] bundle = new int[n];
			for (int k = 0; k < n; k++) {
				bundle[k] = rangeMin[i] + k;
			}
			bundles.add(bundle);
		}

		for (Integer k : valueNeqOne) {
			for (int i = 0; i < m; i++) {
				if (rangeMin[i] <= k && k <= rangeMax[i] && rangeMax[i] > rangeMin[i]) {
					return null;
				}
			}
		}

		return bundles;
	}

	static List <Integer> findNewBundle(ArrayList <int[]> indicesList, int[] numUsed, int dim) {
		ArrayList <Integer> newBundle = new ArrayList <>();

		int candidate = findMaxDegree(numUsed);

		newBundle.add(candidate);

		ArrayList <int[]> subList = new ArrayList <>();
		for (int[] indices : indicesList) {
			if (contains(indices, candidate)) {
				for (int d : indices) {
					numUsed[d] = 0;
				}
			} else {
				subList.add(indices);
			}
		}

		int sum = 0;
		int max = 0;
		for (int d = 0; d < dim; d++) {
			if (numUsed[d] > 0) {
				sum += numUsed[d];
				max = Math.max(max, numUsed[d]);
			}
		}

		if (subList.size() == sum) {
			for (int d = 0; d < dim; d++) {
				if (numUsed[d] > 0) {newBundle.add(d);}
			}
		} else if (1 == max) {
			int cnt = 0;
			for (int d = 0; d < dim; d++) {
				if (numUsed[d] > 0 && cnt < subList.size()) {
					newBundle.add(d);
					cnt++;
				}
			}
		} else {
			newBundle.addAll(findNewBundle(subList, numUsed, dim));
		}

		return newBundle;
	}

	static int sumNumUsed(int[] numUsed) {
		int sum = 0;
		for (int v : numUsed) {
			sum += v;
		}
		return sum;
	}

	static int findMaxDegree(int[] numUsed) {
		int k = 0;
		int v = 0;
		for (int d = 0; d < numUsed.length; d++) {
			if (numUsed[d] > v) {
				k = d;
				v = numUsed[d];
			}
		}
		return k;
	}

	static boolean contains(int[] indices, int target) {
		return contains(indices, 0, indices.length, target);
	}

	static boolean contains(int[] indices, int start, int end, int target) {
		if (end - start < 10) {
			for (int d = start; d < end; d++) {
				if (indices[d] == target) {return true;}
			}
			return false;
		} else {
			int mid = (start + end) / 2;
			if (indices[mid] == target) {
				return true;
			} else if (indices[mid] > target) {
				return contains(indices, start, mid, target);
			} else {
				return contains(indices, mid + 1, end, target);
			}
		}
	}
}
