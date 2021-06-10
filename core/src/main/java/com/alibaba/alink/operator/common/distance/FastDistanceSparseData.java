package com.alibaba.alink.operator.common.distance;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.DenseMatrix;

import java.util.HashMap;
import java.util.List;

/**
 * Save the data for calculating distance fast. The FastDistanceMatrixData saves several dense vectors in a single
 * matrix.
 * The vectors are organized in columns, which means each column is a single vector. For example, vec1: 0,1,2, vec2:
 * 3,4,5, vec3: 6,7,8, then the data in
 * matrix is organized as: vec1,vec2,vec3. And the data array in <code>vectors</code> is {0,1,2,3,4,5,6,7,8}.
 */
public class FastDistanceSparseData extends FastDistanceData {
	private static final long serialVersionUID = 5391411198894516046L;
	/**
	 * Stores several dense vectors in columns. For example, if the vectorSize is n, and matrix saves m vectors, then
	 * the
	 * number of rows of <code>vectors</code> is n and the number of cols of <code>vectors</code> is m.
	 */
	final int[][] indices;
	final double[][] values;

	final int vectorNum;

	/**
	 * Stores some extra info extracted from the vector. It's also organized in columns. For example, if we want to
	 * save
	 * the L1 norm and L2 norm of the vector, then the two values are viewed as a two-dimension label vector. We
	 * organize
	 * the norm vectors together to get the <code>label</code>. If the number of cols of <code>vectors</code> is m,
	 * then in this case the dimension of <code>label</code> is 2 * m.
	 */
	DenseMatrix label;

	public FastDistanceSparseData(List <Integer>[] indices, List <Double>[] values, int vectorNum) {
		this(indices, values, vectorNum, null);
	}

	public FastDistanceSparseData(List <Integer>[] indices, List <Double>[] values, int vectorNum, Row[] rows) {
		super(rows);
		Preconditions.checkNotNull(indices, "Index should not be null!");
		Preconditions.checkNotNull(values, "Index should not be null!");
		Preconditions.checkArgument(indices.length == values.length, "Index size not equal to value size!");
		this.indices = new int[indices.length][];
		this.values = new double[values.length][];
		for (int i = 0; i < this.indices.length; i++) {
			if (indices[i] != null) {
				Preconditions.checkNotNull(values[i], "Index size not equal to value size!");
				Preconditions.checkArgument(indices[i].size() == values[i].size(),
					"Index size not equal to value size!");
				this.indices[i] = new int[indices[i].size()];
				this.values[i] = new double[indices[i].size()];
				for (int j = 0; j < indices[i].size(); j++) {
					this.indices[i][j] = indices[i].get(j);
					this.values[i][j] = values[i].get(j);
				}
			}
		}
		this.vectorNum = vectorNum;
	}

	public FastDistanceSparseData(int[][] indices, double[][] values, int vectorNum, Row[] rows) {
		super(rows);
		this.indices = indices;
		this.values = values;
		this.vectorNum = vectorNum;
	}

	public FastDistanceSparseData(HashMap <Integer, Tuple2 <List <Integer>, List <Double>>> indexHashMap,
								  int vectorNum) {
		this(indexHashMap, vectorNum, null);
	}

	public FastDistanceSparseData(HashMap <Integer, Tuple2 <List <Integer>, List <Double>>> indexHashMap, int
		vectorNum,
								  Row[] rows) {
		super(rows);
		Preconditions.checkNotNull(indexHashMap, "IndexHashMap should not be null!");
		int vectorSize = indexHashMap.keySet().stream().max(Integer::compareTo).get() + 1;
		System.out.println("vectorSize:" + vectorSize);
		this.indices = new int[vectorSize][];
		this.values = new double[vectorSize][];
		for (int i = 0; i < this.indices.length; i++) {
			Tuple2 <List <Integer>, List <Double>> t = indexHashMap.get(i);
			if (t != null) {
				Preconditions.checkArgument(t.f0 != null && t.f1 != null && t.f0.size() == t.f1.size(),
					"Index size not equal to value size!");
				this.indices[i] = new int[t.f0.size()];
				this.values[i] = new double[t.f0.size()];
				for (int j = 0; j < t.f0.size(); j++) {
					this.indices[i][j] = t.f0.get(j);
					this.values[i][j] = t.f1.get(j);
				}
			}
		}
		this.vectorNum = vectorNum;
	}

	/**
	 * @param matrixData
	 */
	public FastDistanceSparseData(FastDistanceSparseData matrixData) {
		super(matrixData);
		this.indices = matrixData.indices.clone();
		this.values = matrixData.values.clone();
		this.label = (null == matrixData.label) ? null : matrixData.label.clone();
		this.vectorNum = matrixData.vectorNum;
	}

	public int[][] getIndices() {
		return indices;
	}

	public double[][] getValues() {
		return values;
	}

	public DenseMatrix getLabel() {
		return label;
	}

	@Override
	public String toString() {
		Params params = new Params();
		params.set("indices", indices);
		params.set("values", values);
		params.set("vectorNum", vectorNum);
		params.set("label", label);
		params.set("rows", rows);
		return params.toJson();
	}

	public static FastDistanceSparseData fromString(String s) {
		Params params = Params.fromJson(s);
		int[][] indices = params.get("indices", int[][].class);
		double[][] values = params.get("values", double[][].class);

		FastDistanceSparseData sparseData = new FastDistanceSparseData(indices, values,
			params.get("vectorNum", int.class), parseRowArrayCompatible(params));
		sparseData.label = params.get("label", DenseMatrix.class);

		return sparseData;
	}
}
