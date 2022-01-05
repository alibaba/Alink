package com.alibaba.alink.common.linalg;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A utility class that provides operations over {@link DenseVector}, {@link SparseVector} and {@link DenseMatrix}.
 */
public class MatVecOp {
	/**
	 * compute vec1 + vec2 .
	 */
	public static Vector plus(Vector vec1, Vector vec2) {
		Preconditions.checkArgument(vec1.size() == vec2.size());
		return vec1.plus(vec2);
	}

	/**
	 * compute vec1 - vec2 .
	 */
	public static Vector minus(Vector vec1, Vector vec2) {
		Preconditions.checkArgument(vec1.size() == vec2.size());
		return vec1.minus(vec2);
	}

	/**
	 * Compute vec1 \cdot vec2 .
	 */
	public static double dot(Vector vec1, Vector vec2) {
		Preconditions.checkArgument(vec1.size() == vec2.size());
		return vec1.dot(vec2);
	}

	/**
	 * Compute cosine of vec1 and vec2 .
	 */
	public static double cosine(Vector vec1, Vector vec2) {
		return vec1.dot(vec2) / (vec1.normL2() * vec2.normL2());
	}

	/**
	 * Compute euclid distance of vec1 and vec2.
	 */
	public static double euclidDistance(Vector vec1, Vector vec2) {
		return MatVecOp.minus(vec1, vec2).normL2();
	}

	/**
	 * Compute || vec1 - vec2 ||_1    .
	 */
	public static double sumAbsDiff(Vector vec1, Vector vec2) {
		if (vec1 instanceof DenseVector) {
			if (vec2 instanceof DenseVector) {
				return MatVecOp.applySum((DenseVector) vec1, (DenseVector) vec2, (a, b) -> Math.abs(a - b));
			} else {
				return MatVecOp.applySum((DenseVector) vec1, (SparseVector) vec2, (a, b) -> Math.abs(a - b));
			}
		} else {
			if (vec2 instanceof DenseVector) {
				return MatVecOp.applySum((SparseVector) vec1, (DenseVector) vec2, (a, b) -> Math.abs(a - b));
			} else {
				return MatVecOp.applySum((SparseVector) vec1, (SparseVector) vec2, (a, b) -> Math.abs(a - b));
			}
		}
	}

	/**
	 * Compute || vec1 - vec2 ||_2^2   .
	 */
	public static double sumSquaredDiff(Vector vec1, Vector vec2) {
		if (vec1 instanceof DenseVector) {
			if (vec2 instanceof DenseVector) {
				return MatVecOp.applySum((DenseVector) vec1, (DenseVector) vec2, (a, b) -> (a - b) * (a - b));
			} else {
				return MatVecOp.applySum((DenseVector) vec1, (SparseVector) vec2, (a, b) -> (a - b) * (a - b));
			}
		} else {
			if (vec2 instanceof DenseVector) {
				return MatVecOp.applySum((SparseVector) vec1, (DenseVector) vec2, (a, b) -> (a - b) * (a - b));
			} else {
				return MatVecOp.applySum((SparseVector) vec1, (SparseVector) vec2, (a, b) -> (a - b) * (a - b));
			}
		}
	}

	/**
	 * vec = [vec1, vec2]
	 */
	public static Vector mergeVector(Vector vec1, Vector vec2) {
		if (vec1 instanceof DenseVector && vec2 instanceof DenseVector) {
			DenseVector vec = new DenseVector(vec1.size() + vec2.size());
			System.arraycopy(((DenseVector) vec1).getData(), 0, vec.getData(), 0, vec1.size());
			System.arraycopy(((DenseVector) vec2).getData(), 0, vec.getData(), vec1.size(), vec2.size());
			return vec;
		} else if (vec1 instanceof SparseVector && vec2 instanceof SparseVector) {
			int vec1Size = ((SparseVector) vec1).getIndices().length;
			int vec2Size = ((SparseVector) vec2).getIndices().length;
			double[] values = new double[vec1Size + vec2Size];
			int[] indices = new int[values.length];
			System.arraycopy(((SparseVector) vec1).getValues(), 0, values, 0, vec1Size);
			System.arraycopy(((SparseVector) vec2).getValues(), 0, values, vec1Size, vec2Size);
			System.arraycopy(((SparseVector) vec1).getIndices(), 0, indices, 0, vec1Size);
			System.arraycopy(((SparseVector) vec2).getIndices(), 0, indices, vec1Size, vec2Size);
			for (int i = 0; i < vec2Size; ++i) {
				indices[vec1Size + i] += vec1.size();
			}
			return new SparseVector(vec1.size() + vec2.size(), indices, values);
		} else if (vec1 instanceof SparseVector && vec2 instanceof DenseVector) {
			int vec1Size = ((SparseVector) vec1).getIndices().length;
			int vec2Size = vec2.size();
			double[] values = new double[vec1Size + vec2Size];
			int[] indices = new int[values.length];
			System.arraycopy(((SparseVector) vec1).getValues(), 0, values, 0, vec1Size);
			System.arraycopy(((DenseVector) vec2).getData(), 0, values, vec1Size, vec2Size);
			System.arraycopy(((SparseVector) vec1).getIndices(), 0, indices, 0, vec1Size);
			for (int i = 0; i < vec2Size; ++i) {
				indices[vec1Size + i] = vec1.size() + i;
			}
			return new SparseVector(vec1.size() + vec2.size(), indices, values);
		} else if (vec1 instanceof DenseVector && vec2 instanceof SparseVector) {
			int vec1Size = vec1.size();
			int vec2Size = ((SparseVector) vec2).getIndices().length;
			double[] values = new double[vec1Size + vec2Size];
			int[] indices = new int[values.length];
			System.arraycopy(((DenseVector) vec1).getData(), 0, values, 0, vec1Size);
			for (int i = 0; i < vec1Size; ++i) {
				indices[i] = i;
			}
			System.arraycopy(((SparseVector) vec2).getValues(), 0, values, vec1Size, vec2Size);
			System.arraycopy(((SparseVector) vec2).getIndices(), 0, indices, vec1Size, vec2Size);
			for (int i = 0; i < vec2Size; ++i) {
				indices[vec1Size + i] += vec1Size;
			}

			return new SparseVector(vec1.size() + vec2.size(), indices, values);
		} else {
			throw new RuntimeException("not support yet.");
		}
	}

	public static Vector elementWiseMultiply(Vector vec1, Vector vec2) {
		if (vec1 instanceof DenseVector && vec2 instanceof DenseVector) {
			return MatVecOp.apply((DenseVector) vec1, (DenseVector) vec2, (a, b) -> (a * b));
		} else if (vec1 instanceof SparseVector && vec2 instanceof SparseVector) {
			return MatVecOp.apply((SparseVector) vec1, (SparseVector) vec2, (a, b) -> (a * b));
		} else if (vec1 instanceof SparseVector && vec2 instanceof DenseVector) {
			return MatVecOp.apply((SparseVector) vec1, (DenseVector) vec2, (a, b) -> (a * b));
		} else if (vec1 instanceof DenseVector && vec2 instanceof SparseVector) {
			return MatVecOp.apply((SparseVector) vec2, (DenseVector) vec1, (a, b) -> (a * b));
		} else {
			throw new RuntimeException("not support yet.");
		}
	}

	/**
	 * y = func(x).
	 */
	public static void apply(DenseMatrix x, DenseMatrix y, Function <Double, Double> func) {
		assert (x.m == y.m && x.n == y.n) : "x and y size mismatched.";
		double[] xdata = x.data;
		double[] ydata = y.data;
		for (int i = 0; i < xdata.length; i++) {
			ydata[i] = func.apply(xdata[i]);
		}
	}

	/**
	 * y = func(x1, x2).
	 */
	public static void apply(DenseMatrix x1, DenseMatrix x2, DenseMatrix y,
							 BiFunction <Double, Double, Double> func) {
		assert (x1.m == y.m && x1.n == y.n) : "x1 and y size mismatched.";
		assert (x2.m == y.m && x2.n == y.n) : "x2 and y size mismatched.";
		double[] x1data = x1.data;
		double[] x2data = x2.data;
		double[] ydata = y.data;
		for (int i = 0; i < ydata.length; i++) {
			ydata[i] = func.apply(x1data[i], x2data[i]);
		}
	}

	/**
	 * y = func(x).
	 */
	public static void apply(DenseVector x, DenseVector y, Function <Double, Double> func) {
		assert (x.data.length == y.data.length) : "x and y size mismatched.";
		for (int i = 0; i < x.data.length; i++) {
			y.data[i] = func.apply(x.data[i]);
		}
	}

	/**
	 * y = func(x1, x2).
	 */
	public static void apply(DenseVector x1, DenseVector x2, DenseVector y,
							 BiFunction <Double, Double, Double> func) {
		assert (x1.data.length == y.data.length) : "x1 and y size mismatched.";
		assert (x2.data.length == y.data.length) : "x1 and y size mismatched.";
		for (int i = 0; i < y.data.length; i++) {
			y.data[i] = func.apply(x1.data[i], x2.data[i]);
		}
	}

	/**
	 * ret = func(x1, x2).
	 */
	public static DenseVector apply(DenseVector x1, DenseVector x2,
									BiFunction <Double, Double, Double> func) {
		assert (x1.data.length == x2.data.length) : "x1 and x2 size mismatched.";
		DenseVector ret = new DenseVector(x1.data.length);
		for (int i = 0; i < x1.data.length; i++) {
			ret.data[i] = func.apply(x1.data[i], x2.data[i]);
		}
		return ret;
	}

	/**
	 * ret = func(x1, x2).
	 */
	public static DenseVector apply(SparseVector x1, DenseVector x2,
									BiFunction <Double, Double, Double> func) {
		assert (x1.size() == x2.size()) : "x1 and x2 size mismatched.";
		DenseVector ret = new DenseVector(x1.size());
		for (int i = 0; i < x1.getValues().length; i++) {
			ret.data[i] = func.apply(x1.get(i), x2.data[i]);
		}
		return ret;
	}

	/**
	 * Create a new {@link SparseVector} by element wise operation between two {@link SparseVector}s. y = func(x1, x2).
	 */
	public static SparseVector apply(SparseVector x1, SparseVector x2, BiFunction <Double, Double, Double> func) {
		assert (x1.size() == x2.size()) : "x1 and x2 size mismatched.";

		int totNnz = x1.values.length + x2.values.length;
		int p0 = 0;
		int p1 = 0;
		while (p0 < x1.values.length && p1 < x2.values.length) {
			if (x1.indices[p0] == x2.indices[p1]) {
				totNnz--;
				p0++;
				p1++;
			} else if (x1.indices[p0] < x2.indices[p1]) {
				p0++;
			} else {
				p1++;
			}
		}

		SparseVector r = new SparseVector(x1.size());
		r.indices = new int[totNnz];
		r.values = new double[totNnz];
		p0 = p1 = 0;
		int pos = 0;
		while (pos < totNnz) {
			if (p0 < x1.values.length && p1 < x2.values.length) {
				if (x1.indices[p0] == x2.indices[p1]) {
					r.indices[pos] = x1.indices[p0];
					r.values[pos] = func.apply(x1.values[p0], x2.values[p1]);
					p0++;
					p1++;
				} else if (x1.indices[p0] < x2.indices[p1]) {
					r.indices[pos] = x1.indices[p0];
					r.values[pos] = func.apply(x1.values[p0], 0.0);
					p0++;
				} else {
					r.indices[pos] = x2.indices[p1];
					r.values[pos] = func.apply(0.0, x2.values[p1]);
					p1++;
				}
				pos++;
			} else {
				if (p0 < x1.values.length) {
					r.indices[pos] = x1.indices[p0];
					r.values[pos] = func.apply(x1.values[p0], 0.0);
					p0++;
					pos++;
					continue;
				}
				if (p1 < x2.values.length) {
					r.indices[pos] = x2.indices[p1];
					r.values[pos] = func.apply(0.0, x2.values[p1]);
					p1++;
					pos++;
				}
			}
		}

		return r;
	}

	/**
	 * \sum_i func(x1_i, x2_i)
	 */
	public static double applySum(DenseVector x1, DenseVector x2, BiFunction <Double, Double, Double> func) {
		assert x1.size() == x2.size() : "x1 and x2 size mismatched.";
		double[] x1data = x1.getData();
		double[] x2data = x2.getData();
		double s = 0.;
		for (int i = 0; i < x1data.length; i++) {
			s += func.apply(x1data[i], x2data[i]);
		}
		return s;
	}

	/**
	 * \sum_i func(x1_i, x2_i)
	 */
	public static double applySum(SparseVector x1, SparseVector x2, BiFunction <Double, Double, Double> func) {
		double s = 0.;
		int p1 = 0;
		int p2 = 0;
		int[] x1Indices = x1.getIndices();
		double[] x1Values = x1.getValues();
		int[] x2Indices = x2.getIndices();
		double[] x2Values = x2.getValues();
		int nnz1 = x1Indices.length;
		int nnz2 = x2Indices.length;
		while (p1 < nnz1 || p2 < nnz2) {
			if (p1 < nnz1 && p2 < nnz2) {
				if (x1Indices[p1] == x2Indices[p2]) {
					s += func.apply(x1Values[p1], x2Values[p2]);
					p1++;
					p2++;
				} else if (x1Indices[p1] < x2Indices[p2]) {
					s += func.apply(x1Values[p1], 0.);
					p1++;
				} else {
					s += func.apply(0., x2Values[p2]);
					p2++;
				}
			} else {
				if (p1 < nnz1) {
					s += func.apply(x1Values[p1], 0.);
					p1++;
				} else { // p2 < nnz2
					s += func.apply(0., x2Values[p2]);
					p2++;
				}
			}
		}
		return s;
	}

	/**
	 * \sum_i func(x1_i, x2_i)
	 */
	public static double applySum(DenseVector x1, SparseVector x2, BiFunction <Double, Double, Double> func) {
		assert x1.size() == x2.size() : "x1 and x2 size mismatched.";
		double s = 0.;
		int p2 = 0;
		int[] x2Indices = x2.getIndices();
		double[] x2Values = x2.getValues();
		int nnz2 = x2Indices.length;
		double[] x1data = x1.getData();
		for (int i = 0; i < x1data.length; i++) {
			if (p2 < nnz2 && x2Indices[p2] == i) {
				s += func.apply(x1data[i], x2Values[p2]);
				p2++;
			} else {
				s += func.apply(x1data[i], 0.);
			}
		}
		return s;
	}

	/**
	 * \sum_i func(x1_i, x2_i)
	 */
	public static double applySum(SparseVector x1, DenseVector x2, BiFunction <Double, Double, Double> func) {
		assert x1.size() == x2.size() : "x1 and x2 size mismatched.";
		double s = 0.;
		int p1 = 0;
		int[] x1Indices = x1.getIndices();
		double[] x1Values = x1.getValues();
		int nnz1 = x1Indices.length;
		double[] x2data = x2.getData();
		for (int i = 0; i < x2data.length; i++) {
			if (p1 < nnz1 && x1Indices[p1] == i) {
				s += func.apply(x1Values[p1], x2data[i]);
				p1++;
			} else {
				s += func.apply(0., x2data[i]);
			}
		}
		return s;
	}

	public static void appendVectorToMatrix(DenseMatrix matrix, boolean trans, int index, Vector vector) {
		if (vector instanceof DenseVector) {
			double[] vectorData = ((DenseVector) vector).getData();
			double[] matrixData = matrix.getData();
			int vectorSize = vectorData.length;
			if (trans) {
				Preconditions.checkArgument(matrix.numCols() == vectorSize,
					"Matrix and vector size mismatched, matrix column number %s, vectorSize %s",
					matrix.numCols(), vectorSize);
				for (double aVectorData : vectorData) {
					matrixData[index] = aVectorData;
					index += vectorSize;
				}
			} else {
				Preconditions.checkArgument(matrix.numRows() == vectorSize,
					"Matrix and vector size mismatched, matrix column number %s, vectorSize %s",
					matrix.numRows(), vectorSize);
				System.arraycopy(vectorData, 0, matrixData, index * vectorSize, vectorSize);
			}
		} else {
			SparseVector sparseVector = (SparseVector) vector;
			int[] indices = sparseVector.getIndices();
			double[] values = sparseVector.getValues();
			double[] matrixData = matrix.getData();
			if (trans) {
				int vectorSize = matrix.numCols();
				for (int j = 0; j < indices.length; j++) {
					Preconditions.checkArgument(indices[j] <= matrix.numCols(), "Index %s out of matrix size %s!",
						indices[j], matrix.numCols());
					matrixData[index + indices[j]] = values[j];
					index += vectorSize;
				}
			} else {
				int startIndex = matrix.numRows() * index;
				sparseVector.forEach((k, v) -> {
					Preconditions.checkArgument(k <= matrix.numRows(), "Index %s out of matrix size %s!", k,
						matrix.numRows());
					matrixData[startIndex + k] = v;
				});
			}
		}
	}

	public static void appendVectorToSparseData(List <Integer>[] indices, List <Double>[] values, int index,
												SparseVector vector) {
		double[] value = vector.getValues();
		int[] key = vector.getIndices();
		for (int j = 0; j < key.length; j++) {
			Preconditions.checkArgument(key[j] < indices.length,
				"SparseVector size not the same, please check the data!");
			if (indices[key[j]] == null) {
				indices[key[j]] = new ArrayList <>();
				values[key[j]] = new ArrayList <>();
			}
			indices[key[j]].add(index);
			values[key[j]].add(value[j]);
		}
	}

	public static void appendVectorToSparseData(HashMap <Integer, Tuple2 <List <Integer>, List <Double>>> indexHashMap,
												int index, SparseVector vector) {
		double[] value = vector.getValues();
		int[] key = vector.getIndices();
		for (int j = 0; j < key.length; j++) {
			Tuple2 <List <Integer>, List <Double>> t = indexHashMap.computeIfAbsent(key[j],
				k -> Tuple2.of(new ArrayList <>(), new ArrayList <>()));
			t.f0.add(index);
			t.f1.add(value[j]);
		}
	}
}