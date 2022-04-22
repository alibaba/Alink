package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.outlier.WithMultiVarParams;

import java.util.ArrayList;
import java.util.List;

public class OutlierUtil {

	public static String[] uniformFeatureColsDefaultAsAll(String[] all, String[] features) {
		return features == null ? all : features;
	}

	public static DenseVector rowToDenseVector(Row row, int[] indexes, int m) {
		DenseVector vector = new DenseVector(m);
		for (int j = 0; j < m; j++) {
			vector.set(j, ((Number) row.getField(indexes[j])).doubleValue());
		}
		return vector;
	}

	public static Vector[] getVectors(MTable series, Params params) {
		int n = series.getNumRow();
		Vector[] vectors = new Vector[n];
		if (params.contains(WithMultiVarParams.FEATURE_COLS)) {
			String[] features = uniformFeatureColsDefaultAsAll(
				series.getColNames(), params.get(WithMultiVarParams.FEATURE_COLS)
			);
			int[] indexes = TableUtil.findColIndicesWithAssertAndHint(series.getSchema(), features);
			int m = indexes.length;
			for (int i = 0; i < n; i++) {
				vectors[i] = rowToDenseVector(series.getRow(i), indexes, m);
			}
		} else if (params.contains(WithMultiVarParams.VECTOR_COL)) {
			int colIndex =
				TableUtil.findColIndex(series.getSchema(), params.get(WithMultiVarParams.VECTOR_COL));
			for (int i = 0; i < n; i++) {
				vectors[i] = VectorUtil.getVector(series.getEntry(i, colIndex));
			}
		} else if (params.contains(WithMultiVarParams.TENSOR_COL)) {
			int colIndex =
				TableUtil.findColIndex(series.getSchema(), params.get(WithMultiVarParams.TENSOR_COL));
			for (int i = 0; i < n; i++) {
				vectors[i] = DoubleTensor.of(TensorUtil.getTensor(series.getEntry(i, colIndex)).flatten(0, -1))
					.toVector();
			}
		} else {
			throw new IllegalArgumentException("Must input one of these params {featureCols, vectorCol, tensorCol}.");
		}

		return vectors;
	}

	public static int vectorSize(Vector vector) {
		if (vector.size() >= 0) {
			return vector.size();
		} else {
			int[] indexes = ((SparseVector) vector).getIndices();
			if (indexes.length > 0) {
				return indexes[indexes.length - 1];
			} else {
				return 0;
			}
		}
	}

	public static Tuple2 <Vector[], Integer> selectVectorCol(MTable series, String colName) {
		return selectVectorCol(series, TableUtil.findColIndexWithAssertAndHint(series.getSchema(), colName));
	}

	public static Tuple2 <Vector[], Integer> selectVectorCol(MTable series, int colIndex) {
		int n = series.getNumRow();
		int m = 0;
		Vector[] vectors = new Vector[n];
		for (int i = 0; i < n; i++) {
			m = Math.max(m, vectorSize(VectorUtil.getVector(series.getEntry(i, colIndex))));
		}

		return Tuple2.of(vectors, m);
	}

	public static Row vectorToRow(Vector vector, int m) {
		Row row = new Row(m);
		for (int j = 0; j < m; j++) {
			row.setField(j, vector.get(j));
		}
		return row;
	}

	public static TableSchema vectorSchema(int m) {
		String[] features = new String[m];
		TypeInformation <?>[] types = new TypeInformation[m];
		for (int i = 0; i < m; i++) {
			features[i] = "c" + i;
			types[i] = AlinkTypes.DOUBLE;
		}
		return new TableSchema(features, types);
	}

	public static MTable vectorsToMTable(Vector[] vectors, int m) {
		final int n = vectors.length;
		Row[] rows = new Row[n];
		for (int i = 0; i < n; i++) {
			rows[i] = vectorToRow(vectors[i], m);
		}
		return new MTable(rows, vectorSchema(m));
	}

	public static MTable selectFeatures(MTable series, String[] featureCols) {
		int[] indices = TableUtil.findColIndicesWithAssertAndHint(series.getSchema(), featureCols);
		int m = indices.length;
		int n = series.getNumRow();
		Row[] rows = new Row[n];
		for (int i = 0; i < n; i++) {
			Row r = series.getRow(i);
			rows[i] = new Row(m);
			for (int j = 0; j < m; j++) {
				rows[i].setField(j, r.getField(indices[j]));
			}
		}

		return new MTable(rows, new TableSchema(
			featureCols, TableUtil.findColTypesWithAssertAndHint(series.getSchema(), featureCols)
		));
	}

	public static String[] fillFeatures(String[] colNames, Params params) {
		String[] features = params.get(WithMultiVarParams.FEATURE_COLS);
		if (null == features) {
			features = colNames;
		}

		return features;
	}

	public static final ParamInfo <Integer> MAX_VECTOR_SIZE = ParamInfoFactory
		.createParamInfo("maxVectorSize", Integer.class)
		.setRequired()
		.build();

	public static MTable getMTable(MTable series, Params params) {
		if (params.contains(WithMultiVarParams.FEATURE_COLS)) {
			return selectFeatures(series, fillFeatures(series.getColNames(), params));
		} else if (params.contains(WithMultiVarParams.VECTOR_COL) || params.contains(WithMultiVarParams.TENSOR_COL)) {
			Vector[] vectors = getVectors(series, params);
			int m = 0;
			for (Vector vector : vectors) {
				m = Math.max(m, vectorSize(vector));
			}
			return vectorsToMTable(vectors, m);
		} else {
			throw new IllegalArgumentException("Must input one of these params {featureCols, vectorCol, tensorCol}.");
		}
	}

	public static double[] getNumericArray(MTable series, String selectedCol) {
		int colIndex = TableUtil.findColIndex(series.getSchema(), selectedCol);
		int n = series.getNumRow();
		double[] values = new double[n];
		for (int i = 0; i < n; i++) {
			values[i] = ((Number) series.getEntry(i, colIndex)).doubleValue();
		}
		return values;
	}

	public static List <Double> getNumericList(MTable series, String selectedCol) {
		int colIndex = TableUtil.findColIndex(series.getSchema(), selectedCol);
		int n = series.getNumRow();
		ArrayList <Double> values = new ArrayList(n);
		for (int i = 0; i < n; i++) {
			values.set(i, ((Number) series.getEntry(i, colIndex)).doubleValue());
		}
		return values;
	}

}
