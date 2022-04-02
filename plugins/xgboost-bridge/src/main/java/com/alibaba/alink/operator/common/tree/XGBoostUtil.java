package com.alibaba.alink.operator.common.tree;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import ml.dmlc.xgboost4j.LabeledPoint;

import java.util.Iterator;
import java.util.function.Function;

public class XGBoostUtil {

	public static Iterator <LabeledPoint> asLabeledPointIterator(
		Iterator <Row> rowIterator, Function <Row, LabeledPoint> converter) {

		return new Iterator <LabeledPoint>() {
			@Override
			public boolean hasNext() {
				return rowIterator.hasNext();
			}

			@Override
			public LabeledPoint next() {
				return converter.apply(rowIterator.next());
			}
		};
	}

	public static Function <Row, LabeledPoint> asLabeledPointConverterFunction(
		Function <Row, Row> preprocess,
		Function <Row, Tuple2 <Vector, float[]>> extractor) {

		return row -> {
			Tuple2 <Vector, float[]> extracted = extractor.apply(preprocess.apply(row));
			return asLabeledPoint(extracted.f0, extracted.f1[0]);
		};
	}

	public static LabeledPoint asLabeledPoint(Vector features, float labelValue) {
		if (features == null) {
			throw new IllegalStateException("The value of labeled point should not be null.");
		} else if (features instanceof SparseVector) {
			int[] indices = ((SparseVector) features).getIndices();
			double[] values = ((SparseVector) features).getValues();
			int size = features.size() >= 0 ? features.size()
				: (indices == null || indices.length == 0 ? 0 : indices[indices.length - 1] + 1);
			return new LabeledPoint(labelValue, size, indices, asFloatArray(values));
		} else {
			return new LabeledPoint(
				labelValue,
				features.size(),
				null,
				asFloatArray(((DenseVector) features).getData())
			);
		}
	}

	private static float[] asFloatArray(double[] array) {
		float[] ret = new float[array.length];

		for (int i = 0; i < array.length; ++i) {
			ret[i] = (float) array[i];
		}

		return ret;
	}
}
