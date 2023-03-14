package com.alibaba.alink.operator.common.linear;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Types;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

import java.util.ArrayList;
import java.util.List;

public class LocalLogistRegression {

	/**
	 * @param trainData:   f0 is label, f1 is weight, f2 is feature.
	 * @param indices:     selected indices.
	 * @param hasIntercept
	 * @param l1
	 * @param l2
	 * @param optimMethod
	 * @param constrained
	 * @return
	 */
	public static LinearModelData train(List <Tuple3 <Double, Object, Vector>> trainData,
										int[] indices,
										boolean hasIntercept,
										double l1,
										double l2,
										String optimMethod,
										String constrained) {

		optimMethod = LocalLinearModel.getDefaultOptimMethod(optimMethod, constrained);
		LinearModelType linearModelType = LinearModelType.LR;
		boolean standardization = false;

		Tuple2 <List <Tuple3 <Double, Double, Vector>>, Object[]> dataAndLabelValues = getLabelValues(trainData);

		List <Tuple3 <Double, Double, Vector>> selectedData = new ArrayList <>();
		for (Tuple3 <Double, Double, Vector> data : dataAndLabelValues.f0) {
			if (indices == null) {
				indices = new int[data.f2.size()];
				for (int i = 0; i < indices.length; i++) {
					indices[i] = i;
				}
			}
			selectedData.add(Tuple3.of(data.f0, data.f1, data.f2.slice(indices).prefix(1.0)));
		}

		Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> model =
			LocalLinearModel.train(dataAndLabelValues.f0, indices, linearModelType, optimMethod,
				hasIntercept, standardization, constrained, l1, l2);

		Params meta = new Params()
			.set(ModelParamName.MODEL_NAME, "model")
			.set(ModelParamName.LINEAR_MODEL_TYPE, linearModelType)
			.set(ModelParamName.LABEL_VALUES, dataAndLabelValues.f1)
			.set(ModelParamName.HAS_INTERCEPT_ITEM, hasIntercept)
			.set(ModelParamName.VECTOR_COL_NAME, "features")
			.set(ModelParamName.FEATURE_TYPES, new String[] {})
			.set(LinearTrainParams.LABEL_COL, "label");

		return BaseLinearModelTrainBatchOp.buildLinearModelData(meta,
			null,
			Types.DOUBLE(),
			null,
			hasIntercept,
			false,
			Tuple2.of(model.f0, new double[] {model.f3}));

	}

	/**
	 * @param trainData: f0: weight, f1 label, f2 feature.
	 * @return f0: data, f2: labelValues.
	 */
	public static Tuple2 <List <Tuple3 <Double, Double, Vector>>, Object[]> getLabelValues(
		List <Tuple3 <Double, Object, Vector>> trainData) {
		List <Tuple3 <Double, Double, Vector>> result = new ArrayList <>();
		Object[] labels = new Object[2];

		int length = trainData.size();
		if (length < 1) {
			throw new RuntimeException("row number must be larger than 0.");
		}
		labels[0] = trainData.get(0).f1;
		for (int i = 1; i < length; i++) {
			Object candidate = trainData.get(i).f1;
			if (!candidate.equals(labels[0])) {
				labels[1] = candidate;
				break;
			}
		}
		for (Tuple3 <Double, Object, Vector> value : trainData) {
			double label = labels[0].equals(value.f1.toString()) ? 1 : -1;
			result.add(Tuple3.of(value.f0, label, value.f2));
		}

		return Tuple2.of(result, labels);
	}

	/**
	 * @param model: LinearModelData
	 * @param data:  data
	 * @return
	 */
	public List <Tuple2 <Object, Vector>> predict(LinearModelData model, List <Vector> data) {
		return LocalLinearModel.predict(model, data);
	}

}
