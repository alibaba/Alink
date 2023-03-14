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

public class LocalLinearRegression {

	public static LinearModelData train(List <Tuple3 <Double, Object, Vector>> trainData,
										int[] indices,
										boolean hasIntercept,
										double l1,
										double l2,
										String optimMethod,
										String constrained) {

		optimMethod = LocalLinearModel.getDefaultOptimMethod(optimMethod, constrained);
		LinearModelType linearModelType = LinearModelType.LinearReg;
		boolean standardization = true;

		List <Tuple3 <Double, Double, Vector>> selectedData = new ArrayList <>();

		for (Tuple3 <Double, Object, Vector> data : trainData) {
			if (indices == null) {
				indices = new int[data.f2.size()];
				for (int i = 0; i < indices.length; i++) {
					indices[i] = i;
				}
			}
			selectedData.add(Tuple3.of(data.f0, ((Number) data.f1).doubleValue(), data.f2.slice(indices)));
		}

		//coef, grad, hession, loss
		Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> model = LocalLinearModel.train(selectedData,
			indices, linearModelType, optimMethod,
			hasIntercept, standardization, constrained, l1, l2);

		Params meta = new Params()
			.set(ModelParamName.MODEL_NAME, "model")
			.set(ModelParamName.LINEAR_MODEL_TYPE, linearModelType)
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

	public static List <Tuple2 <Object, Vector>> predict(LinearModelData model, List <Vector> data) {
		return LocalLinearModel.predict(model, data);
	}

}
