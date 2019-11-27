package com.alibaba.alink.operator.common.optim.subfunc;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * Transfer the state to model rows.
 *
 */
public class OutputModel extends CompleteResultFunction {

	@Override
	public List <Row> calc(ComContext context) {
		if (context.getTaskId() != 0) {
			return null;
		}

		// get the coefficient of min loss.
		Tuple2 <DenseVector, double[]> minCoef = context.getObj(OptimVariable.minCoef);
		double[] lossCurve = context.getObj(OptimVariable.lossCurve);

		int effectiveSize = lossCurve.length;
		for (int i = 0; i < lossCurve.length; ++i) {
			if (Double.isInfinite(lossCurve[i])) {
				effectiveSize = i;
				break;
			}
		}

		double[] effectiveCurve = new double[effectiveSize];
		System.arraycopy(lossCurve, 0, effectiveCurve, 0, effectiveSize);


		Params params = new Params();
		for (int i = 0; i < minCoef.f0.size(); ++i) {
			if (Double.isNaN(minCoef.f0.get(i)) || Double.isInfinite(minCoef.f0.get(i))) {
				throw new RuntimeException("Optimization result has NAN or infinite value, coefficient is invalid");
			}
		}
		params.set(ModelParamName.COEF, minCoef.f0);
		params.set(ModelParamName.LOSS_CURVE, effectiveCurve);
		List <Row> model = new ArrayList <>(1);
		model.add(Row.of(params.toJson()));
		return model;
	}
}
