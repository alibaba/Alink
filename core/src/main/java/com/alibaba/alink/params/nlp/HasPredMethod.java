package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.params.ParamUtil;

import java.io.Serializable;
import java.util.function.BiFunction;

public interface HasPredMethod<T> extends WithParams <T> {
	ParamInfo <PredMethod> PRED_METHOD = ParamInfoFactory
		.createParamInfo("predMethod", PredMethod.class)
		.setDescription("Method to predict doc vector, support 3 method: avg, min and max, default value is avg.")
		.setHasDefaultValue(PredMethod.AVG)
		.setAlias(new String[] {"generationType", "algorithmType"})
		.build();

	default PredMethod getPredMethod() {
		return get(PRED_METHOD);
	}

	default T setPredMethod(PredMethod value) {
		return set(PRED_METHOD, value);
	}

	default T setPredMethod(String value) {
		return set(PRED_METHOD, ParamUtil.searchEnum(PRED_METHOD, value));
	}

	enum PredMethod implements Serializable {
		/**
		 * AVG Method
		 */
		AVG(new DenseVectorBiFuntionSerizlizeable() {
			private static final long serialVersionUID = -2863395126966057125L;

			@Override
			public DenseVector apply(DenseVector denseVector, DenseVector denseVector2) {
				denseVector.plusScaleEqual(denseVector2, 1.0);
				return denseVector;
			}
		}),

		/**
		 * MIN Method
		 */
		MIN(new DenseVectorBiFuntionSerizlizeable() {
			private static final long serialVersionUID = -6685413984808751889L;

			@Override
			public DenseVector apply(DenseVector denseVector, DenseVector denseVector2) {
				MatVecOp.apply(denseVector, denseVector2, denseVector, new BiFunction <Double, Double, Double>() {
					@Override
					public Double apply(Double x, Double y) {
						return Math.min(x, y);
					}
				});

				return denseVector;
			}
		}),

		/**
		 * MAX Method
		 */
		MAX(new DenseVectorBiFuntionSerizlizeable() {
			private static final long serialVersionUID = -1270652533302453867L;

			@Override
			public DenseVector apply(DenseVector denseVector, DenseVector denseVector2) {
				MatVecOp.apply(denseVector, denseVector2, denseVector, new BiFunction <Double, Double, Double>() {
					@Override
					public Double apply(Double x, Double y) {
						return Math.max(x, y);
					}
				});

				return denseVector;
			}
		});

		private DenseVectorBiFuntionSerizlizeable func;

		PredMethod(DenseVectorBiFuntionSerizlizeable func) {
			this.func = func;
		}

		public DenseVectorBiFuntionSerizlizeable getFunc() {
			return func;
		}
	}

	interface DenseVectorBiFuntionSerizlizeable
		extends BiFunction <DenseVector, DenseVector, DenseVector>, Serializable {}
}
