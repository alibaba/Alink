package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.VectorUtil;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;

public class DocVecGenerator {
	private HashMap <String, DenseVector> embed = new HashMap <>();

	private String wordDelimiter;
	private InferVectorMethod inferMethod;

	public DocVecGenerator(
		List <Row> dict,
		String wordDelimiter,
		InferVectorMethod inferMethod) {

		this.wordDelimiter = wordDelimiter;
		this.inferMethod = inferMethod;
		for (Row row : dict) {
			embed.put((String) row.getField(0), (DenseVector)VectorUtil.getVector(VectorUtil.getVector(row.getField(1))));
		}
	}

	public String transform(String content) {
		if (null == content) {
			return null;
		}

		String[] tokens = content.split(wordDelimiter);

		DenseVector dvec = null;
		double cnt = 0;
		for (String word : tokens) {
			DenseVector t = embed.get(word);
			if (null != t) {
				if (null != dvec) {
					dvec = inferMethod.getFunc().apply(dvec, t);
				} else {
					dvec = t.clone();
				}

				cnt += 1.0;
			}
		}

		if (null == dvec) {
			return null;
		} else {
			if (inferMethod == InferVectorMethod.AVG) {
				dvec.scaleEqual(1.0 / cnt);
			}
			return VectorUtil.toString(dvec);
		}
	}

	public interface DenseVectorBiFuntionSerizlizeable
		extends BiFunction<DenseVector, DenseVector, DenseVector>, Serializable {}

	public enum InferVectorMethod implements Serializable {
		/**
		 * AVG Method
		 */
		AVG(new DenseVectorBiFuntionSerizlizeable() {
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
			@Override
			public DenseVector apply(DenseVector denseVector, DenseVector denseVector2) {
				MatVecOp.apply(denseVector, denseVector2, denseVector, new BiFunction<Double, Double, Double>() {
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
			@Override
			public DenseVector apply(DenseVector denseVector, DenseVector denseVector2) {
				MatVecOp.apply(denseVector, denseVector2, denseVector, new BiFunction<Double, Double, Double>() {
					@Override
					public Double apply(Double x, Double y) {
						return Math.max(x, y);
					}
				});

				return denseVector;
			}
		});

		private DenseVectorBiFuntionSerizlizeable func;

		InferVectorMethod(DenseVectorBiFuntionSerizlizeable func) {
			this.func = func;
		}

		public DenseVectorBiFuntionSerizlizeable getFunc() {
			return func;
		}
	}

}
