package com.alibaba.alink.operator.common.tree.xgboost;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.XGboostException;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.tree.XGBoostUtil;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoostError;

import java.util.Iterator;
import java.util.function.Function;

public class BoosterImpl implements Booster {
	private final ml.dmlc.xgboost4j.java.Booster booster;

	public BoosterImpl(ml.dmlc.xgboost4j.java.Booster booster) {
		this.booster = booster;
	}

	@Override
	public byte[] toByteArray() throws XGboostException {
		try {
			return booster.toByteArray();
		} catch (XGBoostError e) {
			throw new XGboostException(e);
		}
	}

	@Override
	public float[] predict(Row row, Function <Row, Row> preprocess,
						   Function <Row, Tuple2 <Vector, float[]>> extractor) throws XGboostException {
		try {
			return booster.predict(
				new DMatrix(
					XGBoostUtil.asLabeledPointIterator(
						new Iterator <Row>() {
							boolean onlyOne = true;

							@Override
							public boolean hasNext() {
								return onlyOne;
							}

							@Override
							public Row next() {
								onlyOne = false;
								return row;
							}
						},
						XGBoostUtil.asLabeledPointConverterFunction(
							preprocess,
							extractor
						)
					),
					null
				)
			)[0];
		} catch (XGBoostError e) {
			throw new XGboostException(e);
		}
	}

}
