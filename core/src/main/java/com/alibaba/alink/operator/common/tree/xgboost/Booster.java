package com.alibaba.alink.operator.common.tree.xgboost;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.XGboostException;
import com.alibaba.alink.common.linalg.Vector;

import java.io.Serializable;
import java.util.function.Function;

public interface Booster extends Serializable {

	byte[] toByteArray() throws XGboostException;

	float[] predict(
		Row row,
		Function <Row, Row> preprocess,
		Function <Row, Tuple2 <Vector, float[]>> extractor) throws XGboostException;
}
