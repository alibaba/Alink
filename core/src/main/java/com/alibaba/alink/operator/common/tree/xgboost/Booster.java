package com.alibaba.alink.operator.common.tree.xgboost;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.XGboostException;
import com.alibaba.alink.common.linalg.Vector;

import java.io.Serializable;
import java.util.Iterator;
import java.util.function.Function;

public interface Booster extends Serializable {

	byte[] toByteArray() throws XGboostException;

	float[] predict(
		Row row,
		Function <Row, Row> preprocess,
		Function <Row, Tuple3 <Vector, float[], Float>> extractor) throws XGboostException;

	float[][] predict(Iterator <Row> rowIterator, Function <Row, Row> preprocess,
					  Function <Row, Tuple3 <Vector, float[], Float>> extractor) throws XGboostException;

	long getNumFeatures() throws XGboostException;
}
