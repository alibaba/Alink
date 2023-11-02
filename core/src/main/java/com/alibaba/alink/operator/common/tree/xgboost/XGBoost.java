package com.alibaba.alink.operator.common.tree.xgboost;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.XGboostException;
import com.alibaba.alink.common.linalg.Vector;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public interface XGBoost extends Serializable {
	Tracker initTracker(int numTask) throws XGboostException;

	void init(List <Tuple2 <String, String>> workerEnvs) throws XGboostException;

	Booster train(
		Iterator <Row> rowIterator,
		Function <Row, Row> preprocess,
		Function <Row, Tuple3 <Vector, float[], Float>> extractor,
		Params params) throws XGboostException;

	void shutdown() throws XGboostException;

	Booster loadModel(InputStream in) throws XGboostException, IOException;
}
