package com.alibaba.alink.operator.common.tree.xgboost;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.List;

public interface Tracker extends Serializable {

	boolean start(long timeout);

	List <Tuple2 <String, String>> getWorkerEnvs();

	void stop();
}
