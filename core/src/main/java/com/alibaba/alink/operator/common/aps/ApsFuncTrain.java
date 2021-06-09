package com.alibaba.alink.operator.common.aps;

import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ApsFuncTrain<DT, MT>
	extends RichCoGroupFunction <Tuple3 <Integer, Long, MT>, Tuple3 <Integer, Integer, DT>, Tuple2 <Long, MT>> {

	private static final Logger LOG = LoggerFactory.getLogger(ApsFuncTrain.class);
	private static final long serialVersionUID = 432623425253133299L;
	protected Params contextParams = null;
	private int pid = -1;

	@Override
	public void open(Configuration parameters) throws Exception {
		LOG.info("{}:{}", Thread.currentThread().getName(), "open");
		String broadcastName = "TrainSubset";
		if (getRuntimeContext().hasBroadcastVariable(broadcastName)) {
			this.contextParams = (Params) getRuntimeContext().getBroadcastVariable(broadcastName).get(0);
		}
	}

	@Override
	public void close() throws Exception {
		LOG.info("{}:{}", Thread.currentThread().getName(), "close");
	}

	@Override
	public void coGroup(Iterable <Tuple3 <Integer, Long, MT>> first,
						Iterable <Tuple3 <Integer, Integer, DT>> second,
						Collector <Tuple2 <Long, MT>> out)
		throws Exception {

		ArrayList <Tuple2 <Long, MT>> oldModel = new ArrayList <>();
		for (Tuple3 <Integer, Long, MT> t3 : first) {
			oldModel.add(new Tuple2 <>(t3.f1, t3.f2));
		}

		ArrayList <DT> dataShard = new ArrayList <>();
		for (Tuple3 <Integer, Integer, DT> t3 : second) {
			this.pid = t3.f0;
			dataShard.add(t3.f2);
		}

		HashMap <Long, Integer> oldIndexMap = new HashMap <>(oldModel.size());
		for (int i = 0; i < oldModel.size(); i++) {
			oldIndexMap.put(oldModel.get(i).f0, i);
		}

		List <Tuple2 <Long, MT>> newModel = train(oldModel, oldIndexMap, dataShard);

		for (Tuple2 <Long, MT> t2 : newModel) {
			out.collect(t2);
		}

	}

	public int getPatitionId() {
		return this.pid;
	}

	protected abstract List <Tuple2 <Long, MT>> train(List <Tuple2 <Long, MT>> relatedFeatures,
													  Map <Long, Integer> mapFeatureId2Local,
													  List <DT> trainData) throws Exception;

}
