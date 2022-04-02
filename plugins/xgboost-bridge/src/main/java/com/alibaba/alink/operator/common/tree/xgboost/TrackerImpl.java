package com.alibaba.alink.operator.common.tree.xgboost;

import org.apache.flink.api.java.tuple.Tuple2;

import ml.dmlc.xgboost4j.java.RabitTracker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TrackerImpl implements Tracker {

	private final RabitTracker tracker;

	public TrackerImpl(RabitTracker tracker) {
		this.tracker = tracker;
	}

	@Override
	public boolean start(long timeout) {
		return tracker.start(timeout);
	}

	@Override
	public List <Tuple2 <String, String>> getWorkerEnvs() {

		List <Tuple2 <String, String>> workerEnvs = new ArrayList <>();

		for (Map.Entry <String, String> entry : tracker.getWorkerEnvs().entrySet()) {
			workerEnvs.add(Tuple2.of(entry.getKey(), entry.getValue()));
		}

		return workerEnvs;
	}

	@Override
	public void stop() {
		tracker.stop();
	}
}
