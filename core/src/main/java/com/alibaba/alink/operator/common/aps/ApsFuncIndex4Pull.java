package com.alibaba.alink.operator.common.aps;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class ApsFuncIndex4Pull<DT>
	extends RichGroupReduceFunction <Tuple3 <Integer, Integer, DT>, Tuple2 <Integer, Long>> {

	private static final Logger LOG = LoggerFactory.getLogger(ApsFuncIndex4Pull.class);

	private static final long serialVersionUID = 4836142501411296580L;
	protected Params contextParams = null;
	private int pid = -1;

	@Override
	public void open(Configuration parameters) throws Exception {
		LOG.info("{}:{}", Thread.currentThread().getName(), "open");
		String broadcastName = "RequestIndex";
		if (getRuntimeContext().hasBroadcastVariable(broadcastName)) {
			this.contextParams = (Params) getRuntimeContext().getBroadcastVariable(broadcastName).get(0);
		}
	}

	@Override
	public void close() throws Exception {
		LOG.info("{}:{}", Thread.currentThread().getName(), "close");
	}

	@Override
	public void reduce(Iterable <Tuple3 <Integer, Integer, DT>> iterable,
					   Collector <Tuple2 <Integer, Long>> collector) throws Exception {
		//Integer pid = null;

		ArrayList <DT> dataShard = new ArrayList <>();
		for (Tuple3 <Integer, Integer, DT> t3 : iterable) {
			pid = t3.f0;
			dataShard.add(t3.f2);
		}

		Set <Long> indexes = requestIndex(dataShard);
		for (Long idx : indexes) {
			collector.collect(new Tuple2 <>(pid, idx));
		}
	}

	public int getPatitionId() {
		return this.pid;
	}

	protected abstract Set <Long> requestIndex(List <DT> data) throws Exception;
}
