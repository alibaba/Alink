package com.alibaba.alink.operator.common.aps;

import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class ApsFuncUpdateModel<MT>
	extends RichCoGroupFunction <Tuple2 <Long, MT>, Tuple2 <Long, MT>, Tuple2 <Long, MT>> {

	private static final Logger LOG = LoggerFactory.getLogger(ApsFuncUpdateModel.class);
	private static final long serialVersionUID = -3138004413804325535L;

	@Override
	public void open(Configuration parameters) throws Exception {
		LOG.info("{}:{}", Thread.currentThread().getName(), "open");
	}

	@Override
	public void close() throws Exception {
		LOG.info("{}:{}", Thread.currentThread().getName(), "close");
	}

	@Override
	public void coGroup(Iterable <Tuple2 <Long, MT>> first,
						Iterable <Tuple2 <Long, MT>> second,
						Collector <Tuple2 <Long, MT>> out
	) throws Exception {
		Iterator <Tuple2 <Long, MT>> iterator1 = first.iterator();
		Iterator <Tuple2 <Long, MT>> iterator2 = second.iterator();
		Tuple2 <Long, MT> oldFeature = iterator2.hasNext() ? iterator2.next() : null;
		if (iterator1.hasNext()) {
			ArrayList <MT> newFeaVals = new ArrayList <>();
			Long idx = null;
			while (iterator1.hasNext()) {
				Tuple2 <Long, MT> t2 = iterator1.next();
				newFeaVals.add(t2.f1);
				idx = t2.f0;
			}
			out.collect(
				new Tuple2 <Long, MT>(idx, update((null == oldFeature) ? null : oldFeature.f1, newFeaVals)));
		} else {
			out.collect(oldFeature);
		}
	}

	/**
	 * update on each feature by the old value and the new updated values
	 *
	 * @param oldFeaVal  original feature value
	 * @param newFeaVals collected feature values from each computing node
	 * @return updated feature value
	 */
	public abstract MT update(MT oldFeaVal, List <MT> newFeaVals);

}
