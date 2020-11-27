package com.alibaba.alink.operator.common.similarity.modeldata;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.recommendation.KObjectUtil;
import com.alibaba.alink.operator.common.similarity.SimilarityUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

public class NearestNeighborModelData implements Serializable, Cloneable {
	private static final long serialVersionUID = -6611164130651479716L;
	protected PriorityQueue <Tuple2 <Double, Object>> queue;
	private Tuple2 <Double, Object> newValue;

	private TypeInformation idType;

	public void setIdType(TypeInformation idType) {
		this.idType = idType;
	}

	public String findNeighbor(Object input, Integer topN, Double radius) {
		PriorityQueue <Tuple2 <Double, Object>> queue;
		if (null == topN) {
			queue = search(input, Tuple2.of(radius, null));
		} else if (null == radius) {
			queue = search(input, topN);
		} else {
			queue = search(input, topN, Tuple2.of(radius, null));
		}

		List <Object> items = new ArrayList <>();
		List <Double> metrics = new ArrayList <>();
		while (!queue.isEmpty()) {
			Tuple2 <Double, Object> result = queue.poll();
			items.add(EvaluationUtil.castTo(result.f1, idType));
			metrics.add(result.f0);
		}
		Collections.reverse(items);
		Collections.reverse(metrics);
		return KObjectUtil.serializeRecomm(
			"ID",
			items,
			ImmutableMap.of("METRIC", metrics)
		);
	}

	protected PriorityQueue <Tuple2 <Double, Object>> search(Object selectedCol, int topN) {
		this.iterabor(selectedCol);
		queue.clear();
		Tuple2 <Double, Object> head = null;
		newValue = Tuple2.of(null, null);
		while (this.hasNext()) {
			this.next(newValue);
			head = SimilarityUtil.updateQueue(queue, topN, newValue, head);
		}
		return queue;
	}

	protected PriorityQueue <Tuple2 <Double, Object>> search(Object selectedCol, Tuple2 <Double, Object> radius) {
		this.iterabor(selectedCol);
		queue.clear();
		newValue = Tuple2.of(null, null);
		while (this.hasNext()) {
			this.next(newValue);
			if (null != newValue && queue.comparator().compare(radius, newValue) <= 0) {
				queue.add(Tuple2.of(newValue.f0, newValue.f1));
			}
		}
		return queue;
	}

	protected PriorityQueue <Tuple2 <Double, Object>> search(Object selectedCol, int topN,
															 Tuple2 <Double, Object> radius) {
		queue.clear();
		this.iterabor(selectedCol);
		Tuple2 <Double, Object> head = null;
		newValue = Tuple2.of(null, null);
		while (this.hasNext()) {
			this.next(newValue);
			if (queue.comparator().compare(radius, newValue) <= 0) {
				head = SimilarityUtil.updateQueue(queue, topN, newValue, head);
			}
		}
		return queue;
	}

	void iterabor(Object selectedCol) {}

	boolean hasNext() {
		return false;
	}

	void next(Tuple2 <Double, Object> newValue) {}

	public NearestNeighborModelData mirror() {
		try {
			NearestNeighborModelData modelData = (NearestNeighborModelData) this.clone();
			modelData.queue = new PriorityQueue(modelData.queue.comparator());
			return modelData;
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
}
