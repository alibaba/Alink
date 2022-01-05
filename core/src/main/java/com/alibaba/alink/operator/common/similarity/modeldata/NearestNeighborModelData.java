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
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class NearestNeighborModelData implements Serializable, Cloneable {
	private static final long serialVersionUID = -6611164130651479716L;
	protected Comparator <? super Tuple2 <Double, Object>> comparator;

	private TypeInformation<?> idType;

	public void setIdType(TypeInformation<?> idType) {
		this.idType = idType;
	}

	public String findNeighbor(Object input, Integer topN, Double radius) {
		PriorityQueue <Tuple2 <Double, Object>> priorityQueue = new PriorityQueue <>(this.getQueueComparator());
		search(input, topN, Tuple2.of(radius, null), priorityQueue);

		List <Object> items = new ArrayList <>();
		List <Double> metrics = new ArrayList <>();
		while (!priorityQueue.isEmpty()) {
			Tuple2 <Double, Object> result = priorityQueue.poll();
			items.add(EvaluationUtil.castTo(result.f1, idType));
			metrics.add(result.f0);
		}
		Collections.reverse(items);
		Collections.reverse(metrics);
		priorityQueue.clear();
		return KObjectUtil.serializeRecomm(
			"ID",
			items,
			ImmutableMap.of("METRIC", metrics)
		);
	}

	protected void search(Object input, Integer topN,
						  Tuple2 <Double, Object> radius,
						  PriorityQueue <Tuple2 <Double, Object>> priorityQueue) {
		Object sample = prepareSample(input);
		Tuple2 <Double, Object> head = null;
		for (int i = 0; i < getLength(); i++) {
			ArrayList<Tuple2 <Double, Object>> values = computeDistiance(sample, i, topN, radius);
			if (null == values || values.size() == 0) { continue; }
			for (Tuple2 <Double, Object> currentValue : values) {
				if (null == topN) {
					priorityQueue.add(Tuple2.of(currentValue.f0, currentValue.f1));
				} else {
					head = SimilarityUtil.updateQueue(priorityQueue, topN, currentValue, head);
				}
			}
		}
	}

	public NearestNeighborModelData mirror() {
		try {
			NearestNeighborModelData modelData = (NearestNeighborModelData) this.clone();
			modelData.comparator = modelData.getQueueComparator();
			return modelData;
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}

	public Comparator <? super Tuple2 <Double, Object>> getQueueComparator() {
		return comparator;
	}

	protected Integer getLength() {
		return null;
	}

	protected Object prepareSample(Object input) {
		return null;
	}

	protected ArrayList<Tuple2 <Double, Object>> computeDistiance(Object input, Integer index, Integer topN,
														 Tuple2 <Double, Object> radius) {
		return null;
	}
}
