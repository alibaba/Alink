package com.alibaba.alink.operator.common.similarity.modeldata;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.operator.common.similarity.KDTree;
import com.alibaba.alink.operator.common.similarity.SimilarityUtil;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class KDTreeModelData extends NearestNeighborModelData {
	private static final long serialVersionUID = 6293716822361507135L;
	private EuclideanDistance distance = new EuclideanDistance();
	private List <KDTree> treeList;

	public KDTreeModelData(List <KDTree> list) {
		this.treeList = list;
		this.queue = new PriorityQueue <>(Comparator.comparingDouble(v -> -v.f0));
	}

	@Override
	public PriorityQueue <Tuple2 <Double, Object>> search(Object s, int topN, Tuple2 <Double, Object> radius) {
		queue.clear();
		Vector vector = VectorUtil.getVector(s);
		FastDistanceVectorData vectorData = distance.prepareVectorData(Tuple2.of(vector, null));

		Tuple2 <Double, Object> head = Tuple2.of(Double.MIN_VALUE, null);
		Tuple2 <Double, Object> newValue = Tuple2.of(null, null);
		for (KDTree tree : treeList) {
			Tuple2 <Double, Row>[] treeTopN = tree.getTopN(topN, vectorData);
			for (Tuple2 <Double, Row> t : treeTopN) {
				newValue.f0 = t.f0;
				newValue.f1 = t.f1.getField(0);
				if (null == radius || queue.comparator().compare(radius, newValue) < 0) {
					head = SimilarityUtil.updateQueue(queue, topN, newValue, head);
				}
			}
		}
		return queue;
	}

	@Override
	public PriorityQueue <Tuple2 <Double, Object>> search(Object selectedCol, int topN) {
		queue.clear();
		Vector vector = VectorUtil.getVector(selectedCol);
		FastDistanceVectorData vectorData = distance.prepareVectorData(Tuple2.of(vector, null));

		Tuple2 <Double, Object> head = Tuple2.of(Double.MIN_VALUE, null);
		Tuple2 <Double, Object> newValue = Tuple2.of(null, null);
		for (KDTree tree : treeList) {
			Tuple2 <Double, Row>[] treeTopN = tree.getTopN(topN, vectorData);
			for (Tuple2 <Double, Row> t : treeTopN) {
				newValue.f0 = t.f0;
				newValue.f1 = t.f1.getField(0);
				head = SimilarityUtil.updateQueue(queue, topN, newValue, head);
			}
		}
		return queue;
	}

	@Override
	public PriorityQueue <Tuple2 <Double, Object>> search(Object selectedCol, Tuple2 <Double, Object> radius) {
		queue.clear();
		Vector vector = VectorUtil.getVector(selectedCol);
		FastDistanceVectorData vectorData = distance.prepareVectorData(Tuple2.of(vector, null));

		for (KDTree tree : treeList) {
			List <FastDistanceVectorData> list = tree.rangeSearch(radius.f0, vectorData);
			list.forEach(v -> queue.add(Tuple2.of(distance.calc(v, vectorData).get(0, 0), v.getRows()[0].getField(0)
			)));
		}
		return queue;
	}

}
