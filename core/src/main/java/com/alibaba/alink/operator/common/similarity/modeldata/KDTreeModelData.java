package com.alibaba.alink.operator.common.similarity.modeldata;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.operator.common.similarity.KDTree;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class KDTreeModelData extends NearestNeighborModelData {
	private static final long serialVersionUID = 6293716822361507135L;
	private static final EuclideanDistance distance = new EuclideanDistance();
	private final List <KDTree> treeList;

	public KDTreeModelData(List <KDTree> list) {
		this.treeList = list;
		this.comparator = Comparator.comparingDouble(v -> -v.f0);
	}

	@Override
	protected Integer getLength() {
		return treeList.size();
	}

	@Override
	protected Object prepareSample(Object input) {
		Vector vector = VectorUtil.getVector(input);
		return distance.prepareVectorData(Tuple2.of(vector, null));
	}

	@Override
	protected ArrayList <Tuple2 <Double, Object>> computeDistiance(Object input, Integer index, Integer topN,
																   Tuple2 <Double, Object> radius) {
		KDTree tree = treeList.get(index);
		ArrayList <Tuple2 <Double, Object>> tupleList = new ArrayList <>();
		if (null != topN) {
			Tuple2 <Double, Row>[] treeTopN = tree.getTopN(topN, (FastDistanceVectorData) input);
			for (int i = 0; i < treeTopN.length; i++) {
				Tuple2 <Double, Object> tuple = Tuple2.of(treeTopN[i].f0, treeTopN[i].f1.getField(0));
				if (null == radius || radius.f0 == null || this.getQueueComparator().compare(radius, tuple) <= 0) {
					tupleList.add(tuple);
				}
			}
		} else {
			List <FastDistanceVectorData> list = tree.rangeSearch(radius.f0, (FastDistanceVectorData) input);
			for (FastDistanceVectorData data : list) {
				Double dist = distance.calc(data, (FastDistanceVectorData) input).get(0, 0);
				tupleList.add(Tuple2.of(dist, data.getRows()[0].getField(0)));
			}
		}
		return tupleList;
	}
}
