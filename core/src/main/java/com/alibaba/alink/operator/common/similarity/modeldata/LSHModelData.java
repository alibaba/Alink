package com.alibaba.alink.operator.common.similarity.modeldata;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.common.similarity.lsh.BaseLSH;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class LSHModelData extends NearestNeighborModelData {
	private static final long serialVersionUID = 1431580949382378771L;
	private Map <Object, Vector> data;
	private Map <Integer, List <Object>> indexMap;
	private BaseLSH lsh;

	private HashSet <Object> set;
	private Iterator <Object> iterator;
	private Vector vector;

	public LSHModelData(Map <Integer, List <Object>> indexMap, Map <Object, Vector> data, BaseLSH lsh) {
		this.indexMap = indexMap;
		this.data = data;
		this.lsh = lsh;
		queue = new PriorityQueue <>(Comparator.comparingDouble(o -> -o.f0));
	}

	@Override
	void iterabor(Object selectedCol) {
		vector = VectorUtil.getVector(selectedCol);
		int[] hashValue = lsh.hashFunction(vector);
		set = new HashSet <>();
		for (int hash : hashValue) {
			List <Object> list = indexMap.get(hash);
			if (null != list) {
				set.addAll(indexMap.get(hash));
			}
		}
		this.iterator = set.iterator();
	}

	@Override
	boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	void next(Tuple2 <Double, Object> newValue) {
		newValue.f1 = iterator.next();
		newValue.f0 = lsh.keyDistance(vector, data.get(newValue.f1));
	}

}
