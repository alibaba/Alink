package com.alibaba.alink.operator.common.similarity.modeldata;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceData;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class VectorModelData extends NearestNeighborModelData {
	private static final long serialVersionUID = -2940551481683238630L;
	private List <FastDistanceData> dictData;
	private FastDistance fastDistance;
	private DenseMatrix res;

	private FastDistanceVectorData vectorData;
	private Iterator <FastDistanceData> iterabor;
	private double[] curData;
	private Row[] curRows;
	private int curIndex = 0;

	public VectorModelData(List <FastDistanceData> list, FastDistance distance) {
		this.dictData = list;
		this.fastDistance = distance;
		queue = new PriorityQueue <>(Comparator.comparingDouble(o -> -o.f0));
	}

	@Override
	void iterabor(Object selectedCol) {
		vectorData = fastDistance.prepareVectorData(Tuple2.of(VectorUtil.getVector(selectedCol), null));
		iterabor = dictData.iterator();
		if (iterabor.hasNext()) {
			curIndex = 0;
			FastDistanceData data = iterabor.next();
			res = fastDistance.calc(vectorData, data, res);
			curRows = data.getRows();
			curData = res.getData();
		} else {
			curIndex = 0;
			curRows = new Row[0];
		}
	}

	@Override
	boolean hasNext() {
		return iterabor.hasNext() || curIndex < curRows.length;
	}

	@Override
	void next(Tuple2 <Double, Object> newValue) {
		if (curIndex >= curRows.length) {
			curIndex = 0;
			FastDistanceData data = iterabor.next();
			res = fastDistance.calc(vectorData, data, res);
			curRows = data.getRows();
			curData = res.getData();
		}
		newValue.f0 = curData[curIndex];
		newValue.f1 = curRows[curIndex].getField(0);
		curIndex++;
	}
}
