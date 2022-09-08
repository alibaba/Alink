package com.alibaba.alink.operator.common.similarity.modeldata;

import com.alibaba.alink.operator.common.distance.InnerProduct;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceData;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class VectorModelData extends NearestNeighborModelData {
	private static final long serialVersionUID = -2940551481683238630L;
	public final List <FastDistanceData> dictData;
	public final FastDistance fastDistance;

	public VectorModelData(List <FastDistanceData> list, FastDistance distance) {
		this.dictData = list;
		this.fastDistance = distance;
		if (distance instanceof InnerProduct) {
			comparator = Comparator.comparingDouble(o -> o.f0);
		} else {
			comparator = Comparator.comparingDouble(o -> -o.f0);
		}
	}

	@Override
	protected Integer getLength() {
		return dictData.size();
	}

	@Override
	protected Object prepareSample(Object input) {
		return fastDistance.prepareVectorData(Tuple2.of(VectorUtil.getVector(input), null));
	}

	@Override
	protected ArrayList<Tuple2 <Double, Object>> computeDistiance(Object input, Integer index, Integer topN,
														 Tuple2 <Double, Object> radius) {
		FastDistanceData data = dictData.get(index);
		DenseMatrix res = fastDistance.calc((FastDistanceVectorData)input, data);
		ArrayList<Tuple2 <Double, Object>> list = new ArrayList();
		Row[] curRows = data.getRows();
		for (int i = 0; i < data.getRows().length; i++){
			Tuple2 <Double, Object> tuple = Tuple2.of(res.getData()[i], curRows[i].getField(0));
			if (null == radius || radius.f0 == null || this.getQueueComparator().compare(radius, tuple) <= 0) {
				list.add(tuple);
			}
		}
		return list;
	}
}
