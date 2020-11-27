package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.operator.common.utils.PrettyDisplayUtils.displayList;

public class VectorMinMaxScalerModelInfo implements Serializable {
	private static final long serialVersionUID = -437969027787321541L;
	double[] mins;
	double[] maxs;

	public VectorMinMaxScalerModelInfo(List <Row> rows) {
		Tuple4 <Double, Double, double[], double[]> tuple4 = new VectorMinMaxScalerModelDataConverter().load(rows);
		mins = tuple4.f2;
		maxs = tuple4.f3;
	}

	public double[] getMins() {
		return mins;
	}

	public double[] getMaxs() {
		return maxs;
	}

	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();
		res.append(PrettyDisplayUtils.displayHeadline("VectorMinMaxScalerModelSummary", '-') + "\n");
		res.append(PrettyDisplayUtils.displayHeadline("lower bound information", '=') + "\n");
		res.append(displayList(Arrays.asList(ArrayUtils.toObject(mins)), false) + "\n");
		res.append(PrettyDisplayUtils.displayHeadline("upper bound information", '=') + "\n");
		res.append(displayList(Arrays.asList(ArrayUtils.toObject(maxs)), false) + "\n");
		return res.toString();
	}
}
