package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.operator.common.utils.PrettyDisplayUtils.displayList;

public class VectorMaxAbsScalarModelInfo implements Serializable {
	private static final long serialVersionUID = -2878000847784601179L;
	double[] maxAbs;

	public VectorMaxAbsScalarModelInfo(List <Row> rows) {
		double[] maxAbs = new VectorMaxAbsScalerModelDataConverter().load(rows);
		this.maxAbs = maxAbs;
	}

	public double[] getMaxAbs() {
		return maxAbs;
	}

	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();
		res.append(PrettyDisplayUtils.displayHeadline("VectorMaxAbsScalarModelSummary", '-') + "\n");
		res.append(PrettyDisplayUtils.displayHeadline("max abs information", '=') + "\n");
		res.append(displayList(Arrays.asList(ArrayUtils.toObject(maxAbs)), false) + "\n");
		return res.toString();
	}
}
