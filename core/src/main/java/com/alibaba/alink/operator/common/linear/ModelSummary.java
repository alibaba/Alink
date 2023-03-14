package com.alibaba.alink.operator.common.linear;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.SelectorStep;

public abstract class ModelSummary implements AlinkSerializable {
	public double loss;
	public DenseVector beta;
	public DenseVector gradient;
	public DenseMatrix hessian;
	public long count;

	public abstract SelectorStep toSelectStep(int inId);

}
