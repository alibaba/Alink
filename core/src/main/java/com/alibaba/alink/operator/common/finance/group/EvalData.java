package com.alibaba.alink.operator.common.finance.group;

import java.io.Serializable;

public class EvalData implements Serializable {
	public double auc;
	public double ks;

	public EvalData clone() {
		EvalData eval = new EvalData();
		eval.auc = auc;
		eval.ks = ks;
		return eval;
	}
}
