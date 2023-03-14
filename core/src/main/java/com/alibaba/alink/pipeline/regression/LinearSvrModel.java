package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.regression.LinearSvrPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * @author yangxu
 */
@NameCn("线性SVR模型")
public class LinearSvrModel extends MapModel <LinearSvrModel>
	implements LinearSvrPredictParams <LinearSvrModel> {

	private static final long serialVersionUID = -4576646447851317055L;

	public LinearSvrModel() {
		this(null);
	}

	public LinearSvrModel(Params params) {
		super(LinearModelMapper::new, params);
	}
}
