package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.BinningModelMapper;
import com.alibaba.alink.params.finance.BinningPredictParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("分箱模型")
public class BinningModel extends MapModel <BinningModel>
	implements BinningPredictParams <BinningModel> {

	private static final long serialVersionUID = -3706609001434209580L;

	public BinningModel() {
		this(null);
	}

	public BinningModel(Params params) {
		super(BinningModelMapper::new, params);
	}
}
