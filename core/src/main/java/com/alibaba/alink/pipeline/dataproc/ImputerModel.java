package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.ImputerModelMapper;
import com.alibaba.alink.params.dataproc.ImputerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is Imputer model for Imputer predict.
 */
@NameCn("缺失值填充模型")
public class ImputerModel extends MapModel <ImputerModel>
	implements ImputerPredictParams <ImputerModel> {

	private static final long serialVersionUID = 4028985365253339542L;

	public ImputerModel(Params params) {
		super(ImputerModelMapper::new, params);
	}

}
