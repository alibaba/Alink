package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.common.dataproc.ImputerModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.dataproc.ImputerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is Imputer model for Imputer predict.
 */
public class ImputerModel extends MapModel<ImputerModel>
	implements ImputerPredictParams <ImputerModel> {

	public ImputerModel(Params params) {
		super(ImputerModelMapper::new, params);
	}

}