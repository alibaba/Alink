package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.StandardScalerModelMapper;
import com.alibaba.alink.params.dataproc.StandardPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is model of StandardScaler, which is used in predict or transform.
 */
@NameCn("标准化模型")
public class StandardScalerModel extends MapModel <StandardScalerModel>
	implements StandardPredictParams <StandardScalerModel> {

	private static final long serialVersionUID = 3973263007567900627L;

	public StandardScalerModel(Params params) {
		super(StandardScalerModelMapper::new, params);
	}

}
