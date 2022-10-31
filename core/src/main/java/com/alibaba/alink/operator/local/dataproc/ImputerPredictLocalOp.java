package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.ImputerModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.dataproc.ImputerPredictParams;

/**
 * Imputer completes missing values in a dataSet, but only same type of columns can be selected at the same time.
 * Imputer Predict completes missing values in a dataSet with model which trained from Inputer train.
 * Strategy support min, max, mean or value.
 */
@NameCn("缺失值填充批预测")
public class ImputerPredictLocalOp extends ModelMapLocalOp <ImputerPredictLocalOp>
	implements ImputerPredictParams <ImputerPredictLocalOp> {

	public ImputerPredictLocalOp() {
		this(null);
	}

	public ImputerPredictLocalOp(Params params) {
		super(ImputerModelMapper::new, params);
	}
}




