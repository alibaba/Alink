package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelMapper;
import com.alibaba.alink.operator.common.feature.MultiHotModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.feature.MultiHotPredictParams;

/**
 *  Multi hot encoding predict process.
 */
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("多热编码预测")
@NameEn("Multi-hot prediction")
public class MultiHotPredictStreamOp extends ModelMapStreamOp <MultiHotPredictStreamOp>
	implements MultiHotPredictParams <MultiHotPredictStreamOp> {

	private static final long serialVersionUID = 3986423530880867993L;

	public MultiHotPredictStreamOp() {
		super(MultiHotModelMapper::new, new Params());
	}

	public MultiHotPredictStreamOp(Params params) {
		super(MultiHotModelMapper::new, params);
	}

	/**
	 * constructor.
	 *
	 * @param model the model.
	 */
	public MultiHotPredictStreamOp(BatchOperator model) {
		super(model, MultiHotModelMapper::new, new Params());
	}

	/**
	 * @param model  the model.
	 * @param params the parameter set.
	 */
	public MultiHotPredictStreamOp(BatchOperator model, Params params) {
		super(model, MultiHotModelMapper::new, params);
	}
}
