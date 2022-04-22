package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.outlier.IForestModelOutlierTrainBatchOp;
import com.alibaba.alink.params.outlier.IForestModelTrainParams;

@NameCn("IForest异常检测")
@NameEn("IForest outlier")
public class IForestModelOutlier extends BaseModelOutlier <IForestModelOutlier, IForestModelOutlierModel>
	implements IForestModelTrainParams <IForestModelOutlier> {

	public IForestModelOutlier() {
		this(null);
	}

	public IForestModelOutlier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new IForestModelOutlierTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
