package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseModelOutlierPredictBatchOp;
import com.alibaba.alink.operator.common.outlier.OcsvmModelDetector;
@NameCn("One Class SVM异常检测模型预测")
@NameEn("Ocsvm outlier model predict")
public final class OcsvmModelOutlierPredictBatchOp
	extends BaseModelOutlierPredictBatchOp <OcsvmModelOutlierPredictBatchOp> {

	private static final long serialVersionUID = 7075220963340722343L;

	public OcsvmModelOutlierPredictBatchOp() {
		this(new Params());
	}

	public OcsvmModelOutlierPredictBatchOp(Params params) {
		super(OcsvmModelDetector::new, params);
	}
}
