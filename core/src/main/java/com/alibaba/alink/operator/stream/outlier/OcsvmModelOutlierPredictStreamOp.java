package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.outlier.BaseModelOutlierPredictStreamOp;
import com.alibaba.alink.operator.common.outlier.IForestModelDetector;
import com.alibaba.alink.operator.common.outlier.OcsvmModelDetector;

@NameCn("One-Class SVM流式异常检测")
@NameEn("One-class svm model outlier prediction")
public class OcsvmModelOutlierPredictStreamOp
	extends BaseModelOutlierPredictStreamOp <OcsvmModelOutlierPredictStreamOp> {

	public OcsvmModelOutlierPredictStreamOp() {
		super(OcsvmModelDetector::new, new Params());
	}

	public OcsvmModelOutlierPredictStreamOp(Params params) {
		super(OcsvmModelDetector::new, params);
	}

	public OcsvmModelOutlierPredictStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public OcsvmModelOutlierPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, OcsvmModelDetector::new, params);
	}
}
