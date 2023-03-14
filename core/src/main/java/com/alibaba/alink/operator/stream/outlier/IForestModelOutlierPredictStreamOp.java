package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.outlier.BaseModelOutlierPredictStreamOp;
import com.alibaba.alink.operator.common.outlier.IForestModelDetector;

@NameCn("IForest模型异常检测预测")
@NameEn("IForest model outlier prediction")
public class IForestModelOutlierPredictStreamOp
	extends BaseModelOutlierPredictStreamOp <IForestModelOutlierPredictStreamOp> {

	public IForestModelOutlierPredictStreamOp() {
		super(IForestModelDetector::new, new Params());
	}

	public IForestModelOutlierPredictStreamOp(Params params) {
		super(IForestModelDetector::new, params);
	}

	public IForestModelOutlierPredictStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public IForestModelOutlierPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, IForestModelDetector::new, params);
	}
}
