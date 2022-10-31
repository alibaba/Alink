package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.IForestDetector;
import com.alibaba.alink.params.outlier.IForestDetectorParams;
import com.alibaba.alink.pipeline.outlier.IForestModelOutlier;

@NameCn("IForest异常检测")
@NameEn("IForest Outlier Detection")
public class IForestOutlierBatchOp extends BaseOutlierBatchOp <IForestOutlierBatchOp>
	implements IForestDetectorParams <IForestOutlierBatchOp> {

	public IForestOutlierBatchOp() {
		this(null);
	}

	public IForestOutlierBatchOp(Params params) {
		super(IForestDetector::new, params);
	}

	@Override
	protected boolean supportDealWholeData() {
		return true;
	}

	@Override
	protected Table dealWholeData(BatchOperator <?> in) {
		return new IForestModelOutlier(getParams()).fitAndTransform(in).getOutputTable();
	}

}
