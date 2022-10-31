package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.IForestDetector;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.outlier.IForestDetectorParams;
import com.alibaba.alink.pipeline.outlier.IForestModelOutlier;

@NameCn("IForest异常检测")
@NameEn("IForest Outlier Detection")
public class IForestOutlierLocalOp extends BaseOutlierLocalOp <IForestOutlierLocalOp>
	implements IForestDetectorParams <IForestOutlierLocalOp> {

	public IForestOutlierLocalOp() {
		this(null);
	}

	public IForestOutlierLocalOp(Params params) {
		super(IForestDetector::new, params);
	}

	@Override
	protected boolean supportDealWholeData() {
		return true;
	}

	@Override
	protected MTable dealWholeData(LocalOperator <?> in) {
		return new IForestModelOutlier(getParams()).fitAndTransform(in).getOutputTable();
	}
}
