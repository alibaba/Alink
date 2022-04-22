package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.EsdDetector;
import com.alibaba.alink.params.outlier.EsdDetectorParams;

@NameCn("ESD异常检测")
@NameEn("ESD Outlier")
/**
 * ESD outlier for data.
 */
public class EsdOutlierStreamOp extends BaseOutlierStreamOp <EsdOutlierStreamOp>
	implements EsdDetectorParams <EsdOutlierStreamOp> {

	public EsdOutlierStreamOp() {
		this(null);
	}

	public EsdOutlierStreamOp(Params params) {
		super(EsdDetector::new, params);
	}

}
