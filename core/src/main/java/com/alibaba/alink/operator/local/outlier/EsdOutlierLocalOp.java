package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.EsdDetector;
import com.alibaba.alink.params.outlier.EsdDetectorParams;

@NameCn("ESD异常检测")
@NameEn("ESD Outlier")
/**
 * ESD outlier for data.
 */
public class EsdOutlierLocalOp extends BaseOutlierLocalOp <EsdOutlierLocalOp>
	implements EsdDetectorParams <EsdOutlierLocalOp> {

	public EsdOutlierLocalOp() {
		this(null);
	}

	public EsdOutlierLocalOp(Params params) {
		super(EsdDetector::new, params);
	}

}
