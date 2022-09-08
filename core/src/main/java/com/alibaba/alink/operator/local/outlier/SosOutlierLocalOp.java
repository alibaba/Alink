package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.outlier.SosDetector;
import com.alibaba.alink.params.outlier.SosDetectorParams;

@NameCn("SOS 异常检测")
public class SosOutlierLocalOp extends BaseOutlierLocalOp <SosOutlierLocalOp>
	implements SosDetectorParams <SosOutlierLocalOp> {

	public SosOutlierLocalOp() {
		this(null);
	}

	public SosOutlierLocalOp(Params params) {
		super(SosDetector::new, params);
	}

}
