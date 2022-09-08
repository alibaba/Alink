package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.HbosDetector;
import com.alibaba.alink.params.outlier.HbosDetectorParams;

@NameCn("HBOS异常检测")
@NameEn("HBOS Outlier")
public class HbosOutlierLocalOp extends BaseOutlierLocalOp <HbosOutlierLocalOp>
	implements HbosDetectorParams <HbosOutlierLocalOp> {

	public HbosOutlierLocalOp() {
		this(null);
	}

	public HbosOutlierLocalOp(Params params) {
		super(HbosDetector::new, params);
	}

}
