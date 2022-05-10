package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.HbosDetector;
import com.alibaba.alink.params.outlier.HbosDetectorParams;

@NameCn("HBOS异常检测")
@NameEn("HBOS Outlier")
public class HbosOutlierStreamOp extends BaseOutlierStreamOp <HbosOutlierStreamOp>
	implements HbosDetectorParams <HbosOutlierStreamOp> {

	public HbosOutlierStreamOp() {
		this(null);
	}

	public HbosOutlierStreamOp(Params params) {
		super(HbosDetector::new, params);
	}

}
