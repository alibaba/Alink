package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.HbosDetector;
import com.alibaba.alink.params.outlier.HbosDetectorParams;

@NameCn("HBOS序列异常检测")
@NameEn("HBOS Series Outlier")
public class HbosOutlier4GroupedDataLocalOp extends BaseOutlier4GroupedDataLocalOp <HbosOutlier4GroupedDataLocalOp>
	implements HbosDetectorParams <HbosOutlier4GroupedDataLocalOp> {

	public HbosOutlier4GroupedDataLocalOp() {
		this(null);
	}

	public HbosOutlier4GroupedDataLocalOp(Params params) {
		super(HbosDetector::new, params);
	}

}
