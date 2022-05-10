package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataStreamOp;
import com.alibaba.alink.operator.common.outlier.HbosDetector;
import com.alibaba.alink.params.outlier.HbosDetectorParams;

@NameCn("HBOS序列异常检测")
@NameEn("HBOS Series Outlier")
public class HbosOutlier4GroupedDataStreamOp extends BaseOutlier4GroupedDataStreamOp <HbosOutlier4GroupedDataStreamOp>
	implements HbosDetectorParams <HbosOutlier4GroupedDataStreamOp> {

	public HbosOutlier4GroupedDataStreamOp() {
		this(null);
	}

	public HbosOutlier4GroupedDataStreamOp(Params params) {
		super(HbosDetector::new, params);
	}

}
