package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataBatchOp;
import com.alibaba.alink.operator.common.outlier.HbosDetector;
import com.alibaba.alink.params.outlier.HbosDetectorParams;

@NameCn("HBOS序列异常检测")
@NameEn("HBOS Series Outlier")
public class HbosOutlier4GroupedDataBatchOp extends BaseOutlier4GroupedDataBatchOp <HbosOutlier4GroupedDataBatchOp>
	implements HbosDetectorParams <HbosOutlier4GroupedDataBatchOp> {

	public HbosOutlier4GroupedDataBatchOp() {
		this(null);
	}

	public HbosOutlier4GroupedDataBatchOp(Params params) {
		super(HbosDetector::new, params);
	}

}
