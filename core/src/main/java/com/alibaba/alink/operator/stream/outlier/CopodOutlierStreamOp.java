package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.CopodDetector;
import com.alibaba.alink.params.outlier.CopodDetectorParams;

@NameCn("COP异常检测")
@NameEn("COP Outlier")
public class CopodOutlierStreamOp extends BaseOutlierStreamOp <CopodOutlierStreamOp>
	implements CopodDetectorParams <CopodOutlierStreamOp> {

	public CopodOutlierStreamOp() {this(null);}

	public CopodOutlierStreamOp(Params params) {
		super(CopodDetector::new, params);
	}
}
