package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.feature.featurebuilder.BaseOverWindowStreamOp;
import com.alibaba.alink.params.feature.featuregenerator.OverCountWindowParams;

/**
 * Stream feature builder base on over window with user-defined recent several pieces of data.
 */
@NameCn("特征构造：OverCountWindow")
@NameEn("Over count window generator")
public class OverCountWindowStreamOp
	extends BaseOverWindowStreamOp <OverCountWindowStreamOp>
	implements OverCountWindowParams <OverCountWindowStreamOp> {

	public OverCountWindowStreamOp() {
		super(null);
	}

	public OverCountWindowStreamOp(Params params) {
		super(params);
	}

	@Override
	public void generateWindowClause() {
		Integer precedingRows = getPrecedingRows();
		String strWindowRows = "UNBOUNDED";
		if (null != precedingRows && precedingRows >= 0) {
			strWindowRows = String.valueOf(precedingRows);
		}
		setWindowParams("ROWS", strWindowRows, -1);
	}
}
