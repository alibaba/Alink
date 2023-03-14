package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.TripleToCsvParams;

/**
 * Transform data type from Triple to Csv.
 */
@ParamSelectColumnSpec(name = "tripleColumnCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("三元组转CSV")
@NameEn("Triple To CSV")
public class TripleToCsvBatchOp extends TripleToAnyBatchOp <TripleToCsvBatchOp>
	implements TripleToCsvParams <TripleToCsvBatchOp> {

	private static final long serialVersionUID = 7557633497922961728L;

	public TripleToCsvBatchOp() {
		this(new Params());
	}

	public TripleToCsvBatchOp(Params params) {
		super(FormatType.CSV, params);
	}
}
