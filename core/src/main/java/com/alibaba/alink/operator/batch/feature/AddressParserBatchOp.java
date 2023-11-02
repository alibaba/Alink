package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.feature.address.AddressParserMapper;
import com.alibaba.alink.params.mapper.SISOMapperParams;


@ParamSelectColumnSpec(name="selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("地址解析")
@NameEn("Address Parser")
public class AddressParserBatchOp extends MapBatchOp <AddressParserBatchOp>
	implements SISOMapperParams <AddressParserBatchOp> {

	public AddressParserBatchOp() {
		this(null);
	}

	public AddressParserBatchOp(Params params) {
		super(AddressParserMapper::new, params);
	}
}