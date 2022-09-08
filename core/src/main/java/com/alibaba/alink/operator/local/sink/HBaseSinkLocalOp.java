package com.alibaba.alink.operator.local.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.io.HBaseOutputFormat;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.io.HBaseSinkParams;

/**
 * BatchOperator to sink data to HBase.
 */
@ParamSelectColumnSpec(name = "rowKeyCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("导出到HBase")
public final class HBaseSinkLocalOp extends BaseSinkLocalOp <HBaseSinkLocalOp>
	implements HBaseSinkParams <HBaseSinkLocalOp> {

	public HBaseSinkLocalOp() {
		this(new Params());
	}

	public HBaseSinkLocalOp(Params params) {
		super(params);
	}

	@Override
	public HBaseSinkLocalOp sinkFrom(LocalOperator <?> in) {
		TableSchema schema = in.getSchema();

		output(
			in.getOutputTable().getRows(),
			new HBaseOutputFormat(getParams(), schema.getFieldNames(), schema.getFieldTypes()),
			1
		);

		return this;
	}

}
