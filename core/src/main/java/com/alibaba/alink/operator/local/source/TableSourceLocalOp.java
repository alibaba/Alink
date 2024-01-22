package com.alibaba.alink.operator.local.source;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.operator.local.LocalOperator;

/**
 * Transform the Table to SourceBatchOp.
 */
@InputPorts()
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("Table数据读入")
public final class TableSourceLocalOp extends LocalOperator <TableSourceLocalOp> {

	public TableSourceLocalOp(MTable table) {
		super(null);
		AkPreconditions.checkArgument(table != null, "The source table cannot be null.");
		this.setOutputTable(table);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		throw new AkUnsupportedOperationException("Table source operator should not have any upstream to link from.");
	}

}
