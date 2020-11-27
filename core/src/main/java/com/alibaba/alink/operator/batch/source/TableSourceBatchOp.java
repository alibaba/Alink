package com.alibaba.alink.operator.batch.source;

import org.apache.flink.table.api.Table;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Transform the Table to SourceBatchOp.
 */
public final class TableSourceBatchOp extends BatchOperator <TableSourceBatchOp> {

	private static final long serialVersionUID = -5220231513565199001L;

	public TableSourceBatchOp(Table table) {
		super(null);
		Preconditions.checkArgument(table != null, "The source table cannot be null.");
		this.setOutputTable(table);
	}

	@Override
	public TableSourceBatchOp linkFrom(BatchOperator <?>... inputs) {
		throw new UnsupportedOperationException("Table source operator should not have any upstream to link from.");
	}

}
