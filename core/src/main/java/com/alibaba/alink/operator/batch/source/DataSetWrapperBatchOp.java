package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * A BatchOperator that wraps a DataSet.
 */
@Internal
public final class DataSetWrapperBatchOp extends BatchOperator <DataSetWrapperBatchOp> {

	private static final long serialVersionUID = -6352627701658703146L;
	final DataSet <Row> rawData;
	final String[] rawColNames;
	final TypeInformation <?>[] rawColTypes;

	public DataSetWrapperBatchOp(DataSet <Row> batchOperator, String[] colNames, TypeInformation <?>[] colTypes) {
		super(null);
		this.rawData = batchOperator;
		this.rawColNames = colNames;
		this.rawColTypes = colTypes;
	}

	@Override
	public DataSet <Row> getDataSet() {
		return rawData;
	}

	@Override
	public Table getOutputTable() {
		if (isNullOutputTable()) {
			super.setOutput(rawData, rawColNames, rawColTypes);
		}
		return super.getOutputTable();
	}

	@Override
	public DataSetWrapperBatchOp linkFrom(BatchOperator <?>... inputs) {
		throw new RuntimeException("Unsupported now.");
	}
}
