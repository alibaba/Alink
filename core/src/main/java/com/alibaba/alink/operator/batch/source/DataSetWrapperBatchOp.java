package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;

import com.alibaba.alink.operator.batch.BatchOperator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

/**
 * A BatchOperator that wraps a DataSet.
 */
public final class DataSetWrapperBatchOp extends BatchOperator<DataSetWrapperBatchOp> {

	final DataSet<Row> rawData;
	final String[] rawColNames;
	final TypeInformation<?>[] rawColTypes;

	public DataSetWrapperBatchOp(DataSet<Row> batchOperator, String[] colNames, TypeInformation <?>[] colTypes) {
		super(null);
		this.rawData = batchOperator;
		this.rawColNames = colNames;
		this.rawColTypes = colTypes;
	}

	@Override
	public DataSet<Row> getDataSet() {
		return rawData;
	}

	@Override
	public Table getOutputTable() {
		if (super.getOutputTable() == null) {
			super.setOutput(rawData, rawColNames, rawColTypes);
		}
		return super.getOutputTable();
	}

	@Override
	public DataSetWrapperBatchOp linkFrom(BatchOperator<?>... inputs) {
		throw new RuntimeException("Unsupported now.");
	}
}
