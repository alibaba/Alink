package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.stream.StreamOperator;

/**
 * Transform the Table to SourceStreamOp.
 */
public final class TableSourceStreamOp extends StreamOperator <TableSourceStreamOp> {

	public TableSourceStreamOp(DataStream <Row> rows, String[] colNames, TypeInformation <?>[] colTypes) {
		//todo: sessionId
		this(DataStreamConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, rows, colNames, colTypes));
	}

	public TableSourceStreamOp(DataStream <Row> rows, String[] colNames, TypeInformation <?>[] colTypes, long sessionId) {
		this(DataStreamConversionUtil.toTable(sessionId, rows, colNames, colTypes));
		setMLEnvironmentId(sessionId);
	}

	public TableSourceStreamOp(Table table) {
		super(null);
		Preconditions.checkArgument(table != null, "The source table cannot be null.");
		this.setOutputTable(table);
	}

	@Override
	public TableSourceStreamOp linkFrom(StreamOperator<?>... inputs) {
		throw new UnsupportedOperationException("Table source operator should not have any upstream to link from.");
	}

}
