package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.stream.StreamOperator;

/**
 * Transform the Table to SourceStreamOp.
 */
@InputPorts()
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("Table数据源")
@NameEn("Table Source")
public final class TableSourceStreamOp extends StreamOperator <TableSourceStreamOp> {

	private static final long serialVersionUID = 6011949833466268149L;

	public TableSourceStreamOp(DataStream <Row> rows, String[] colNames, TypeInformation <?>[] colTypes) {
		//todo: sessionId
		this(
			DataStreamConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, rows, colNames,
				colTypes));
	}

	public TableSourceStreamOp(DataStream <Row> rows, String[] colNames, TypeInformation <?>[] colTypes,
							   long sessionId) {
		this(DataStreamConversionUtil.toTable(sessionId, rows, colNames, colTypes));
		setMLEnvironmentId(sessionId);
	}

	public TableSourceStreamOp(Table table) {
		super(null);
		AkPreconditions.checkArgument(table != null, "The source table cannot be null.");
		this.setOutputTable(table);
	}

	@Override
	public TableSourceStreamOp linkFrom(StreamOperator <?>... inputs) {
		throw new AkUnsupportedOperationException("Table source operator should not have any upstream to link from.");
	}

}
