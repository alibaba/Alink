package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;

import java.util.Arrays;
import java.util.List;

/**
 * Stream source that reads data from memory.
 */
@IoOpAnnotation(name = MemSourceStreamOp.NAME, ioType = IOType.SourceStream)
@NameCn("内存数据源")
@NameEn("Memory Source")
public final class MemSourceStreamOp extends BaseSourceStreamOp <MemSourceStreamOp> {

	static final String NAME = "memory";
	private static final long serialVersionUID = -6284651345216197637L;

	private final MTable mt;

	public MemSourceStreamOp(MTable mt) {
		super(NAME, null);
		this.mt = mt;
	}

	public MemSourceStreamOp(Object[] vals, String colName) {
		super(NAME, null);
		this.mt = new MTable(vals, colName);
	}

	public MemSourceStreamOp(Object[][] vals, String[] colNames) {
		super(NAME, null);
		this.mt = new MTable(vals, colNames);
	}

	public MemSourceStreamOp(List <Row> rows, TableSchema schema) {
		super(NAME, null);
		this.mt = new MTable(rows, schema);
	}

	public MemSourceStreamOp(List <Row> rows, String schemaStr) {
		this(rows, TableUtil.schemaStr2Schema(schemaStr));
	}

	public MemSourceStreamOp(List <Row> rows, String[] colNames) {
		super(NAME, null);
		this.mt = new MTable(rows, colNames);
	}

	public MemSourceStreamOp(Row[] rows, TableSchema schema) {
		this(Arrays.asList(rows), schema);
	}

	public MemSourceStreamOp(Row[] rows, String schemaStr) {
		this(Arrays.asList(rows), schemaStr);
	}

	public MemSourceStreamOp(Row[] rows, String[] colNames) {
		this(Arrays.asList(rows), colNames);
	}

	@Override
	protected Table initializeDataSource() {
		Long mlEnvironmentId = getMLEnvironmentId();
		TypeInformation <?>[] colTypes = mt.getColTypes();
		DataStream <Row> dataStream = MLEnvironmentFactory.get(mlEnvironmentId)
			.getStreamExecutionEnvironment()
			.fromCollection(mt.getRows(), new RowTypeInfo(colTypes));
		return DataStreamConversionUtil.toTable(mlEnvironmentId, dataStream, mt.getColNames(), colTypes);
	}

}
