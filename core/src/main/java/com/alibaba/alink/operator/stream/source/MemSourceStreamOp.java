package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Stream source that reads data from memory.
 */
@IoOpAnnotation(name = MemSourceStreamOp.NAME, ioType = IOType.SourceStream)
public final class MemSourceStreamOp extends BaseSourceStreamOp <MemSourceStreamOp> {

	static final String NAME = "memory";
	private static final long serialVersionUID = -6284651345216197637L;

	private List <Row> rows;
	private String[] colNames;
	private TypeInformation <?>[] colTypes;

	public MemSourceStreamOp(Object[] vals, String colName) {
		super(NAME, null);
		List <Row> rows = new ArrayList <Row>();
		for (Object str : vals) {
			rows.add(Row.of(str));
		}
		init(rows, new String[] {colName});
	}

	public MemSourceStreamOp(Object[][] vals, String[] colNames) {
		super(NAME, null);
		List <Row> rows = new ArrayList <Row>();
		for (int i = 0; i < vals.length; i++) {
			rows.add(Row.of(vals[i]));
		}
		init(rows, colNames);
	}

	public MemSourceStreamOp(List <Row> rows, TableSchema schema) {
		super(NAME, null);
		init(rows, schema.getFieldNames(), schema.getFieldTypes());
	}

	public MemSourceStreamOp(List <Row> rows, String schemaStr) {
		this(rows, CsvUtil.schemaStr2Schema(schemaStr));
	}

	public MemSourceStreamOp(Row[] rows, String[] colNames) {
		this(Arrays.asList(rows), colNames);
	}

	public MemSourceStreamOp(List <Row> rows, String[] colNames) {
		super(NAME, null);
		init(rows, colNames);
	}

	private void init(List <Row> rows, String[] colNames) {
		if (rows == null || rows.size() < 1) {
			throw new IllegalArgumentException("Values can not be empty.");
		}

		Row first = rows.iterator().next();

		int arity = first.getArity();

		TypeInformation <?>[] types = new TypeInformation[arity];

		for (int i = 0; i < arity; ++i) {
			types[i] = TypeExtractor.getForObject(first.getField(i));
		}

		init(rows, colNames, types);
	}

	private void init(List <Row> rows, String[] colNames, TypeInformation <?>[] colTypes) {
		if (null == colNames || colNames.length < 1) {
			throw new IllegalArgumentException("colNames can not be empty.");
		}
		this.rows = rows;
		this.colNames = colNames;
		this.colTypes = colTypes;
	}

	@Override
	protected Table initializeDataSource() {
		Long mlEnvironmentId = getMLEnvironmentId();
		DataStream <Row> dataStream = MLEnvironmentFactory.get(mlEnvironmentId)
			.getStreamExecutionEnvironment()
			.fromCollection(rows, new RowTypeInfo(colTypes))
			.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());;
		return DataStreamConversionUtil.toTable(mlEnvironmentId, dataStream, colNames, colTypes);
	}

}
