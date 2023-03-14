package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;

import java.util.Arrays;
import java.util.List;

/**
 * A data source that reads memory data.
 */
@IoOpAnnotation(name = MemSourceBatchOp.NAME, ioType = IOType.SourceBatch)
@NameCn("内存数据读入")
@NameEn("Memory Source")
public final class MemSourceBatchOp extends BaseSourceBatchOp <MemSourceBatchOp> {

	static final String NAME = "memory";
	private static final long serialVersionUID = 5004724502456338971L;

	private final MTable mt;

	public MemSourceBatchOp(MTable mt) {
		super(NAME, null);
		this.mt = mt;
	}

	public MemSourceBatchOp(Object[] vals, String colName) {
		super(NAME, null);
		this.mt = new MTable(vals, colName);
	}

	public MemSourceBatchOp(Object[][] vals, String[] colNames) {
		super(NAME, null);
		this.mt = new MTable(vals, colNames);
	}

	public MemSourceBatchOp(List <Row> rows, TableSchema schema) {
		super(NAME, null);
		this.mt = new MTable(rows, schema);
	}

	public MemSourceBatchOp(List <Row> rows, String schemaStr) {
		this(rows, TableUtil.schemaStr2Schema(schemaStr));
	}

	public MemSourceBatchOp(List <Row> rows, String[] colNames) {
		super(NAME, null);
		this.mt = new MTable(rows, colNames);
	}

	public MemSourceBatchOp(Row[] rows, TableSchema schema) {
		this(Arrays.asList(rows), schema);
	}

	public MemSourceBatchOp(Row[] rows, String schemaStr) {
		this(Arrays.asList(rows), schemaStr);
	}

	public MemSourceBatchOp(Row[] rows, String[] colNames) {
		this(Arrays.asList(rows), colNames);
	}

	public MTable getMt() {
		return mt;
	}

	@Override
	protected Table initializeDataSource() {
		Long mlEnvironmentId = getMLEnvironmentId();
		TypeInformation <?>[] colTypes = mt.getColTypes();
		DataSet <Row> dataSet = MLEnvironmentFactory.get(mlEnvironmentId)
			.getExecutionEnvironment()
			.fromCollection(mt.getRows(), new RowTypeInfo(colTypes));
		return DataSetConversionUtil.toTable(mlEnvironmentId, dataSet, mt.getColNames(), colTypes);
	}
}
