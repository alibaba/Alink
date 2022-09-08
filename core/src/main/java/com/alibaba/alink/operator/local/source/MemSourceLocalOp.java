package com.alibaba.alink.operator.local.source;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.TableUtil;

import java.util.Arrays;
import java.util.List;

/**
 * A data source that reads memory data.
 */
@NameCn("内存数据读入")
public final class MemSourceLocalOp extends BaseSourceLocalOp <MemSourceLocalOp> {

	private final MTable mt;

	public MemSourceLocalOp(MTable mt) {
		super(null);
		this.mt = mt;
	}

	public MemSourceLocalOp(Object[] vals, String colName) {
		super(null);
		this.mt = new MTable(vals, colName);
	}

	public MemSourceLocalOp(Object[][] vals, String[] colNames) {
		super(null);
		this.mt = new MTable(vals, colNames);
	}

	public MemSourceLocalOp(List <Row> rows, TableSchema schema) {
		super(null);
		this.mt = new MTable(rows, schema);
	}

	public MemSourceLocalOp(List <Row> rows, String schemaStr) {
		this(rows, TableUtil.schemaStr2Schema(schemaStr));
	}

	public MemSourceLocalOp(List <Row> rows, String[] colNames) {
		super(null);
		this.mt = new MTable(rows, colNames);
	}

	public MemSourceLocalOp(Row[] rows, TableSchema schema) {
		this(Arrays.asList(rows), schema);
	}

	public MemSourceLocalOp(Row[] rows, String schemaStr) {
		this(Arrays.asList(rows), schemaStr);
	}

	public MemSourceLocalOp(Row[] rows, String[] colNames) {
		this(Arrays.asList(rows), colNames);
	}

	@Override
	protected MTable initializeDataSource() {
		return mt;
	}
}
