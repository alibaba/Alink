package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A data source that reads memory data.
 */
@IoOpAnnotation(name = MemSourceBatchOp.NAME, ioType = IOType.SourceBatch)
public final class MemSourceBatchOp extends BaseSourceBatchOp<MemSourceBatchOp> {

	static final String NAME = "memory";

	private List<Row> rows;
	private String[] colNames;
	private TypeInformation<?>[] colTypes;

	public MemSourceBatchOp(Object[] vals, String colName) {
		super(NAME, null);
		List<Row> rows = new ArrayList<>();
		for (Object val : vals) {
			rows.add(Row.of(val));
		}
		init(rows, new String[] {colName});
	}

	public MemSourceBatchOp(Object[][] vals, String[] colNames) {
		super(NAME, null);
		List <Row> rows = new ArrayList <>();
		for (Object[] val : vals) {
			rows.add(Row.of(val));
		}
		init(rows, colNames);
	}

	public MemSourceBatchOp(List <Row> rows, TableSchema schema) {
		super(NAME, null);
		init(rows, schema.getFieldNames(), schema.getFieldTypes());
	}

	public MemSourceBatchOp(Row[] rows, String[] colNames) {
		this(Arrays.asList(rows), colNames);
	}

	public MemSourceBatchOp(List <Row> rows, String[] colNames) {
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

	private void init(List<Row> rows, String[] colNames, TypeInformation<?>[] colTypes) {
		this.rows = rows;
		this.colNames = colNames;
		this.colTypes = colTypes;
	}

	@Override
	public MemSourceBatchOp linkFrom(BatchOperator<?>... inputs) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	protected Table initializeDataSource() {
		DataSet <Row> dataSet = MLEnvironmentFactory.get(getMLEnvironmentId())
				.getExecutionEnvironment()
				.fromCollection(rows, new RowTypeInfo(colTypes));
		return DataSetConversionUtil.toTable(getMLEnvironmentId(), dataSet, colNames, colTypes);
	}

}
