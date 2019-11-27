package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.stream.StreamOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Stream source that reads data from memory.
 */
public final class MemSourceStreamOp extends StreamOperator<MemSourceStreamOp> {

	public MemSourceStreamOp(Object[] vals, String colName) {
		super(null);
		List <Row> rows = new ArrayList <Row>();
		for (Object str : vals) {
			rows.add(Row.of(str));
		}
		init(rows, new String[] {colName});
	}

	public MemSourceStreamOp(Object[][] vals, String[] colNames) {
		super(null);
		List <Row> rows = new ArrayList <Row>();
		for (int i = 0; i < vals.length; i++) {
			rows.add(Row.of(vals[i]));
		}
		init(rows, colNames);
	}

	public MemSourceStreamOp(List <Row> rows, TableSchema schema) {
		super();
		init(rows, schema.getFieldNames(), schema.getFieldTypes());
	}

	public MemSourceStreamOp(Row[] rows, String[] colNames) {
		this(Arrays.asList(rows), colNames);
	}

	public MemSourceStreamOp(List <Row> rows, String[] colNames) {
		super(null);
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

		DataStream <Row> dastr = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamExecutionEnvironment().fromCollection(rows);

		StringBuilder sbd = new StringBuilder();
		sbd.append(colNames[0]);
		for (int i = 1; i < colNames.length; i++) {
			sbd.append(",").append(colNames[i]);
		}

		this.setOutput(dastr, colNames, colTypes);
	}

	@Override
	public MemSourceStreamOp linkFrom(StreamOperator<?>... inputs) {
		throw new UnsupportedOperationException(
			"Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

}
