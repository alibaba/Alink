package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.commons.lang.ArrayUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Utils for handling dataset.
 */
@SuppressWarnings("unchecked")
public class DataSetUtil {
	/**
	 * Count number of records in the dataset.
	 *
	 * @return a dataset of one record, recording the number of records of [[dataset]]
	 */
	public static <T> DataSet <Long> count(DataSet <T> dataSet) {
		return dataSet
			.mapPartition(new MapPartitionFunction <T, Long>() {
				private static final long serialVersionUID = 5351290184340971835L;

				@Override
				public void mapPartition(Iterable <T> values, Collector <Long> out) throws Exception {
					long cnt = 0L;
					for (T v : values) {
						cnt++;
					}
					out.collect(cnt);
				}
			})
			.name("count_dataset")
			.returns(Types.LONG)
			.reduce(new ReduceFunction <Long>() {
				private static final long serialVersionUID = -4281590383844098422L;

				@Override
				public Long reduce(Long value1, Long value2) throws Exception {
					return value1 + value2;
				}
			});
	}

	public static BatchOperator count(BatchOperator data) {
		DataSet <Long> count = data.getDataSet()
			.mapPartition(new MapPartitionFunction <Row, Long>() {
				private static final long serialVersionUID = -7352692344227251372L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Long> out) throws Exception {
					long cnt = 0L;
					for (Row v : values) {
						cnt++;
					}
					out.collect(cnt);
				}
			})
			.name("count_dataset")
			.reduce(new ReduceFunction <Long>() {
				private static final long serialVersionUID = 1164352453904681248L;

				@Override
				public Long reduce(Long value1, Long value2) throws Exception {
					return value1 + value2;
				}
			});
		DataSet <Row> rowDataSet = count.map(new MapFunction <Long, Row>() {
			private static final long serialVersionUID = -5014428103964711477L;

			@Override
			public Row map(Long value) throws Exception {
				return Row.of(value);
			}
		});
		return BatchOperator.fromTable(DataSetConversionUtil.toTable(data.getMLEnvironmentId(),
			rowDataSet, new String[] {"num_records"}, new TypeInformation[] {Types.LONG}));
	}

	/**
	 * Returns an empty dataset of the same type as [[dataSet]].
	 */
	public static <T> DataSet <T> empty(DataSet <T> dataSet) {
		return dataSet
			.flatMap(new FlatMapFunction <T, T>() {
				private static final long serialVersionUID = 4385675521544606204L;

				@Override
				public void flatMap(T t, Collector <T> collector) throws Exception {
				}
			})
			.returns(dataSet.getType());
	}

	/**
	 * Add a hand made barrier to a dataset.
	 */
	public static <T> DataSet <T> barrier(DataSet <T> dataSet) {
		return dataSet
			.flatMap(new FlatMapFunction <T, T>() {
				private static final long serialVersionUID = -314924938979563398L;

				@Override
				public void flatMap(T t, Collector <T> collector) throws Exception {
					collector.collect(t);
				}
			})
			.withBroadcastSet(empty(dataSet), "empty")
			.name("barrier")
			.returns(dataSet.getType());
	}

	public static DataSet <Row> createEmptyDataSet(ExecutionEnvironment env, TableSchema schema1, TableSchema
		schema2) {
		TableSchema mergedSchema = new TableSchema(
			(String[]) ArrayUtils.addAll(schema1.getFieldNames(), schema2.getFieldNames()),
			(TypeInformation <?>[]) ArrayUtils.addAll(schema1.getFieldTypes(), schema2.getFieldTypes())
		);
		return createEmptyDataSet(env, mergedSchema);
	}

	public static DataSet <Row> createEmptyDataSet(ExecutionEnvironment env, TableSchema schema) {
		DataSet <Row> rows = env
			.fromElements(0)
			.flatMap(new FlatMapFunction <Integer, Row>() {
				private static final long serialVersionUID = 7566732134539040198L;

				@Override
				public void flatMap(Integer value, Collector <Row> out) throws Exception {
				}
			})
			.returns(new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames()));
		return rows;
	}

	public static DataSet <Row> removeLastColumn(DataSet <Row> rows) {
		return rows.map(new MapFunction <Row, Row>() {
			private static final long serialVersionUID = -6052009263843274262L;

			@Override
			public Row map(Row value) throws Exception {
				Row row = new Row(value.getArity() - 1);
				for (int i = 0; i < value.getArity() - 1; i++) {
					row.setField(i, value.getField(i));
				}
				return row;
			}
		});
	}

	/**
	 * Stack a dataset of rows
	 */
	public static DataSet <List <Row>> stack(DataSet <Row> input, final int size) {
		return input
			.mapPartition(new RichMapPartitionFunction <Row, List <Row>>() {
				private static final long serialVersionUID = 4066908302859627800L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <List <Row>> out) throws Exception {
					List <Row> buffer = new ArrayList <>();
					for (Row row : values) {
						buffer.add(row);
						if (buffer.size() >= size) {
							out.collect(buffer);
							buffer.clear();
						}
					}
					if (buffer.size() > 0) {
						out.collect(buffer);
						buffer.clear();
					}
				}
			});
	}
}
