package com.alibaba.alink.operator.batch.graph.utils;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class IDMappingUtils {

	/**
	 * building ID mapping: Map<String, Long>: treat colIds in dataset as String and use one dict to train them.
	 *
	 * @param dataSet
	 * @param colIds
	 * @return
	 */
	public static DataSet <Tuple2 <String, Long>> computeIdMapping(DataSet <Row> dataSet, int[] colIds) {
		return dataSet.flatMap(
			new FlatMapFunction <Row, String>() {
				@Override
				public void flatMap(Row value, Collector <String> out) throws Exception {
					for (int i = 0; i < colIds.length; i++) {
						out.collect((String) value.getField(colIds[i]));
					}
				}
			})
			.distinct()
			.map(new RichMapFunction <String, Tuple2 <String, Long>>() {

				long cnt;
				int numTasks;
				int taskId;

				@Override
				public void open(Configuration parameters) throws Exception {
					cnt = 0L;
					numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
					taskId = getRuntimeContext().getIndexOfThisSubtask();
				}

				@Override
				public Tuple2 <String, Long> map(String value) throws Exception {
					return Tuple2.of(value, numTasks * (cnt++) + taskId);
				}
			})
			.name("build_node_mapping");
	}

	/**
	 * map ${colIds} columns of ${original} dataset using ${dict}
	 *
	 * @param original
	 * @param dict
	 * @param colIds
	 * @return
	 */
	public static DataSet <Row> mapDataSetWithIdMapping(DataSet <Row> original, DataSet <Tuple2 <String, Long>> dict,
														int[] colIds) {
		DataSet <Row> tmp = original;
		for (int i = 0; i < colIds.length; i++) {
			int colId = colIds[i];
			tmp = tmp.coGroup(dict).where(new KeySelector <Row, String>() {
				@Override
				public String getKey(Row value) throws Exception {
					return (String) value.getField(colId);
				}
			}).equalTo(0).with(new CoGroupFunction <Row, Tuple2 <String, Long>, Row>() {
				@Override
				public void coGroup(Iterable <Row> first, Iterable <Tuple2 <String, Long>> second,
									Collector <Row> out) throws Exception {
					long idx = second.iterator().next().f1;
					for (Row r : first) {
						r.setField(colId, idx);
						out.collect(r);
					}
				}
			}).name("cogroup at " + colId);
		}
		return tmp;
	}

	/**
	 * transform random walk back to string
	 */
	public static DataSet <Row> mapWalkToStringWithIdMapping(DataSet <long[]> path,
															 DataSet <Tuple2 <String, Long>> nodeMapping,
															 final int walkLength,
															 final String delimiter) {
		DataSet <Tuple3 <Long, Long, Long>> pathTriple = path.flatMap(
			new RichFlatMapFunction <long[], Tuple3 <Long, Long, Long>>() {
				long cnt;
				int numTasks;
				int taskId;

				@Override
				public void open(Configuration parameters) throws Exception {
					cnt = 0L;
					numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
					taskId = getRuntimeContext().getIndexOfThisSubtask();
				}

				@Override
				public void flatMap(long[] walk, Collector <Tuple3 <Long, Long, Long>> out) throws Exception {
					for (int i = 0; i < walk.length; ++i) {
						out.collect(Tuple3.of(cnt * numTasks + taskId, (long) i, walk[i]));
					}
					cnt++;
				}
			}).name("int2string_map");

		return pathTriple.coGroup(nodeMapping).where(2).equalTo(1).with(
			new CoGroupFunction <Tuple3 <Long, Long, Long>, Tuple2 <String, Long>, Tuple3 <Long,
				Long,
				String>>() {
				private static final long serialVersionUID = 1881422618869562798L;

				@Override
				public void coGroup(Iterable <Tuple3 <Long, Long, Long>> first,
									Iterable <Tuple2 <String, Long>> second,
									Collector <Tuple3 <Long, Long, String>> out) throws Exception {
					String strVal = second.iterator().next().f0;
					for (Tuple3 <Long, Long, Long> t3 : first) {
						out.collect(Tuple3.of(t3.f0, t3.f1, strVal));
					}
				}
			}).name("int2string_cogroup")
			.groupBy(0).reduceGroup(new GroupReduceFunction <Tuple3 <Long, Long, String>, Row>() {
				private static final long serialVersionUID = 4413395610271058332L;
				String[] tmpArray = new String[walkLength];
				int maxIdx;

				@Override
				public void reduce(Iterable <Tuple3 <Long, Long, String>> values, Collector <Row> out)
					throws Exception {
					maxIdx = -1;
					for (Tuple3 <Long, Long, String> t3 : values) {
						int idx = t3.f1.intValue();
						tmpArray[idx] = t3.f2;
						maxIdx = Math.max(maxIdx, idx);
					}
					StringBuilder sb = new StringBuilder();
					for (int i = 0; i < maxIdx; ++i) {
						sb.append(tmpArray[i]);
						sb.append(delimiter);
					}
					sb.append(tmpArray[maxIdx]);
					Row r = new Row(1);
					r.setField(0, sb.toString());
					out.collect(r);
				}
			}).name("int2string_reduce");
	}
}
