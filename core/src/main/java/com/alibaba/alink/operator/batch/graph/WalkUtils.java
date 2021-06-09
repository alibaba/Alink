package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.DataSetConversionUtil;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

public class WalkUtils {

	/**
	 * building node mapping.
	 */
	public static DataSet <Tuple2 <String, Integer>> getStringIndexMapping(DataSet <Edge <String, Double>> edge) {
		return edge.flatMap(
			new FlatMapFunction <Edge <String, Double>, Tuple1 <String>>() {
				private static final long serialVersionUID = -5370176397845035191L;

				@Override
				public void flatMap(Edge <String, Double> value, Collector <Tuple1 <String>> out) throws Exception {
					out.collect(Tuple1.of(value.f0));
					out.collect(Tuple1.of(value.f1));
				}
			})
			.distinct(0)
			.mapPartition(new RichMapPartitionFunction <Tuple1 <String>, Tuple2 <String, Integer>>() {
				private static final long serialVersionUID = 8151116141251455335L;
				private int cnt = 0;

				@Override
				public void mapPartition(Iterable <Tuple1 <String>> values, Collector <Tuple2 <String, Integer>> out)
					throws Exception {
					int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
					int taskId = getRuntimeContext().getIndexOfThisSubtask();

					for (Tuple1 <String> node : values) {
						out.collect(Tuple2.of(node.f0, numTasks * cnt + taskId));
						cnt++;
					}
				}
			}).name("build_node_mapping");
	}

	/**
	 * transform string node to int node.
	 */
	public static DataSet <Edge <Integer, Double>> string2Index(DataSet <Edge <String, Double>> edge,
																DataSet <Tuple2 <String, Integer>> nodeMapping) {
		return edge.coGroup(nodeMapping).where(0).equalTo(0).with(
			new RichCoGroupFunction <Edge <String, Double>, Tuple2 <String, Integer>, Edge <String, Double>>() {
				private static final long serialVersionUID = 701006967052390253L;

				@Override
				public void coGroup(Iterable <Edge <String, Double>> first, Iterable <Tuple2 <String, Integer>> second,
									Collector <Edge <String, Double>> out) throws Exception {
					String val = null;
					for (Tuple2 <String, Integer> t2 : second) {
						val = String.valueOf(t2.f1);
					}
					if (val != null) {
						for (Edge <String, Double> e : first) {
							e.f0 = val;
							out.collect(e);
						}
					}

				}
			}).name("cogroup_1_string2int")
			.coGroup(nodeMapping).where(1).equalTo(0).with(
				new RichCoGroupFunction <Edge <String, Double>, Tuple2 <String, Integer>, Edge <String, Double>>() {
					private static final long serialVersionUID = -4562871061588704431L;

					@Override
					public void coGroup(Iterable <Edge <String, Double>> first,
										Iterable <Tuple2 <String, Integer>> second,
										Collector <Edge <String, Double>> out) throws Exception {
						String val = null;
						for (Tuple2 <String, Integer> t2 : second) {
							val = String.valueOf(t2.f1);
						}
						if (val != null) {
							for (Edge <String, Double> e : first) {
								e.f1 = val;
								out.collect(e);
							}
						}
					}
				}).name("cogroup_2_string2int")
			.map(new MapFunction <Edge <String, Double>, Edge <Integer, Double>>() {
				private static final long serialVersionUID = 6843565151543859939L;

				@Override
				public Edge <Integer, Double> map(Edge <String, Double> value) throws Exception {
					return new Edge <Integer, Double>(Integer.valueOf(value.f0), Integer.valueOf(value.f1), value.f2);
				}
			}).name("map_string2int");
	}

	/**
	 * transform int node to string node.
	 */
	public static DataSet <String[]> index2String(DataSet <Integer[]> path,
												  DataSet <Tuple2 <String, Integer>> nodeMapping,
												  final int walkLength) {
		DataSet <Tuple3 <Integer, Integer, Integer>> pathTriple = path.mapPartition(
			new RichMapPartitionFunction <Integer[], Tuple3 <Integer, Integer, Integer>>() {
				private static final long serialVersionUID = -595431693895175701L;

				@Override
				public void mapPartition(Iterable <Integer[]> values,
										 Collector <Tuple3 <Integer, Integer, Integer>> out)
					throws Exception {
					int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					int cnt = 0;
					for (Integer[] node : values) {
						for (int i = 0; i < node.length; ++i) {
							if (node[i] != null) {
								out.collect(Tuple3.of(cnt * numTasks + taskId, i, node[i]));
							}
						}
						cnt++;
					}
				}
			}).name("int2string_map");

		return pathTriple.coGroup(nodeMapping).where(2).equalTo(1).with(
			new CoGroupFunction <Tuple3 <Integer, Integer, Integer>, Tuple2 <String, Integer>, Tuple3 <Integer,
				Integer,
				String>>() {
				private static final long serialVersionUID = 1881422618869562798L;

				@Override
				public void coGroup(Iterable <Tuple3 <Integer, Integer, Integer>> first,
									Iterable <Tuple2 <String, Integer>> second,
									Collector <Tuple3 <Integer, Integer, String>> out) throws Exception {
					String val = null;
					for (Tuple2 <String, Integer> t2 : second) {
						val = t2.f0;
					}
					if (val != null) {
						for (Tuple3 <Integer, Integer, Integer> t3 : first) {
							out.collect(Tuple3.of(t3.f0, t3.f1, val));
						}
					}
				}
			}).name("int2string_cogroup")
			.groupBy(0).reduceGroup(new GroupReduceFunction <Tuple3 <Integer, Integer, String>, String[]>() {
				private static final long serialVersionUID = 4413395610271058332L;

				@Override
				public void reduce(Iterable <Tuple3 <Integer, Integer, String>> values, Collector <String[]> out)
					throws Exception {
					String[] array = new String[walkLength];
					int maxidx = -1;
					for (Tuple3 <Integer, Integer, String> t3 : values) {
						array[t3.f1] = t3.f2;
						maxidx = Math.max(maxidx, t3.f1);
					}
					String[] ret = new String[maxidx + 1];
					for (int i = 0; i < maxidx + 1; ++i) {
						ret[i] = array[i];
					}
					out.collect(ret);
				}
			}).name("int2string_reduce");
	}

	public static Integer weightSample(Tuple2 <Integer, Double>[] dic, Random seed) {
		double val = seed.nextDouble();
		int idx = Arrays.binarySearch(dic, 0, dic.length - 1, Tuple2.of(-1, val),
			new Comparator <Tuple2 <Integer, Double>>() {
				@Override
				public int compare(Tuple2 <Integer, Double> s1, Tuple2 <Integer, Double> s2) {
					return s1.f1.compareTo(s2.f1);
				}
			});

		return dic[Math.abs(idx) - 1].f0;
	}

	public static Table transString(DataSet <String[]> data, String[] colNames, final String delimiter,
									Long sessionId) {
		return DataSetConversionUtil.toTable(sessionId, data
			.map(new MapFunction <String[], Row>() {
				private static final long serialVersionUID = -619480692071872228L;

				@Override
				public Row map(String[] t) {

					Row r = new Row(1);
					StringBuilder s = new StringBuilder();
					for (int i = 0; i < t.length; i++) {
						if (i == 0) {
							s.append(t[i].toString());
						} else {
							s.append(delimiter);
							s.append(t[i].toString());
						}
					}
					r.setField(0, s.toString());
					return r;
				}
			})
			.returns(new RowTypeInfo(new TypeInformation <?>[] {Types.STRING})), colNames);
	}

	public static DataSet <Edge <String, Double>> buildEdge(DataSet <Row> data,
															final int node0Idx,
															final int node1Idx,
															final int valueIdx,
															final boolean isToUnDigraph) {

		return data.flatMap(new FlatMapFunction <Row, Tuple3 <String, String, Double>>() {
			private static final long serialVersionUID = -8067433646736979293L;

			@Override
			public void flatMap(Row value, Collector <Tuple3 <String, String, Double>> out) throws Exception {
				String obj0 = value.getField(node0Idx).toString();
				String obj1 = value.getField(node1Idx).toString();
				double val = 1.0;
				if (valueIdx != -1) {
					Double weightVal = Double.valueOf(value.getField(valueIdx).toString());
					if (weightVal >= 0.0) {
						val = weightVal;
					} else {
						throw new RuntimeException("Walk algorithm : Weight value can not be a negative value.");
					}
				}
				if (isToUnDigraph) {
					out.collect(Tuple3.of(obj0, obj1, val));
					out.collect(Tuple3.of(obj1, obj0, val));
				} else {
					out.collect(Tuple3.of(obj0, obj1, val));
				}
			}
		}).groupBy(0, 1).reduceGroup(new GroupReduceFunction <Tuple3 <String, String, Double>,
			Edge <String, Double>>() {

			private static final long serialVersionUID = -3248939387422387825L;

			@Override
			public void reduce(Iterable <Tuple3 <String, String, Double>> values,
							   Collector <Edge <String, Double>> out) throws Exception {
				Tuple3 <String, String, Double> ret = null;
				if (valueIdx == -1) {
					for (Tuple3 <String, String, Double> t3 : values) {
						out.collect(new Edge <>(t3.f0, t3.f1, 1.0));
						break;
					}
				} else {
					double val = 0.0;
					double cnt = 0;
					for (Tuple3 <String, String, Double> t3 : values) {
						ret = t3;
						val += t3.f2;
						cnt++;
					}
					ret.f2 = val / cnt;
					out.collect(new Edge <>(ret.f0, ret.f1, ret.f2));
				}
			}
		});
	}
}
