package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.HugeIndexerStringPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.HugeStringIndexerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.StringIndexerTrainBatchOp;
import com.alibaba.alink.params.graph.CommonNeighborsTrainParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

@InputPorts(values = {
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES)
})
@OutputPorts(values = @PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT))
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)
@NameCn("共同邻居计算")
public class CommonNeighborsBatchOp extends BatchOperator <CommonNeighborsBatchOp>
	implements CommonNeighborsTrainParams <CommonNeighborsBatchOp> {
	private static final long serialVersionUID = -9221019571132151284L;
	private static final String COMMON_NEIGHBOR_ID_COL = "alink_common_neighbors_col";
	private static final String ADAMIC_OUTPUT_COL = "adamic_score";
	private static final String JACCARDS_OUTPUT_COL = "jaccards_score";
	private static final String NEIGHBORS_OUTPUT_COL = "neighbors";
	private static final String CN_OUTPUT_COL = "cn_count";

	public CommonNeighborsBatchOp(Params params) {
		super(params);
	}

	public CommonNeighborsBatchOp() {
		this(new Params());
	}

	@Override
	public CommonNeighborsBatchOp linkFrom(BatchOperator<?>... inputs) {
		checkOpSize(1, inputs);
		String sourceCol = getEdgeSourceCol();
		String targetCol = getEdgeTargetCol();
		String[] selectedCols = new String[]{sourceCol, targetCol};

		Long mlId = getMLEnvironmentId();
		boolean needTransformID = getNeedTransformID();
		BatchOperator<?> in = inputs[0].select(selectedCols);
		StringIndexerTrainBatchOp itemIndexModel = new StringIndexerTrainBatchOp();
		if (needTransformID) {
			itemIndexModel.setSelectedCol(sourceCol)
				.setSelectedCols(targetCol)
				.setStringOrderType("random")
				.linkFrom(in);

			in = new HugeStringIndexerPredictBatchOp()
				.setSelectedCols(selectedCols)
				.linkFrom(itemIndexModel, in);
		}

		DataSet<Tuple2<Long, Long>> inputData = in.getDataSet().mapPartition(new LongTupleData());
		if (!getIsBipartiteGraph()) {
			inputData = inputData.mapPartition(new AddReverseEdge());
		}

		DataSet<Tuple2<Long, Double>> itemWeight = inputData.groupBy(1)
			.reduceGroup(new GroupReduceFunction <Tuple2<Long, Long>, Tuple2<Long, Double>>() {
				@Override
				public void reduce(Iterable <Tuple2 <Long, Long>> iterable,
								   Collector <Tuple2 <Long, Double>> collector)
					throws Exception {
					int num = 0;
					Long mainItem = null;
					for (Tuple2 <Long, Long> t : iterable) {
						num += 1;
						mainItem = t.f1;
					}
					Double adamic = 0.0;
					if (num > 1) {
						adamic = 1.0 / Math.log(num);
					}
					collector.collect(Tuple2.of(mainItem, adamic));
				}
			}).name("compute_adamic_weight");

		DataSet<Tuple3<Long, Long, Long[]>> neighborsData = inputData
			.groupBy(0).reduceGroup(new BuildNeighborData()).name("build_neighbor_data");

		DataSet<Tuple4<Long, Long, Long[], Double>> neighborsResult = neighborsData
			.groupBy(0).reduceGroup(new ComputeCommonNeighbors()).name("compute_neighbor_data");

		DataSet<Tuple4 <Long, Long, Long[], Double>> filterResult = neighborsResult
			.groupBy(0, 1)
			.reduceGroup(new GroupReduceFunction <Tuple4<Long, Long, Long[], Double>, Tuple4 <Long, Long, Long[], Double>>() {
				@Override
				public void reduce(Iterable <Tuple4 <Long, Long, Long[], Double>> iterable,
								   Collector <Tuple4 <Long, Long, Long[], Double>> collector) throws Exception {
					collector.collect(iterable.iterator().next());
				}
			})
			.name("filter_multi_data");

		DataSet<Tuple3 <Long, Long, Double>> adamicResult = filterResult.flatMap(
			new FlatMapFunction <Tuple4 <Long, Long, Long[], Double>, Tuple3 <Long, Long, Long>>() {
				@Override
				public void flatMap(Tuple4 <Long, Long, Long[], Double> t,
									Collector <Tuple3 <Long, Long, Long>> collector) throws Exception {
					Long[] neighbors = t.f2;
					for (Long neighbor : neighbors) {
						collector.collect(Tuple3.of(t.f0, t.f1, neighbor));
					}
				}
			})
			.leftOuterJoin(itemWeight).where(2).equalTo(0)
			.with(new JoinFunction <Tuple3 <Long, Long, Long>, Tuple2 <Long, Double>, Tuple3 <Long, Long, Double>>() {
				@Override
				public Tuple3 <Long, Long, Double> join(Tuple3 <Long, Long, Long> left,
														Tuple2 <Long, Double> right) throws Exception {
					return Tuple3.of(left.f0, left.f1, right.f1);
				}
			})
			.groupBy(0, 1)
			.reduceGroup(new GroupReduceFunction <Tuple3 <Long, Long, Double>, Tuple3 <Long, Long, Double>>() {
				@Override
				public void reduce(Iterable <Tuple3 <Long, Long, Double>> iterable,
								   Collector <Tuple3 <Long, Long, Double>> collector) throws Exception {
					Long f0 = null;
					Long f1 = null;
					Double sum = 0.0;
					for (Tuple3 <Long, Long, Double> tuple3 : iterable) {
						f0 = tuple3.f0;
						f1 = tuple3.f1;
						sum += tuple3.f2;
					}
					collector.collect(Tuple3.of(f0, f1, sum));
				}
			}).name("merge_adamic_adar_score");

		DataSet<Tuple5 <Long, Long, Long[], Double, Double>> mergeAdamicResult = filterResult.join(adamicResult).where(0, 1).equalTo(0, 1)
			.projectFirst(0, 1, 2, 3).projectSecond(2);
		DataSet<Row> result = mergeAdamicResult.map(
			new MapFunction <Tuple5 <Long, Long, Long[], Double, Double>, Row>() {
				@Override
				public Row map(Tuple5 <Long, Long, Long[], Double, Double> tuple5)
					throws Exception {
					Row row = new Row(7);
					row.setField(0, tuple5.f0);
					row.setField(1, tuple5.f1);
					row.setField(2, tuple5.f2);
					row.setField(3, Long.valueOf(tuple5.f2.length));
					Long[] neighbors = tuple5.f2;
					StringBuffer sb = new StringBuffer();
					for (Long neighbor : neighbors) {
						sb.append(",").append(neighbor);
					}
					row.setField(4, sb.toString().substring(1));
					row.setField(5, tuple5.f3);
					row.setField(6, tuple5.f4);
					return row;
				}
			}).name("refine_final_result");

		String[] outputCols = new String[]{sourceCol, targetCol, NEIGHBORS_OUTPUT_COL, CN_OUTPUT_COL, JACCARDS_OUTPUT_COL, ADAMIC_OUTPUT_COL};
		TypeInformation[] outputTypes = new TypeInformation[]{Types.LONG, Types.LONG, Types.STRING, Types.LONG, Types.DOUBLE, Types.DOUBLE};

		String[] tableCols = new String[]{sourceCol, targetCol, NEIGHBORS_OUTPUT_COL, CN_OUTPUT_COL, COMMON_NEIGHBOR_ID_COL, JACCARDS_OUTPUT_COL, ADAMIC_OUTPUT_COL};
		TypeInformation[] tableTypes = new TypeInformation[]{Types.LONG, Types.LONG, Types.OBJECT_ARRAY(Types.LONG), Types.LONG, Types.STRING, Types.DOUBLE, Types.DOUBLE};
		Table resultTable = DataSetConversionUtil.toTable(mlId, result, tableCols, tableTypes);
		BatchOperator<?> resultOp = BatchOperator.fromTable(resultTable);
		if (needTransformID) {
			HugeIndexerStringPredictBatchOp indexerStringPredict = new HugeIndexerStringPredictBatchOp()
				.setSelectedCols(sourceCol, targetCol, NEIGHBORS_OUTPUT_COL)
				.linkFrom(itemIndexModel, resultOp);

			outputTypes = new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.DOUBLE, Types.DOUBLE};
			this.setOutput(indexerStringPredict.select(outputCols).getDataSet(), outputCols, outputTypes);
		} else {
			this.setOutput(
				resultOp.select(new String[]{sourceCol, targetCol, COMMON_NEIGHBOR_ID_COL, CN_OUTPUT_COL, JACCARDS_OUTPUT_COL, ADAMIC_OUTPUT_COL}).getDataSet(),
				outputCols,
				outputTypes);
		}
		return this;
	}

	/**
	 * add reverse edges
	 */
	private static class AddReverseEdge implements MapPartitionFunction <Tuple2<Long, Long>, Tuple2<Long, Long>> {
		private static final long serialVersionUID = 8536124777027322457L;

		AddReverseEdge() { }

		@Override
		public void mapPartition(Iterable <Tuple2<Long, Long>> iterable, Collector <Tuple2<Long, Long>> collector)
					throws Exception {
			iterable.forEach(tuple -> {
				collector.collect(tuple);
				collector.collect(Tuple2.of(tuple.f1, tuple.f0));
			});
		}
	}

	/**
	 * change row to comparable
	 */
	private static class LongTupleData implements MapPartitionFunction <Row, Tuple2<Long, Long>> {
		private static final long serialVersionUID = -2659982156687136255L;

		LongTupleData() { }
		@Override
		public void mapPartition(Iterable <Row> iterable, Collector <Tuple2<Long, Long>> collector)
			throws Exception {
			iterable.forEach(row -> {
				collector.collect(Tuple2.of((Long) row.getField(0), (Long) row.getField(1)));
			});
		}
	}

	/**
	 * group by id.
	 */
	private static class BuildNeighborData
		implements GroupReduceFunction <Tuple2<Long, Long>, Tuple3<Long, Long, Long[]>> {
		private static final long serialVersionUID = -987657040866491039L;

		BuildNeighborData() { }

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values,
						   Collector<Tuple3<Long, Long, Long[]>> out) throws Exception {
			ArrayList<Long> neighborsList = new ArrayList <>();
			Long mainId = null;
			for (Tuple2 <Long, Long> value : values) {
				mainId = value.f0;
				neighborsList.add(value.f1);
			}
			Long[] neighbors = new Long[neighborsList.size()];
			int index = 0;
			for (Long aLong : neighborsList) {
				neighbors[index++] = aLong;
			}
			for (Long neighbor : neighbors) {
				out.collect(Tuple3.of(neighbor, mainId, neighbors));
			}
		}
	}

	/**
	 * compute common neighbors.
	 */
	private static class ComputeCommonNeighbors
		implements GroupReduceFunction <Tuple3<Long, Long, Long[]>, Tuple4<Long, Long, Long[], Double>> {
		private static final long serialVersionUID = -4665094384893739366L;

		ComputeCommonNeighbors() {}

		@Override
		public void reduce(Iterable <Tuple3 <Long, Long, Long[]>> iterable,
						   Collector <Tuple4<Long, Long, Long[], Double>> collector) throws Exception {
			ArrayList<Long> nodes = new ArrayList <>();
			ArrayList<HashSet <Long>> neighborsSet = new ArrayList <>();
			for (Tuple3 <Long, Long, Long[]> value : iterable) {
				nodes.add(value.f1);
				HashSet<Long> set = new HashSet <>();
				set.addAll(Arrays.asList(value.f2));
				neighborsSet.add(set);
			}
			for (int i = 0; i < neighborsSet.size(); i++) {
				for (int j = i + 1; j < neighborsSet.size(); j++) {
					HashSet <Long> iSet = (HashSet <Long>) neighborsSet.get(i).clone();
					iSet.retainAll(neighborsSet.get(j));
					Long[] commonNeighbors = new Long[iSet.size()];
					int index = 0;
					for (Long aLong : iSet) {
						commonNeighbors[index++] = aLong;
					}
					Double jaccard = commonNeighbors.length * 1.0 / (neighborsSet.get(i).size() + neighborsSet.get(j).size() - commonNeighbors.length);
					collector.collect(Tuple4.of(nodes.get(i), nodes.get(j), commonNeighbors, jaccard));
					collector.collect(Tuple4.of(nodes.get(j), nodes.get(i), commonNeighbors, jaccard));
				}
			}
		}
	}

}