package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborPredictBatchOp;
import com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborTrainBatchOp;
import com.alibaba.alink.operator.batch.source.DataSetWrapperBatchOp;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanConstant;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanModelDataConverter;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanModelInfoBatchOp;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanModelTrainData;
import com.alibaba.alink.operator.common.clustering.dbscan.LocalCluster;
import com.alibaba.alink.operator.common.clustering.dbscan.Type;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.clustering.DbscanParams;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND;

/**
 * @author guotao.gt
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT),
	@PortSpec(value = PortType.MODEL, desc = PortDesc.DBSCAN_MODEL),
})
@ReservedColsWithSecondInputSpec
@ParamSelectColumnSpec(name = "vectorCol", portIndices = 0, allowedTypeCollections = {TypeCollections.VECTOR_TYPES})
@ParamSelectColumnSpec(name = "idCol", portIndices = 0)
@NameCn("DBSCAN")
@NameEn("DBSCAN")
public final class DbscanBatchOp extends BatchOperator <DbscanBatchOp>
	implements DbscanParams <DbscanBatchOp>,
	WithModelInfoBatchOp <DbscanModelInfoBatchOp.DbscanModelInfo, DbscanBatchOp, DbscanModelInfoBatchOp> {

	private static final long serialVersionUID = 2680984884814078788L;

	public DbscanBatchOp() {
		super(new Params());
	}

	public DbscanBatchOp(Params params) {
		super(params);
	}

	@Override
	public DbscanBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		String idColName = this.getIdCol();
		String vectorColName = this.getVectorCol();

		DataSet <Tuple3 <Integer, Object, Vector>> idLabelVector = DataSetUtils
			.zipWithIndex(in.select(new String[] {idColName, vectorColName}).getDataSet())
			.map(new RichMapFunction <Tuple2 <Long, Row>, Tuple3 <Integer, Object, Vector>>() {
				private static final long serialVersionUID = -4516567863938069544L;

				@Override
				public Tuple3 <Integer, Object, Vector> map(Tuple2 <Long, Row> value) {
					return Tuple3.of(value.f0.intValue(), value.f1.getField(0),
						VectorUtil.getVector(value.f1.getField(1)));
				}
			});

		BatchOperator data = new DataSetWrapperBatchOp(
			idLabelVector.map(new MapFunction <Tuple3 <Integer, Object, Vector>, Row>() {
				private static final long serialVersionUID = 672726382584730805L;

				@Override
				public Row map(Tuple3 <Integer, Object, Vector> t) {
					return Row.of(t.f0, t.f2);
				}
			}), new String[] {"alink_unique_id", "vector"}, new TypeInformation[] {AlinkTypes.INT, AlinkTypes.VECTOR});

		VectorNearestNeighborTrainBatchOp train = new VectorNearestNeighborTrainBatchOp()
			.setIdCol("alink_unique_id")
			.setSelectedCol("vector")
			.setMetric(this.getDistanceType().name())
			.linkFrom(data);

		VectorNearestNeighborPredictBatchOp predict = new VectorNearestNeighborPredictBatchOp()
			.setSelectedCol("vector")
			.setRadius(this.getEpsilon())
			.setReservedCols("alink_unique_id")
			.linkFrom(train, data);

		DataSet <Tuple3 <Integer, Type, int[]>> dataSet = predict
			.select(new String[] {"alink_unique_id", "vector"})
			.getDataSet()
			.mapPartition(new GetCorePoints(this.getMinPoints()));

		DataSet <Tuple2 <Integer, LocalCluster>> taskIdLabel = dataSet
			//.rebalance()
			.mapPartition(new ReduceLocal());

		IterativeDataSet <Tuple2 <Integer, LocalCluster>> loop = taskIdLabel.iterate(Integer.MAX_VALUE);

		//得到全局的label
		DataSet <Tuple2 <Integer, Integer>> globalMaxLabel = loop.flatMap(
			new FlatMapFunction <Tuple2 <Integer, LocalCluster>, Tuple2 <Integer, Integer>>() {
				private static final long serialVersionUID = -4049782728006537532L;

				@Override
				public void flatMap(Tuple2 <Integer, LocalCluster> value, Collector <Tuple2 <Integer, Integer>> out) {
					int[] keys = value.f1.getKeys();
					int[] clusterIds = value.f1.getClusterIds();
					for (int i = 0; i < keys.length; i++) {
						out.collect(Tuple2.of(keys[i], clusterIds[i]));
					}
				}
			}).groupBy(0)
			.aggregate(Aggregations.MAX, 1);

		DataSet <Tuple3 <Integer, LocalCluster, Boolean>> update = taskIdLabel
			.mapPartition(new LocalMerge())
			.withBroadcastSet(globalMaxLabel, "global");

		DataSet <Tuple2 <Integer, LocalCluster>> feedBack = update.project(0, 1);

		DataSet <Tuple3 <Integer, LocalCluster, Boolean>> filter = update.filter(
			new FilterFunction <Tuple3 <Integer, LocalCluster, Boolean>>() {
				private static final long serialVersionUID = -1489238776369734510L;

				@Override
				public boolean filter(Tuple3 <Integer, LocalCluster, Boolean> value) {
					return !value.f2;
				}
			});

		DataSet <Tuple2 <Integer, LocalCluster>> idNeighborFinalLabel = loop.closeWith(feedBack, filter);

		DataSet <Tuple2 <Integer, Integer>> idClusterId = idNeighborFinalLabel
			.mapPartition(new AssignContinuousClusterId());

		DataSet <Tuple3 <Integer, Type, Integer>> idTypeClusterId = dataSet
			.leftOuterJoin(idClusterId, BROADCAST_HASH_SECOND)
			.where(0)
			.equalTo(0)
			.with(new AssignAllClusterId());

		DataSet <Row> out = idLabelVector
			.join(idTypeClusterId)
			.where(0)
			.equalTo(0)
			.with(new JoinFunction <Tuple3 <Integer, Object, Vector>, Tuple3 <Integer, Type, Integer>, Row>() {
				private static final long serialVersionUID = 7638483527530324994L;

				@Override
				public Row join(Tuple3 <Integer, Object, Vector> first, Tuple3 <Integer, Type, Integer> second) {
					return Row.of(first.f1, second.f1.name(), (long) second.f2);
				}
			});

		DataSet <Tuple2 <Integer, Long>> idLongClusterId = idTypeClusterId.flatMap(
			new FlatMapFunction <Tuple3 <Integer, Type, Integer>, Tuple2 <Integer, Long>>() {
				private static final long serialVersionUID = -4449631564554949600L;

				@Override
				public void flatMap(Tuple3 <Integer, Type, Integer> value, Collector <Tuple2 <Integer, Long>> out) {
					if (value.f1.equals(Type.CORE)) {
						out.collect(Tuple2.of(value.f0, (long) value.f2));
					}
				}
			});

		DataSet <Tuple2 <Vector, Long>> coreVectorClusterId = idLongClusterId.join(idLabelVector)
			.where(0)
			.equalTo(0)
			.with(new JoinFunction <Tuple2 <Integer, Long>, Tuple3 <Integer, Object, Vector>, Tuple2 <Vector, Long>>
				() {
				private static final long serialVersionUID = -1388744253754875541L;

				@Override
				public Tuple2 <Vector, Long> join(Tuple2 <Integer, Long> first,
												  Tuple3 <Integer, Object, Vector> second) {
					return Tuple2.of(second.f2, first.f1);
				}
			});

		DataSet <Row> modelRows = coreVectorClusterId.mapPartition(
			new SaveModel(vectorColName, getEpsilon(), getDistanceType())).setParallelism(1);

		TypeInformation type = in.getColTypes()[TableUtil.findColIndexWithAssertAndHint(in.getColNames(), idColName)];
		this.setOutput(out, new String[] {idColName, DbscanConstant.TYPE, this.getPredictionCol()},
			new TypeInformation[] {
				type, AlinkTypes.STRING, AlinkTypes.LONG});

		this.setSideOutputTables(new Table[] {
			DataSetConversionUtil.toTable(getMLEnvironmentId(), modelRows,
				new DbscanModelDataConverter().getModelSchema())});
		return this;
	}

	static class SaveModel implements MapPartitionFunction <Tuple2 <Vector, Long>, Row> {
		private static final long serialVersionUID = 7638276873515252678L;
		private String vectorColName;
		private double epsilon;
		private DistanceType distanceType;

		public SaveModel(String vectorColName, double epsilon, DistanceType distanceType) {
			this.vectorColName = vectorColName;
			this.epsilon = epsilon;
			this.distanceType = distanceType;
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Vector, Long>> values, Collector <Row> out) throws Exception {
			DbscanModelTrainData modelData = new DbscanModelTrainData();
			modelData.coreObjects = values;
			modelData.vectorColName = vectorColName;
			modelData.epsilon = epsilon;
			modelData.distanceType = distanceType;

			new DbscanModelDataConverter().save(modelData, out);
		}
	}

	static class AssignAllClusterId implements
		JoinFunction <Tuple3 <Integer, Type, int[]>, Tuple2 <Integer, Integer>, Tuple3 <Integer, Type, Integer>> {
		private static final long serialVersionUID = -3817364693537340163L;

		@Override
		public Tuple3 <Integer, Type, Integer> join(Tuple3 <Integer, Type, int[]> first,
													Tuple2 <Integer, Integer> second) {
			if (null == second) {
				return Tuple3.of(first.f0, first.f1, Integer.MIN_VALUE);
			} else {
				if (first.f1.equals(Type.NOISE)) {
					first.f1 = Type.LINKED;
				}
				return Tuple3.of(first.f0, first.f1, second.f1);
			}
		}
	}

	public static class AssignContinuousClusterId
		extends RichMapPartitionFunction <Tuple2 <Integer, LocalCluster>, Tuple2 <Integer, Integer>> {
		private static final long serialVersionUID = -503944144706407009L;

		@Override
		public void mapPartition(Iterable <Tuple2 <Integer, LocalCluster>> values,
								 Collector <Tuple2 <Integer, Integer>> out) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				for (Tuple2 <Integer, LocalCluster> value : values) {
					int[] keys = value.f1.getKeys();
					int[] clusterIds = value.f1.getClusterIds();
					Map <Integer, Integer> hashMap = new HashMap <>();
					int cnt = 0;
					for (int i = 0; i < keys.length; i++) {
						Integer clusterId = hashMap.get(clusterIds[i]);
						if (null == clusterId) {
							clusterId = cnt++;
							hashMap.put(clusterIds[i], clusterId);
						}
						out.collect(Tuple2.of(keys[i], clusterId));
					}
					return;
				}
			}
		}
	}

	public static class GetCorePoints implements MapPartitionFunction <Row, Tuple3 <Integer, Type, int[]>> {
		private int minPoints;

		public GetCorePoints(int minPoints) {
			this.minPoints = minPoints;
		}

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Tuple3 <Integer, Type, int[]>> collector) {
			for (Row t : values) {
				Integer id = (int) t.getField(0);
				Tuple2 <List <Object>, List <Object>> tuple = NearestNeighborsMapper.extractKObject(
					(String) t.getField(1), Integer.class);
				if (null != tuple.f0 && tuple.f0.size() >= minPoints) {
					int[] keys = new int[tuple.f0.size()];
					for (int i = 0; i < tuple.f0.size(); i++) {
						keys[i] = (int) tuple.f0.get(i);
					}
					Arrays.sort(keys);
					collector.collect(Tuple3.of(id, Type.CORE, keys));
				} else {
					collector.collect(Tuple3.of(id, Type.NOISE, new int[0]));
				}
			}
		}
	}

	public static class ReduceLocal
		extends RichMapPartitionFunction <Tuple3 <Integer, Type, int[]>, Tuple2 <Integer, LocalCluster>> {

		private static final long serialVersionUID = -6516417460468964305L;

		@Override
		public void mapPartition(Iterable <Tuple3 <Integer, Type, int[]>> tuples,
								 Collector <Tuple2 <Integer, LocalCluster>> collector) {
			TreeMap <Integer, Integer> treeMap = new TreeMap();
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			for (Tuple3 <Integer, Type, int[]> t : tuples) {
				Preconditions.checkArgument(t.f1.equals(Type.NOISE) ^ t.f2.length > 0, "Noise must be empty!");
				if (t.f2.length > 0) {
					updateTreeMap(treeMap, t.f2);
				}
			}

			collector.collect(Tuple2.of(taskId, treeMapToLocalCluster(treeMap)));
		}
	}

	public static void updateTreeMap(TreeMap <Integer, Integer> map, int[] keys) {
		int max = keys[keys.length - 1];
		for (int key : keys) {
			max = Math.max(map.getOrDefault(key, max), max);
		}
		for (int key : keys) {
			Integer value = map.get(key);
			if (null == value) {
				map.put(key, max);
			} else {
				if (max > value) {
					map.put(map.get(value), max);
				}
			}
		}
		DbscanBatchOp.reduceTreeMap(map);
	}

	public static boolean updateTreeMap(TreeMap <Integer, Integer> map, LocalCluster sparseVector) {
		int[] keys = sparseVector.getKeys();
		int[] clusterIds = sparseVector.getClusterIds();
		boolean isFinished = true;

		for (int i = 0; i < keys.length; i++) {
			int parent = clusterIds[i];
			if (map.get(keys[i]) > map.get(parent)) {
				isFinished = false;
				map.put(parent, map.get(keys[i]));
			}
		}
		reduceTreeMap(map);
		return isFinished;
	}

	public static void reduceTreeMap(TreeMap <Integer, Integer> map) {
		for (int key : map.descendingKeySet()) {
			int parent = map.get(key);
			int parentClusterId = map.get(parent);
			if (parentClusterId > parent) {
				map.put(key, parentClusterId);
			}
		}
	}

	public static LocalCluster treeMapToLocalCluster(TreeMap <Integer, Integer> treeMap) {
		int[] clusterIds = new int[treeMap.size()];
		int[] keys = new int[treeMap.size()];
		int cnt = 0;
		for (Map.Entry <Integer, Integer> entry : treeMap.entrySet()) {
			keys[cnt] = entry.getKey();
			clusterIds[cnt++] = entry.getValue();
		}
		return new LocalCluster(keys, clusterIds);
	}

	public static class LocalMerge
		extends RichMapPartitionFunction <Tuple2 <Integer, LocalCluster>, Tuple3 <Integer, LocalCluster, Boolean>> {
		private static final long serialVersionUID = -7265351591038567537L;
		private TreeMap <Integer, Integer> global;

		@Override
		public void open(Configuration parameters) {
			List <Tuple2 <Integer, Integer>> list = getRuntimeContext().getBroadcastVariable("global");
			global = new TreeMap <>();
			for (Tuple2 <Integer, Integer> t : list) {
				global.put(t.f0, t.f1);
			}
			reduceTreeMap(global);
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Integer, LocalCluster>> tuples,
								 Collector <Tuple3 <Integer, LocalCluster, Boolean>> collector) {
			boolean isFinished = true;
			Integer taskId = null;
			for (Tuple2 <Integer, LocalCluster> t : tuples) {
				if (null == taskId) {
					taskId = t.f0;
				}
				isFinished = updateTreeMap(global, t.f1);
			}

			if (null != taskId) {
				collector.collect(Tuple3.of(taskId, treeMapToLocalCluster(global), isFinished));
			}
		}
	}

	@Override
	public DbscanModelInfoBatchOp getModelInfoBatchOp() {
		return new DbscanModelInfoBatchOp(this.getParams()).linkFrom(this);
	}
}
