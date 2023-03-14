package com.alibaba.alink.operator.common.similarity.dataConverter;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.operator.common.similarity.KDTree;
import com.alibaba.alink.operator.common.similarity.modeldata.KDTreeModelData;
import com.alibaba.alink.operator.batch.statistics.utils.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.similarity.VectorApproxNearestNeighborTrainParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class KDTreeModelDataConverter extends NearestNeighborDataConverter <KDTreeModelData> {
	private static final long serialVersionUID = 7886707008132061008L;
	private static int ROW_SIZE = 4;
	private static int TASKID_INDEX = 0;
	private static int DATA_ID_INDEX = 1;
	private static int DATA_IDNEX = 2;
	private static int ROOT_IDDEX = 3;

	private static ParamInfo <Integer> VECTOR_SIZE = ParamInfoFactory
		.createParamInfo("vectorSize", Integer.class)
		.setRequired()
		.build();

	public KDTreeModelDataConverter() {
		this.rowSize = ROW_SIZE;
	}

	@Override
	public TableSchema getModelDataSchema() {
		return new TableSchema(new String[] {"TASKID", "DATAID", "DATA", "ROOT"},
			new TypeInformation[] {Types.LONG, Types.LONG, Types.STRING, Types.STRING});
	}

	@Override
	public KDTreeModelData loadModelData(List <Row> list) {
		HashMap <Long, TreeMap <Integer, FastDistanceVectorData>> data = new HashMap <>();
		HashMap <Long, KDTree.TreeNode> root = new HashMap <>();
		for (Row row : list) {
			if (row.getField(TASKID_INDEX) != null) {
				long taskId = (long) row.getField(TASKID_INDEX);
				TreeMap <Integer, FastDistanceVectorData> vectorDataMap = data.get(taskId);
				if (null == vectorDataMap) {
					vectorDataMap = new TreeMap <>();
				}
				if (row.getField(DATA_IDNEX) != null) {
					vectorDataMap.put(((Number) row.getField(DATA_ID_INDEX)).intValue(),
						FastDistanceVectorData.fromString((String) row.getField(DATA_IDNEX)));
					data.put(taskId, vectorDataMap);
				} else if (row.getField(ROOT_IDDEX) != null) {
					KDTree.TreeNode node = JsonConverter.fromJson((String) row.getField(ROOT_IDDEX),
						new TypeReference <KDTree.TreeNode>() {}.getType());
					root.put(taskId, node);
				}
			}
		}
		List <KDTree> treeList = new ArrayList <>();
		int vectorSize = meta.get(VECTOR_SIZE);
		EuclideanDistance distance = new EuclideanDistance();
		for (Map.Entry <Long, TreeMap <Integer, FastDistanceVectorData>> entry : data.entrySet()) {
			long taskId = entry.getKey();
			KDTree.TreeNode node = root.get(taskId);
			FastDistanceVectorData[] vectorData = entry.getValue().values().toArray(new FastDistanceVectorData[0]);
			KDTree kdTree = new KDTree(vectorData, vectorSize, distance);
			kdTree.setRoot(node);
			treeList.add(kdTree);
		}
		return new KDTreeModelData(treeList);
	}

	@Override
	public DataSet <Row> buildIndex(BatchOperator in, Params params) {
		AkPreconditions.checkArgument(params.get(VectorApproxNearestNeighborTrainParams.METRIC)
				.equals(VectorApproxNearestNeighborTrainParams.Metric.EUCLIDEAN),
			"KDTree solver only supports Euclidean distance!");
		EuclideanDistance distance = new EuclideanDistance();
		Tuple2 <DataSet <Vector>, DataSet <BaseVectorSummary>> statistics =
			StatisticsHelper.summaryHelper(in, null, params.get(VectorApproxNearestNeighborTrainParams.SELECTED_COL));

		return in.getDataSet()
			.rebalance()
			.mapPartition(new RichMapPartitionFunction <Row, Row>() {
				private static final long serialVersionUID = 6654757741959479783L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
					BaseVectorSummary summary = (BaseVectorSummary) getRuntimeContext().getBroadcastVariable(
						"vectorSize").get(0);
					int vectorSize = summary.vectorSize();
					List <FastDistanceVectorData> list = new ArrayList <>();
					for (Row row : values) {
						FastDistanceVectorData vector = distance.prepareVectorData(row, 1, 0);
						list.add(vector);
						vectorSize = vector.getVector().size();
					}
					if (list.size() > 0) {
						FastDistanceVectorData[] vectorArray = list.toArray(new FastDistanceVectorData[0]);
						KDTree tree = new KDTree(vectorArray, vectorSize, distance);
						tree.buildTree();
						int taskId = getRuntimeContext().getIndexOfThisSubtask();
						Row row = new Row(ROW_SIZE);
						row.setField(TASKID_INDEX, (long) taskId);
						for (int i = 0; i < vectorArray.length; i++) {
							row.setField(DATA_ID_INDEX, (long) i);
							row.setField(DATA_IDNEX, vectorArray[i].toString());
							out.collect(row);
						}
						row.setField(DATA_ID_INDEX, null);
						row.setField(DATA_IDNEX, null);
						row.setField(ROOT_IDDEX, JsonConverter.toJson(tree.getRoot()));
						out.collect(row);
					}
				}
			}).withBroadcastSet(statistics.f1, "vectorSize")
			.mapPartition(new RichMapPartitionFunction <Row, Row>() {
				private static final long serialVersionUID = 6849403933586157611L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
					Params meta = null;
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						meta = params;
						BaseVectorSummary summary = (BaseVectorSummary) getRuntimeContext().getBroadcastVariable(
							"vectorSize").get(0);
						int vectorSize = summary.vectorSize();
						meta.set(VECTOR_SIZE, vectorSize);
					}
					new KDTreeModelDataConverter().save(Tuple2.of(meta, values), out);
				}
			}).withBroadcastSet(statistics.f1, "vectorSize");

	}
}
