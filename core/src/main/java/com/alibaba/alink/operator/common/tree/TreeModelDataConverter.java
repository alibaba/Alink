package com.alibaba.alink.operator.common.tree;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkIllegalModelException;
import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.classification.GbdtTrainParams;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TreeModelDataConverter
	extends LabeledModelDataConverter <TreeModelDataConverter, TreeModelDataConverter> implements Serializable {
	public final static ParamInfo <Partition> STRING_INDEXER_MODEL_PARTITION = ParamInfoFactory
		.createParamInfo("stringIndexerModelPartition", Partition.class)
		.setDescription("stringIndexerModelPartition")
		.setRequired()
		.build();
	public final static ParamInfo <Partitions> TREE_PARTITIONS = ParamInfoFactory
		.createParamInfo("treePartition", Partitions.class)
		.setDescription("treePartition")
		.setRequired()
		.build();

	public final static ParamInfo <String> IMPORTANCE_FIRST_COL = ParamInfoFactory
		.createParamInfo("importanceFirstCol", String.class)
		.setHasDefaultValue("feature")
		.build();
	public final static ParamInfo <String> IMPORTANCE_SECOND_COL = ParamInfoFactory
		.createParamInfo("importanceSecondCol", String.class)
		.setHasDefaultValue("importance")
		.build();
	private static final long serialVersionUID = 6997356679076377663L;

	public List <Row> stringIndexerModelSerialized;
	public Node[] roots;
	public Params meta;
	public Object[] labels;

	public TreeModelDataConverter() {
		this(null);
	}

	public TreeModelDataConverter(TypeInformation labelType) {
		super(labelType);
	}

	@Override
	protected Tuple3 <Params, Iterable <String>, Iterable <Object>> serializeModel(TreeModelDataConverter modelData) {
		List <String> serialized = new ArrayList <>();

		int pStart = 0, pEnd = 0;

		// toString partition of stringIndexerModel
		if (modelData.stringIndexerModelSerialized != null) {
			for (Row row : stringIndexerModelSerialized) {
				Object[] objs = new Object[row.getArity()];

				for (int i = 0; i < row.getArity(); ++i) {
					objs[i] = row.getField(i);
				}

				serialized.add(JsonConverter.toJson(objs));
			}

			pEnd = serialized.size();
		}

		modelData.meta.set(STRING_INDEXER_MODEL_PARTITION, Partition.of(pStart, pEnd));

		// toString partition of trees
		Partitions treesPartition = new Partitions();

		for (Node root : modelData.roots) {
			List <String> localSerialize = serializeTree(root);

			pStart = pEnd;
			pEnd = pStart + localSerialize.size();

			treesPartition.add(Partition.of(pStart, pEnd));

			serialized.addAll(localSerialize);
		}

		modelData.meta.set(TREE_PARTITIONS, treesPartition);

		return Tuple3.of(
			modelData.meta,
			serialized,
			modelData.labels == null ? null : Arrays.asList(modelData.labels)
		);
	}

	public static List <Row> saveModelWithData(
		List <Node> roots,
		Params params,
		List <Row> stringIndexerModel,
		Object[] labels) {
		TreeModelDataConverter model = new TreeModelDataConverter(
			FlinkTypeConverter.getFlinkType(params.get(ModelParamName.LABEL_TYPE_NAME))
		);

		model.meta = params;
		model.roots = roots.toArray(new Node[0]);
		model.stringIndexerModelSerialized = stringIndexerModel;
		model.labels = labels;

		RowCollector collector = new RowCollector();
		model.save(model, collector);
		return collector.getRows();
	}

	public static List <String> serializeTree(Node node) {
		List <String> serialized = new ArrayList <>();

		int id = 0;

		Deque <NodeSerializable> nodeQ = new ArrayDeque <>();

		nodeQ.addFirst(new NodeSerializable()
			.setNode(node)
			.setId(id++)
		);

		while (!nodeQ.isEmpty()) {
			NodeSerializable nodeSerializable = nodeQ.pollLast();

			if (!(nodeSerializable.node.isLeaf())) {
				int len = nodeSerializable.node.getNextNodes().length;

				int[] nextIds = new int[len];

				for (int j = 0; j < nodeSerializable.node.getNextNodes().length; ++j) {
					nextIds[j] = id;
					nodeQ.addFirst(
						new NodeSerializable()
							.setNode(nodeSerializable.node.getNextNodes()[j])
							.setId(id++)
					);
				}

				nodeSerializable.setNextIds(nextIds);
			}

			serialized.add(JsonConverter.toJson(nodeSerializable));
		}

		return serialized;
	}

	public static Node deserializeTree(List <String> serialized) {
		int size = serialized.size();

		NodeSerializable[] serializables = new NodeSerializable[size];

		int validSize = 0;
		for (String nodeStr : serialized) {

			NodeSerializable node = JsonConverter.fromJson(nodeStr, NodeSerializable.class);

			if (node.id < 0
				|| node.id >= size) {
				throw new AkIllegalModelException("Model is broken. node index: " + node.id);
			}

			serializables[node.id] = node;
			++validSize;
		}

		for (int i = 0; i < validSize; ++i) {
			if (serializables[i] == null) {
				throw new AkIllegalModelException("Model is broken. index: " + i);
			}

			int[] nextIDs = serializables[i].nextIds;

			if (nextIDs == null) {
				continue;
			}

			int curLen = nextIDs.length;
			Node[] nexts = new Node[curLen];

			for (int j = 0; j < curLen; ++j) {
				nexts[j] = serializables[nextIDs[j]].node;
			}

			serializables[i].node.setNextNodes(nexts);
		}

		if (size != 0) {
			return serializables[0].node;
		} else {
			return null;
		}
	}

	@Override
	protected TreeModelDataConverter deserializeModel(Params meta, Iterable <String> iterable,
													  Iterable <Object> distinctLabels) {
		// parseDense partition of categorical
		Partition stringIndexerModelPartition = meta.get(
			STRING_INDEXER_MODEL_PARTITION
		);

		List <String> data = new ArrayList <>();
		iterable.forEach(data::add);
		if (stringIndexerModelPartition.getF1() != stringIndexerModelPartition.getF0()) {
			stringIndexerModelSerialized = new ArrayList <>();

			for (int i = stringIndexerModelPartition.getF0(); i < stringIndexerModelPartition.getF1(); ++i) {
				Object[] deserialized = JsonConverter.fromJson(data.get(i), Object[].class);
				stringIndexerModelSerialized.add(
					Row.of(
						((Integer) deserialized[0]).longValue(),
						deserialized[1],
						deserialized[2]
					)
				);
			}
		} else {
			stringIndexerModelSerialized = null;
		}

		// toString partition of trees
		Partitions treesPartition = meta.get(
			TREE_PARTITIONS
		);

		roots = treesPartition.getPartitions().stream()
			.map(x -> deserializeTree(data.subList(x.getF0(), x.getF1())))
			.toArray(Node[]::new);

		this.meta = meta;
		List <Object> labelList = new ArrayList <>();
		distinctLabels.forEach(labelList::add);
		this.labels = labelList.toArray();

		return this;
	}

	private static class NodeSerializable implements Serializable {
		private static final long serialVersionUID = 3621266425332831183L;
		public Node node;
		public int id;
		public int[] nextIds;

		public NodeSerializable setNode(Node node) {
			this.node = node;
			return this;
		}

		public NodeSerializable setId(int id) {
			this.id = id;
			return this;
		}

		public NodeSerializable setNextIds(int[] nextIds) {
			this.nextIds = nextIds;
			return this;
		}
	}

	public static final class Partition implements Serializable {
		private static final long serialVersionUID = -646027497988196810L;
		private int f0;
		private int f1;

		public Partition() {
		}

		public static Partition of(int f0, int f1) {
			return new Partition()
				.setF0(f0)
				.setF1(f1);
		}

		public int getF0() {
			return f0;
		}

		public Partition setF0(int f0) {
			this.f0 = f0;
			return this;
		}

		public int getF1() {
			return f1;
		}

		public Partition setF1(int f1) {
			this.f1 = f1;
			return this;
		}
	}

	public static final class Partitions implements Serializable {
		private static final long serialVersionUID = -6525488145026282786L;
		private List <Partition> partitions = new ArrayList <>();

		public Partitions() {
		}

		public List <Partition> getPartitions() {
			return partitions;
		}

		public Partitions setPartitions(List <Partition> partitions) {
			this.partitions = partitions;
			return this;
		}

		public Partitions add(Partition partition) {
			partitions.add(partition);
			return this;
		}
	}

	//feature importance
	public static class FeatureImportanceReducer extends RichGroupReduceFunction <Row, Row> {
		private static final long serialVersionUID = -4934720404391098100L;

		@Override
		public void reduce(Iterable <Row> input, Collector <Row> output) throws Exception {
			ArrayList <Row> modelData = new ArrayList <>();
			for (Row row : input) {
				modelData.add(row);
			}

			TreeModelDataConverter modelDataConverter = new TreeModelDataConverter();
			modelDataConverter.load(modelData);

			Map <Integer, Double> featureImportanceMap = new HashMap <>();

			GbdtTrainParams.FeatureImportanceType featureImportanceType
				= modelDataConverter.meta.get(GbdtTrainParams.FEATURE_IMPORTANCE_TYPE);

			ArrayDeque <Node> deque = new ArrayDeque <>();

			double sum = 0.0;
			for (Node root : modelDataConverter.roots) {
				deque.push(root);

				while (!deque.isEmpty()) {
					Node node = deque.pop();

					if (node.isLeaf()) {
						continue;
					}

					switch (featureImportanceType) {
						case GAIN:
							featureImportanceMap.merge(node.getFeatureIndex(), node.getGain(), Double::sum);
							sum += node.getGain();
							break;
						case COVER:
							featureImportanceMap
								.merge(
									node.getFeatureIndex(),
									(double) node.getCounter().getNumInst(),
									Double::sum
								);
							break;
						case WEIGHT:
							featureImportanceMap.merge(node.getFeatureIndex(), 1.0, Double::sum);
							break;
						default:
							throw new IllegalArgumentException(
								"Could not find the feature importace type. type: " + featureImportanceType
							);
					}

					for (Node child : node.getNextNodes()) {
						deque.push(child);
					}
				}
			}

			// normalize
			if (sum > 0.0) {
				final double localSum = sum;
				for (Integer key : featureImportanceMap.keySet()) {
					featureImportanceMap.compute(key, (integer, aDouble) -> aDouble / localSum);
				}
			}

			if (modelDataConverter.meta != null
				&& modelDataConverter.meta.contains(GbdtTrainParams.FEATURE_COLS)) {

				String[] featureCols = modelDataConverter.meta.get(GbdtTrainParams.FEATURE_COLS);

				for (int i = 0; i < featureCols.length; ++i) {
					Row row = new Row(2);
					row.setField(0, featureCols[i]);
					row.setField(1, featureImportanceMap.getOrDefault(i, 0.0));
					output.collect(row);
				}
			} else {

				for (Map.Entry <Integer, Double> entry : featureImportanceMap.entrySet()) {
					Row row = new Row(2);
					row.setField(0, String.valueOf(entry.getKey()));
					row.setField(1, entry.getValue());
					output.collect(row);
				}
			}
		}
	}
}
