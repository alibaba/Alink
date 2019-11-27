package com.alibaba.alink.operator.common.tree;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;

public class TreeModelDataConverter
	extends LabeledModelDataConverter<TreeModelDataConverter, TreeModelDataConverter> implements Serializable {
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
	protected Tuple3<Params, Iterable<String>, Iterable<Object>> serializeModel(TreeModelDataConverter modelData) {
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
				throw new RuntimeException("Model is broken. node index: " + node.id);
			}

			serializables[node.id] = node;
			++validSize;
		}

		for (int i = 0; i < validSize; ++i) {
			if (serializables[i] == null) {
				throw new RuntimeException("Model is broken. index: " + i);
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
	protected TreeModelDataConverter deserializeModel(Params meta, Iterable<String> iterable, Iterable<Object> distinctLabels) {
		// parseDense partition of categorical
		Partition stringIndexerModelPartition = meta.get(
			STRING_INDEXER_MODEL_PARTITION
		);

		List<String> data = new ArrayList<>();
		iterable.forEach(data::add);
		if (stringIndexerModelPartition.getF1() != stringIndexerModelPartition.getF0()) {
			stringIndexerModelSerialized = new ArrayList<>();

			for (int i = stringIndexerModelPartition.getF0(); i < stringIndexerModelPartition.getF1(); ++i) {
				Object[] deserialized = JsonConverter.fromJson(data.get(i), Object[].class);
				stringIndexerModelSerialized.add(
					Row.of(
						((Integer)deserialized[0]).longValue(),
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
		List<Object> labelList = new ArrayList<>();
		distinctLabels.forEach(labelList::add);
		this.labels = labelList.toArray();

		return this;
	}

	private static class NodeSerializable implements Serializable {
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

		String[] featureColNames;

		public FeatureImportanceReducer(String[] featureColNames) {
			this.featureColNames = featureColNames;
		}

		@Override
		public void reduce(Iterable <Row> input, Collector <Row> output) throws Exception {
			HashMap <Integer, Integer> finalMap = new HashMap <>(1000);

			for (Row value : input) {
				if ((Long)value.getField(0) == 0L || value.getField(2) != null) {
					continue;
				}
				String nodeDetail = (String) value.getField(1);
				TreeModelDataConverter.NodeSerializable lc = JsonConverter.fromJson(
					nodeDetail, TreeModelDataConverter.NodeSerializable.class);
				if (lc != null && lc.node != null) {
					int featureId = lc.node.getFeatureIndex();
					if (featureId < 0) {
						continue;
					}
					if (!finalMap.containsKey(featureId)) {
						finalMap.put(featureId, 1);
					} else {
						finalMap.put(featureId, finalMap.get(featureId) + 1);
					}
					//System.out.println("lc " + );
				}
			}
			for (HashMap.Entry <Integer, Integer> index : finalMap.entrySet()) {
				//System.out.println("key "+index.getKey()+" value "+index.getValue());
				Row row = new Row(2);
				row.setField(0, featureColNames[index.getKey()]);
				row.setField(1, (long) index.getValue());
				output.collect(row);
				//System.out.println("row "+row);
			}
		}
	}
}
