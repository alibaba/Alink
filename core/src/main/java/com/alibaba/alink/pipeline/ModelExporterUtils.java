package com.alibaba.alink.pipeline;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.filesystem.AkStream;
import com.alibaba.alink.common.io.filesystem.AkStream.AkReader;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils.FileProcFunction;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.mapper.ComboMapper;
import com.alibaba.alink.common.mapper.ComboModelMapper;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.mapper.MapperChain;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.PipelineModelMapper;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.ModelStreamScanParams;
import com.alibaba.alink.params.io.ModelFileSinkParams;
import com.alibaba.alink.params.shared.HasModelFilePath;
import com.alibaba.alink.params.shared.HasOverwriteSink;
import com.alibaba.alink.pipeline.recommendation.BaseRecommender;
import com.alibaba.alink.pipeline.recommendation.RecommenderUtil;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A utility class for exporting {@link PipelineModel}.
 */
public class ModelExporterUtils {

	// meta col
	private static final TypeInformation <?>[] PREFIX_TYPES = new TypeInformation <?>[] {Types.STRING};
	private static final String MODEL_COL_PREFIX = "p";
	private static final String META_DELIMITER = "__ALINK_META_ROW_DELIMITER__";
	static final String ID_COL_NAME = "id";

	private interface Node {
		int[] getChildren();
	}

	private static <T extends Node> void preOrder(T[] stages, Consumer <Integer> consumer) {
		LinkedList <Integer> stack = new LinkedList <>();
		stack.push(0);

		while (!stack.isEmpty()) {
			int lp = stack.pop();

			// visit
			consumer.accept(lp);

			T node = stages[lp];

			if (node.getChildren() != null) {
				// left to right
				for (int i = node.getChildren().length - 1; i >= 0; --i) {
					stack.push(node.getChildren()[i]);
				}
			}
		}
	}

	private static <T extends Node> void postOrder(T[] stages, Consumer <Integer> consumer) {

		LinkedList <Integer> stack = new LinkedList <>();
		stack.push(0);
		int lastLp = -1;

		while (!stack.isEmpty()) {
			int lp = stack.peek();

			T node = stages[lp];

			if (node.getChildren() != null && lastLp != node.getChildren()[0]) {
				// right to left
				for (int i = 0; i < node.getChildren().length; ++i) {
					stack.push(node.getChildren()[i]);
				}
			} else {

				// visit
				consumer.accept(lp);

				lastLp = lp;
				stack.pop();
			}
		}
	}

	public static class StageNode implements Node {
		// valid information

		// the identifier of stage.
		public String identifier;

		// the params in the stage.
		public Params params;

		// the index of schema in the root node.
		public int[] schemaIndices;

		// the column names of stage. only on the leaf node.
		public String[] colNames;

		// the parent node's index.
		public int parent;

		// Temporary

		// stage in memory
		public transient PipelineStageBase <?> stage;

		// index in the parent's array of children
		public transient int inParentIndex;

		// index array of the child.
		public transient int[] children;

		// Temporary size of children.
		public transient int sizeChildren;

		// Temporary types for construct schema.
		public transient TypeInformation <?>[] types;

		public StageNode() {
		}

		public StageNode(PipelineStageBase <?> stage, int parent, int inParentIndex, int[] children) {
			this.stage = stage;
			this.parent = parent;
			this.inParentIndex = inParentIndex;
			this.children = children;

			if (stage != null) {
				identifier = stage.getClass().getName();
				params = stage.getParams();
			}
		}

		@Override
		public int[] getChildren() {
			return children;
		}
	}

	// serialize
	private static int findFirst(TypeInformation <?>[] all, int start, TypeInformation <?> t) {
		for (int i = start; i < all.length; ++i) {
			if (t.equals(all[i])) {
				return i;
			}
		}

		return -1;
	}

	public static Tuple2 <TypeInformation <?>[], int[]> mergeType(
		TypeInformation <?>[] a, TypeInformation <?>[] b) {

		if (a == null) {
			return Tuple2.of(b, initialSchemaIndices(b));
		}

		if (b == null) {
			return Tuple2.of(a, initialSchemaIndices(a));
		}

		List <TypeInformation <?>> merged = new ArrayList <>(Arrays.asList(a));

		Map <TypeInformation <?>, Integer> bCurrentIdx = new HashMap <>();

		int[] bIdx = new int[b.length];

		for (int i = 0; i < b.length; ++i) {
			Integer currentIdx = bCurrentIdx.getOrDefault(b[i], 0);

			if (currentIdx < 0) {
				bIdx[i] = merged.size();
				merged.add(b[i]);
				continue;
			}

			int found = findFirst(a, currentIdx, b[i]);

			if (found < 0) {
				bIdx[i] = merged.size();
				merged.add(b[i]);
				bCurrentIdx.put(b[i], found);
			} else {
				bIdx[i] = found;
				bCurrentIdx.put(b[i], found + 1);
			}
		}

		return Tuple2.of(merged.toArray(new TypeInformation <?>[0]), bIdx);
	}

	private static TypeInformation <?>[] getTypes(PipelineStageBase <?> stage) {
		if (stage instanceof PipelineModel) {
			throw new IllegalArgumentException(
				"Error pipeline stage. Could not get column types from pipeline model."
			);
		} else if (stage instanceof ModelBase) {
			ModelBase<?> model = (ModelBase <?>) stage;

			Params params = model.getParams();

			if (params.get(ModelFileSinkParams.MODEL_FILE_PATH) != null) {
				if (model.modelData != null) {

					model
						.modelData
						.link(
							new AkSinkBatchOp()
								.setFilePath(FilePath.deserialize(params.get(ModelFileSinkParams.MODEL_FILE_PATH)))
								.setMLEnvironmentId(model.getModelData().getMLEnvironmentId())
								.setOverwriteSink(params.get(HasOverwriteSink.OVERWRITE_SINK))
						);
				}

				return null;
			}

			return ((ModelBase <?>) stage).getModelData().getColTypes();
		} else if (stage instanceof Pipeline) {
			throw new IllegalArgumentException(
				"Error pipeline stage. Could not get column types from pipeline."
			);
		} else {
			return null;
		}
	}

	private static String[] getColNames(PipelineStageBase <?> stage) {
		if (stage instanceof PipelineModel) {
			throw new IllegalArgumentException(
				"Error pipeline stage. Could not get column names from pipeline model."
			);
		} else if (stage instanceof ModelBase) {
			return ((ModelBase <?>) stage).getModelData().getColNames();
		} else if (stage instanceof Pipeline) {
			throw new IllegalArgumentException(
				"Error pipeline stage. Could not get column names from pipeline."
			);
		} else {
			return null;
		}
	}

	public static int[] initialSchemaIndices(TypeInformation <?>[] types) {
		if (types == null) {
			return null;
		}

		int[] schemaIndices = new int[types.length];

		for (int i = 0; i < types.length; ++i) {
			schemaIndices[i] = i;
		}

		return schemaIndices;
	}

	private static String[] appendPrefix(String prefix, int len) {
		String[] result = new String[len];
		for (int i = 0; i < len; ++i) {
			result[i] = prefix + i;
		}

		return result;
	}

	private static StageNode[] postOrderCreateSchema(StageNode[] stages) {
		if (stages == null || stages.length == 0) {
			return stages;
		}

		postOrder(stages, lp -> {
			StageNode node = stages[lp];

			if (node.children != null) {
				StageNode firstNode = stages[node.children[0]];
				TypeInformation <?>[] first = firstNode.types;
				firstNode.schemaIndices = initialSchemaIndices(first);

				for (int i = 1; i < node.children.length; ++i) {
					StageNode currentNode = stages[node.children[i]];
					Tuple2 <TypeInformation <?>[], int[]> merged
						= mergeType(first, currentNode.types);

					if (currentNode.types != null) {
						currentNode.schemaIndices = merged.f1;
					}

					first = merged.f0;
				}

				node.types = first;
			} else {
				node.types = getTypes(node.stage);
				node.colNames = node.types == null ? null : getColNames(node.stage);
			}
		});

		stages[0].schemaIndices = initialSchemaIndices(stages[0].types);

		return stages;
	}

	private static TypeInformation <?>[] preOrderCorrectIndex(StageNode[] stages, TypeInformation <?>[] prefixTypes) {
		if (stages == null || stages.length == 0 || stages[0].schemaIndices == null) {
			return prefixTypes;
		}

		Tuple2 <TypeInformation <?>[], int[]> merged = mergeType(prefixTypes, stages[0].types);

		for (int i = 0; i < stages[0].schemaIndices.length; ++i) {
			stages[0].schemaIndices[i] = merged.f1[stages[0].schemaIndices[i]];
		}

		preOrder(stages, lp -> {
			StageNode node = stages[lp];

			if (node.parent >= 0 && node.schemaIndices != null) {
				for (int i = 0; i < node.schemaIndices.length; ++i) {
					node.schemaIndices[i]
						= stages[node.parent].schemaIndices[node.schemaIndices[i]];
				}
			}
		});

		return merged.f0;
	}

	private static StageNode[] preOrderConstructStages(List <PipelineStageBase <?>> stages) {
		if (stages == null || stages.isEmpty()) {
			return null;
		}

		int numStages = stages.size();

		List <StageNode> tree = new ArrayList <>(numStages);
		tree.add(new StageNode(null, -1, -1, new int[stages.size()]));

		LinkedList <StageNode> stack = new LinkedList <>();

		for (int i = numStages - 1; i >= 0; --i) {
			stack.push(new StageNode(stages.get(i), 0, i, null));
		}

		while (!stack.isEmpty()) {
			StageNode node = stack.pop();
			int index = tree.size();
			tree.get(node.parent).children[node.inParentIndex] = index;
			tree.add(node);

			if (node.stage instanceof PipelineModel) {
				PipelineModel pipelineModel = (PipelineModel) node.stage;
				node.children = new int[pipelineModel.transformers.length];

				for (int i = pipelineModel.transformers.length - 1; i >= 0; --i) {
					stack.push(new StageNode(pipelineModel.transformers[i], index, i, null));
				}
			} else if (node.stage instanceof Pipeline) {
				Pipeline pipeline = (Pipeline) node.stage;
				node.children = new int[pipeline.stages.size()];

				for (int i = pipeline.stages.size() - 1; i >= 0; --i) {
					stack.push(new StageNode(pipeline.stages.get(i), index, i, null));
				}
			}
		}

		return tree.toArray(new StageNode[0]);
	}

	private static Row serializeMeta(StageNode[] tree, int len, Params params) {
		Map <String, String> meta = new HashMap <>();
		meta.put("stages", JsonConverter.toJson(tree));

		Row metaRow = new Row(len);
		metaRow.setField(0, -1L);

		metaRow.setField(1, JsonConverter.toJson(meta) + META_DELIMITER + params.toJson());
		return metaRow;
	}

	private static BatchOperator <?> preOrderSerialize(
		StageNode[] stages, BatchOperator <?> packed, final TableSchema schema, final int offset) {

		if (stages == null || stages.length == 0) {
			return packed;
		}

		final int len = schema.getFieldTypes().length;
		final long[] id = new long[1];
		final BatchOperator <?>[] localPacked = new BatchOperator <?>[] {packed};

		Consumer <Integer> serializeModelData = lp -> {

			StageNode stageNode = stages[lp];

			if (stageNode.parent >= 0
				&& stageNode.schemaIndices != null
				&& stageNode.children == null
				&& stageNode.stage instanceof ModelBase <?>) {

				ModelBase <?> model = ((ModelBase <?>) stageNode.stage);

				final long localId = id[0];
				final int[] localSchemaIndices = stageNode.schemaIndices;

				DataSet <Row> modelData =
					model
						.getModelData()
						.getDataSet()
						.map(new MapFunction <Row, Row>() {
							private static final long serialVersionUID = 5218543921039328938L;

							@Override
							public Row map(Row value) {
								Row ret = new Row(len);
								ret.setField(0, localId);
								for (int i = 0; i < localSchemaIndices.length; ++i) {
									ret.setField(localSchemaIndices[i] + offset, value.getField(i));
								}
								return ret;
							}
						})
						.returns(new RowTypeInfo(schema.getFieldTypes()));

				localPacked[0] = new TableSourceBatchOp(
					DataSetConversionUtil.toTable(
						localPacked[0].getMLEnvironmentId(),
						localPacked[0].getDataSet().union(modelData),
						schema
					)
				).setMLEnvironmentId(localPacked[0].getMLEnvironmentId());
			}

			id[0]++;
		};

		preOrder(stages, serializeModelData);

		return localPacked[0];
	}

	static BatchOperator <?> serializePipelineStages(List <PipelineStageBase <?>> stages, Params params) {

		StageNode[] stageNodes = preOrderConstructStages(stages);

		TypeInformation <?>[] typesWithPrefix
			= preOrderCorrectIndex(postOrderCreateSchema(stageNodes), PREFIX_TYPES);

		TableSchema finalSchema = new TableSchema(
			ArrayUtils.addAll(new String[] {ID_COL_NAME}, appendPrefix(MODEL_COL_PREFIX, typesWithPrefix.length)),
			ArrayUtils.addAll(new TypeInformation[] {Types.LONG}, typesWithPrefix)
		);

		Row metaRow = serializeMeta(stageNodes, finalSchema.getFieldTypes().length, params);

		return preOrderSerialize(
			stageNodes,
			new MemSourceBatchOp(Collections.singletonList(metaRow), finalSchema)
				.setMLEnvironmentId(stages.size() > 0 ? stages.get(0).getMLEnvironmentId() :
					MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID),
			finalSchema,
			1);
	}

	// deserialize
	private static Tuple2 <StageNode[], Params> deserializeMeta(Row metaRow, TableSchema schema, final int offset) {
		String[] metaAndParams = ((String) metaRow.getField(1)).split(META_DELIMITER);
		Map <String, String> meta = JsonConverter.fromJson(metaAndParams[0]
			, new TypeReference <Map <String, String>>() {}.getType()
		);
		Params pipeParams = metaAndParams.length == 2 ? Params.fromJson(metaAndParams[1]) : new Params();
		StageNode[] stages = JsonConverter.fromJson(meta.get("stages"), StageNode[].class);

		if (stages == null || stages.length == 0) {
			return Tuple2.of(stages, pipeParams);
		}

		TypeInformation <?>[] types = schema.getFieldTypes();

		for (StageNode stage : stages) {
			if (stage.parent >= 0) {
				stages[stage.parent].sizeChildren++;
			}
		}

		for (int i = 0; i < stages.length; ++i) {
			if (stages[i].sizeChildren > 0) {
				stages[i].children = new int[stages[i].sizeChildren];
				stages[i].sizeChildren = 0;
			}

			if (stages[i].parent >= 0) {
				stages[stages[i].parent].children[stages[stages[i].parent].sizeChildren++] = i;
			}

			if (stages[i].schemaIndices != null) {
				int len = stages[i].schemaIndices.length;
				stages[i].types = new TypeInformation[len];
				for (int j = 0; j < len; ++j) {
					stages[i].types[j] = types[stages[i].schemaIndices[j] + offset];
				}
			}
		}

		return Tuple2.of(stages, pipeParams);
	}

	private static <T extends PipelineStageBase <?>> List <T> postOrderDeserialize(
		StageNode[] stages, BatchOperator <?> unpacked, final TableSchema schema, final int offset) {

		if (stages == null || stages.length == 0) {
			return new ArrayList <>();
		}

		final long[] id = new long[] {stages.length - 1};
		final BatchOperator <?>[] deserialized = new BatchOperator <?>[] {unpacked};

		List <T> result = new ArrayList <>();

		Consumer <Integer> deserializer = lp -> {
			StageNode stageNode = stages[lp];

			try {
				if (stageNode.identifier != null) {
					stageNode.stage = (PipelineStageBase <?>) Class
						.forName(stageNode.identifier)
						.getConstructor(Params.class)
						.newInstance(stageNode.params);
				}
			} catch (ClassNotFoundException
				| NoSuchMethodException
				| InstantiationException
				| IllegalAccessException
				| InvocationTargetException ex) {

				throw new IllegalArgumentException(ex);
			}

			// leaf node.
			if (stageNode.children == null) {

				if (stageNode.stage == null
					|| stageNode.stage.getParams().get(HasModelFilePath.MODEL_FILE_PATH) != null) {

					// pass
				} else if (stageNode.stage instanceof ModelBase <?>) {

					final long localId = id[0];
					final int[] localSchemaIndices = stageNode.schemaIndices;
					BatchOperator <?> model = new TableSourceBatchOp(
						DataSetConversionUtil.toTable(
							deserialized[0].getMLEnvironmentId(),
							deserialized[0]
								.getDataSet()
								.filter(new FilterFunction <Row>() {
									private static final long serialVersionUID = 355683133177055891L;

									@Override
									public boolean filter(Row value) {
										return value.getField(0).equals(localId);
									}
								})
								.map(new MapFunction <Row, Row>() {
									private static final long serialVersionUID = -4286266312978550037L;

									@Override
									public Row map(Row value) throws Exception {
										Row ret = new Row(localSchemaIndices.length);

										for (int i = 0; i < localSchemaIndices.length; ++i) {
											ret.setField(i, value.getField(localSchemaIndices[i] + offset));
										}
										return ret;
									}
								})
								.returns(new RowTypeInfo(stageNode.types)),
							new TableSchema(stageNode.colNames, stageNode.types)
						)
					).setMLEnvironmentId(deserialized[0].getMLEnvironmentId());

					((ModelBase <?>) stageNode.stage).setModelData(model);

					deserialized[0] = new TableSourceBatchOp(
						DataSetConversionUtil.toTable(
							deserialized[0].getMLEnvironmentId(),
							deserialized[0]
								.getDataSet()
								.filter(new FilterFunction <Row>() {
									private static final long serialVersionUID = -2803966833769030531L;

									@Override
									public boolean filter(Row value) {
										return !value.getField(0).equals(localId);
									}
								}),
							schema
						)
					).setMLEnvironmentId(deserialized[0].getMLEnvironmentId());
				}
			} else {
				List <T> pipelineStageBases = new ArrayList <>();

				for (int i = 0; i < stageNode.children.length; ++i) {
					pipelineStageBases.add((T) stages[stageNode.children[i]].stage);
				}

				if (stageNode.stage == null) {
					result.addAll(pipelineStageBases);
					return;
				}

				if (stageNode.stage instanceof Pipeline) {
					stageNode.stage = new Pipeline(pipelineStageBases.toArray(new PipelineStageBase <?>[0]));
				} else if (stageNode.stage instanceof PipelineModel) {
					stageNode.stage = new PipelineModel(pipelineStageBases.toArray(new TransformerBase <?>[0]));
				} else {
					throw new IllegalArgumentException("Unsupported stage.");
				}
			}

			id[0]--;
		};

		postOrder(stages, deserializer);

		return result;
	}

	private static <T extends PipelineStageBase <?>> List <T> postOrderUnPackWithoutModelData(StageNode[] stages) {

		if (stages == null || stages.length == 0) {
			return null;
		}

		LinkedList <Integer> stack = new LinkedList <>();
		stack.push(0);

		long id = stages.length - 1;
		int lastLp = -1;

		while (!stack.isEmpty()) {
			int lp = stack.peek();
			if (stages[lp].children != null && lastLp != stages[lp].children[0]) {
				for (int i = 0; i < stages[lp].children.length; ++i) {
					stack.push(stages[lp].children[i]);
				}
			} else {

				try {
					if (stages[lp].identifier != null) {
						stages[lp].stage = (PipelineStageBase <?>) Class
							.forName(stages[lp].identifier)
							.getConstructor(Params.class)
							.newInstance(stages[lp].params);
					}
				} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException |
					IllegalAccessException | InvocationTargetException e) {
					throw new RuntimeException(e);
				}

				// leaf node.
				if (stages[lp].children != null) {
					List <T> pipelineStageBases = new ArrayList <>();

					for (int i = 0; i < stages[lp].children.length; ++i) {
						pipelineStageBases.add((T) stages[stages[lp].children[i]].stage);
					}

					if (stages[lp].stage == null) {
						return pipelineStageBases;
					}

					if (stages[lp].stage instanceof Pipeline) {
						stages[lp].stage = new Pipeline(pipelineStageBases.toArray(new PipelineStageBase <?>[0]));
					} else if (stages[lp].stage instanceof PipelineModel) {
						stages[lp].stage = new PipelineModel(pipelineStageBases.toArray(new TransformerBase <?>[0]));
					} else {
						throw new IllegalArgumentException("Unsupported stage.");
					}
				}

				id--;

				lastLp = lp;
				stack.pop();
			}
		}

		return null;
	}

	public static Tuple3 <StageNode[], TableSchema, Row> extractStagesMeta(List <PipelineStageBase <?>> stages) {
		StageNode[] stageNodes = preOrderConstructStages(stages);

		TypeInformation <?>[] typesWithPrefix
			= preOrderCorrectIndex(postOrderCreateSchema(stageNodes), PREFIX_TYPES);

		TableSchema finalSchema = new TableSchema(
			ArrayUtils.addAll(new String[] {ID_COL_NAME}, appendPrefix(MODEL_COL_PREFIX, typesWithPrefix.length)),
			ArrayUtils.addAll(new TypeInformation[] {Types.LONG}, typesWithPrefix)
		);
		Row metaRow = serializeMeta(stageNodes, finalSchema.getFieldTypes().length, new Params());
		return Tuple3.of(stageNodes, finalSchema, metaRow);
	}

	static Tuple2 <StageNode[], Params> collectMetaFromOp(BatchOperator <?> packed) {
		return deserializeMeta(
			packed.filter(String.format("%s < 0", ID_COL_NAME)).collect().get(0),
			packed.getSchema(), 1
		);
	}

	static StageNode[] deserializePipelineStagesFromMeta(Row metaRow, TableSchema schema) {
		return deserializeMeta(metaRow, schema, 1).f0;
	}

	static Tuple2 <StageNode[], Params> deserializePipelineStagesAndParamsFromMeta(Row metaRow, TableSchema schema) {
		return deserializeMeta(metaRow, schema, 1);
	}

	static <T extends PipelineStageBase <?>> List <T> fillPipelineStages(
		BatchOperator <?> packed, StageNode[] stages, TableSchema schema) {

		return postOrderDeserialize(stages, packed, schema, 1);
	}

	public static <T extends PipelineStageBase <?>> List <T> constructPipelineStagesFromMeta(
		Row metaRow, TableSchema schema) {

		StageNode[] stages = deserializeMeta(metaRow, schema, 1).f0;
		return postOrderUnPackWithoutModelData(stages);
	}

	private static class MetaReader implements FileProcFunction <FilePath, Boolean> {
		private Row meta;
		private TableSchema schema;

		@Override
		public Boolean apply(FilePath filePath) throws IOException {

			boolean fileExists = filePath.getFileSystem().exists(filePath.getPath());

			if (!fileExists) {
				throw new IllegalArgumentException("Could not find the file: " + filePath.getPathStr());
			}

			AkStream stream = new AkStream(filePath);

			schema = TableUtil.schemaStr2Schema(stream.getAkMeta().schemaStr);

			final int idColIndex = TableUtil.findColIndexWithAssertAndHint(schema, ID_COL_NAME);

			try (AkReader reader = stream.getReader()) {
				for (Row r : reader) {
					if ((Long) r.getField(idColIndex) < 0) {
						meta = r;

						// find the meta row, interrupt the loop.
						return false;
					}
				}

				// continue to scan folder
				return true;
			}
		}

		public Row getMeta() {
			return meta;
		}

		public TableSchema getSchema() {
			return schema;
		}
	}

	public static Tuple2 <TableSchema, Row> loadMetaFromAkFile(FilePath filePath) {
		MetaReader metaReader = new MetaReader();

		try {
			AkUtils.getFromFolderForEach(filePath, metaReader);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}

		if (metaReader.getMeta() == null || metaReader.getSchema() == null) {
			throw new IllegalArgumentException(String.format("Count not get meta from %s.", filePath.getPathStr()));
		}

		return Tuple2.of(metaReader.getSchema(), metaReader.getMeta());
	}

	public static List <Tuple3 <PipelineStageBase <?>, TableSchema, List <Row>>> loadStagesFromPipelineModel(
		List <Row> pipelineModel, TableSchema modelSchema) {

		final int idColIndex = TableUtil.findColIndexWithAssertAndHint(modelSchema, ID_COL_NAME);

		int size = pipelineModel.size();

		Integer[] order = new Integer[size];

		for (int i = 0; i < size; ++i) {
			order[i] = i;
		}

		Arrays.sort(order, Comparator.comparing(o -> ((Long) pipelineModel.get(o).getField(idColIndex))));

		Preconditions.checkState(!pipelineModel.isEmpty(), "Model to load should not be empty.");

		StageNode[] stages = deserializePipelineStagesFromMeta(pipelineModel.get(order[0]), modelSchema);

		if (stages == null || stages.length == 0) {
			return new ArrayList <>();
		}

		final int[] cursor = new int[] {size - 1};

		final int offset = 1;

		List <Tuple3 <PipelineStageBase <?>, TableSchema, List <Row>>> reverseStages = new ArrayList <>();

		Consumer <Integer> deserializer = lp -> {
			StageNode stageNode = stages[lp];

			try {
				if (stageNode.identifier != null) {
					stageNode.stage = (PipelineStageBase <?>) Class
						.forName(stageNode.identifier)
						.getConstructor(Params.class)
						.newInstance(stageNode.params);
				}
			} catch (ClassNotFoundException
				| NoSuchMethodException
				| InstantiationException
				| IllegalAccessException
				| InvocationTargetException e) {

				throw new IllegalArgumentException(e);
			}

			// leaf node.
			if (stageNode.children == null) {
				if (stageNode.stage == null) {
					reverseStages.add(Tuple3.of(null, null, null));
				} else if (stageNode.stage.getParams().get(HasModelFilePath.MODEL_FILE_PATH) != null) {

					List<Row> modelData = new ArrayList <>();
					TableSchema schema;
					try {
						AkStream akStream = new AkStream(
							FilePath.deserialize(stageNode.stage.getParams().get(HasModelFilePath.MODEL_FILE_PATH))
						);

						schema = TableUtil.schemaStr2Schema(akStream.getAkMeta().schemaStr);

						try (AkReader akReader = akStream.getReader()) {
							for (Row row : akReader) {
								modelData.add(row);
							}
						}
					} catch (IOException e) {
						throw new IllegalStateException(e);
					}

					reverseStages.add(Tuple3.of(stageNode.stage, schema, modelData));
				} else if (stageNode.stage instanceof ModelBase) {

					final int[] localSchemaIndices = stageNode.schemaIndices;
					final int oldCursor = cursor[0];
					cursor[0] = next(pipelineModel, order, cursor[0], idColIndex);
					List <Row> localData = IntStream
						.range(cursor[0] + 1, oldCursor + 1)
						.mapToObj(value -> pipelineModel.get(order[value]))
						.map(value -> {
							Row ret = new Row(localSchemaIndices.length);

							for (int i = 0; i < localSchemaIndices.length; ++i) {
								ret.setField(i, value.getField(localSchemaIndices[i] + offset));
							}
							return ret;
						}).collect(Collectors.toList());

					reverseStages.add(
						Tuple3.of(
							stageNode.stage,
							new TableSchema(stageNode.colNames, stageNode.types),
							localData
						)
					);
				} else {
					reverseStages.add(Tuple3.of(stageNode.stage, null, null));
				}
			} else {
				if (lp != 0) {
					throw new IllegalArgumentException("There should not have nested pipeline or pipeline model.");
				}
			}
		};

		postOrder(stages, deserializer);

		return Lists.reverse(reverseStages);
	}

	public static LocalPredictor loadLocalPredictorFromPipelineModel(
		List <Tuple3 <PipelineStageBase <?>, TableSchema, List <Row>>> stages, TableSchema inputSchema) {
		return new LocalPredictor(loadMapperListFromStages(stages, inputSchema).getMappers());
	}

	public static LocalPredictor loadLocalPredictorFromPipelineModel(
		List <Row> pipelineModel, TableSchema modelSchema, TableSchema inputSchema) {

		return loadLocalPredictorFromPipelineModel(
			loadStagesFromPipelineModel(pipelineModel, modelSchema),
			inputSchema
		);
	}

	//mapper not open.
	public static MapperChain loadMapperListFromStages(
		List <Row> pipelineModel, TableSchema modelSchema, TableSchema inputSchema) {
		return loadMapperListFromStages(
			loadStagesFromPipelineModel(pipelineModel, modelSchema),
			inputSchema
		);
	}

	//mapper not open.
	public static MapperChain loadMapperListFromStages(
		List <Tuple3 <PipelineStageBase <?>, TableSchema, List <Row>>> stages, TableSchema inputSchema) {

		TableSchema outSchema = inputSchema;
		List <Mapper> mappers = new ArrayList <>();
		for (Tuple3 <PipelineStageBase <?>, TableSchema, List <Row>> stageTuple3 : stages) {
			Mapper mapper = createMapperFromStage(stageTuple3.f0, stageTuple3.f1, outSchema, stageTuple3.f2);
			mappers.add(mapper);
			outSchema = mapper.getOutputSchema();
		}

		return new MapperChain(mappers.toArray(new Mapper[0]));
	}

	//not open and load.
	public static Mapper createMapperFromStage(
		PipelineStageBase <?> stage, TableSchema modelSchema, TableSchema inputSchema, List <Row> data) {
		Mapper mapper = null;
		if (stage instanceof MapModel) {
			MapModel <?> mapModel = (MapModel <?>) stage;
			mapper = mapModel
				.mapperBuilder
				.apply(modelSchema, inputSchema, mapModel.getParams());
			if (data != null) {
				((ModelMapper) mapper).loadModel(data);
			}
		} else if (stage instanceof BaseRecommender) {
			mapper = RecommenderUtil.createRecommMapper(
				(BaseRecommender <?>) stage, modelSchema, inputSchema, data
			);
		} else if (stage instanceof MapTransformer) {
			MapTransformer <?> mapTransformer = (MapTransformer <?>) stage;
			mapper = mapTransformer
				.mapperBuilder
				.apply(inputSchema, mapTransformer.getParams());
		} else {
			throw new RuntimeException("not support yet.");
		}

		if (mapper instanceof ComboModelMapper) {
			((ComboModelMapper) mapper).newMapperList();
		}

		if (mapper instanceof ComboMapper) {
			((ComboMapper) mapper).newMapperList();
		}
		return mapper;
	}

	static Mapper[] loadLocalPredictorFromPipelineModelAsMappers(
		FilePath filePath, TableSchema inputSchema) throws Exception {
		Tuple2 <TableSchema, List <Row>> readed = AkUtils.readFromPath(filePath);
		Tuple2 <TableSchema, Row> schemaAndMeta = ModelExporterUtils.loadMetaFromAkFile(filePath);
		Tuple2 <StageNode[], Params> stagesAndParams
			= ModelExporterUtils.deserializePipelineStagesAndParamsFromMeta(schemaAndMeta.f1, schemaAndMeta.f0);
		Mapper[] mappers = loadMapperListFromStages(readed.f1, readed.f0, inputSchema).getMappers();
		Params params = stagesAndParams.f1;
		if (params.get(ModelStreamScanParams.MODEL_STREAM_FILE_PATH) != null) {
			TableSchema extendSchema = mappers[mappers.length - 1].getOutputSchema();
			params.set(PipelineModelMapper.PIPELINE_TRANSFORM_OUT_COL_NAMES, extendSchema.getFieldNames());
			params.set(PipelineModelMapper.PIPELINE_TRANSFORM_OUT_COL_TYPES,
				FlinkTypeConverter.getTypeString(extendSchema.getFieldTypes()));
			PipelineModelMapper pipelineModelMapper
				= new PipelineModelMapper(readed.f0, inputSchema, params);
			pipelineModelMapper.loadModel(readed.f1);
			return new Mapper[]{pipelineModelMapper};
		}
		return mappers;
	}

	private static int next(List <Row> all, Integer[] order, int cursor, int field) {
		Object obj = all.get(order[cursor]).getField(field);
		cursor--;

		while (cursor >= 0
			&& all.get(order[cursor]) != null
			&& all.get(order[cursor]).getField(field) != null
			&& all.get(order[cursor]).getField(field).equals(obj)) {

			cursor--;
		}

		return cursor;
	}
}
