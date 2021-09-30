package com.alibaba.alink.pipeline;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.UnionAllBatchOp;
import com.alibaba.alink.operator.batch.utils.TensorSerializeBatchOp;
import com.alibaba.alink.operator.batch.utils.VectorSerializeBatchOp;
import com.alibaba.alink.operator.common.io.csv.CsvFormatter;
import com.alibaba.alink.operator.common.io.csv.CsvParser;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.io.CsvSourceParams;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;
import com.jayway.jsonpath.JsonPath;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Deprecated
public class LegacyModelExporterUtils {
	@Deprecated
	public static final TableSchema PIPELINE_MODEL_SCHEMA = new TableSchema(
		new String[] {"model_id", "model_data"}, new TypeInformation[] {Types.LONG, Types.STRING}
	);

	@Deprecated
	public static boolean isLegacyPipelineModel(TableSchema schema) {
		return schema.equals(PIPELINE_MODEL_SCHEMA);
	}

	/**
	 * Extract meta data of a transformer array from the BatchOperator that stores a packed transformers array. An
	 * <code>collect</code> of dataset would be incur.
	 */
	@Deprecated
	static String extractMetaOfPipelineStages(BatchOperator <?> batchOp) {
		String configStr;
		try {
			List <Row> rows = batchOp.as(new String[] {"f1", "f2"}).where("f1=-1").collect();
			Preconditions.checkArgument(rows.size() == 1, "Invalid stages.");
			configStr = (String) rows.get(0).getField(1);
		} catch (Exception e) {
			throw new RuntimeException("Fail to collect stages config.", e);
		}
		return configStr;
	}

	@Deprecated
	static String extractMetaOfPipelineStages(Row metaRow) {
		return (String) metaRow.getField(1);
	}

	/**
	 * Unpack transformers array from a BatchOperator.
	 */
	@Deprecated
	static TransformerBase <?>[] unpackTransformersArray(BatchOperator <?> batchOp,
														 String[] clazzNames, Params[] params, TableSchema[] schemas) {
		return unpackPipelineStages(batchOp, clazzNames, params, schemas).toArray(new TransformerBase <?>[0]);
	}

	@Deprecated
	static List <PipelineStageBase <?>> unpackPipelineStages(BatchOperator <?> batchOp,
															 String[] clazzNames, Params[] params,
															 TableSchema[] schemas) {
		int numPipelineStages = clazzNames.length;
		PipelineStageBase <?>[] pipelineStageBases = new PipelineStageBase[numPipelineStages];
		for (int i = 0; i < numPipelineStages; i++) {
			try {
				Class <?> clazz = Class.forName(clazzNames[i]);
				pipelineStageBases[i] = (PipelineStageBase <?>) clazz.getConstructor(Params.class).newInstance(
					params[i].set(HasMLEnvironmentId.ML_ENVIRONMENT_ID, batchOp.getMLEnvironmentId()));
			} catch (Exception e) {
				throw new RuntimeException(String.format("Fail to re construct transformer %s.", clazzNames[i]), e);
			}

			BatchOperator <?> packed = batchOp.as(new String[] {"f1", "f2"}).where("f1=" + i);
			if (pipelineStageBases[i] instanceof PipelineModel) {
				BatchOperator <?> data = unpackBatchOp(packed, schemas[i]);
				pipelineStageBases[i] = new PipelineModel(unpackTransformersArray(data))
					.setMLEnvironmentId(batchOp.getMLEnvironmentId());
			} else if (pipelineStageBases[i] instanceof ModelBase <?>) {
				if (schemas[i] != null) {
					BatchOperator <?> data = unpackBatchOp(packed, schemas[i]);
					((ModelBase <?>) pipelineStageBases[i]).setModelData(data);
				}
			} else if (pipelineStageBases[i] instanceof Pipeline) {
				BatchOperator <?> data = unpackBatchOp(packed, schemas[i]);
				pipelineStageBases[i] = new Pipeline(unpackPipelineStages(data).toArray(new PipelineStageBase[0]))
					.setMLEnvironmentId(batchOp.getMLEnvironmentId());
			}
		}
		return Arrays.asList(pipelineStageBases);
	}

	/**
	 * Unpack transformers array from a BatchOperator.
	 */
	@Deprecated
	static TransformerBase <?>[] unpackTransformersArray(BatchOperator <?> batchOp) {
		return unpackPipelineStages(batchOp).toArray(new TransformerBase <?>[0]);
	}

	@Deprecated
	static TransformerBase <?>[] unpackTransformersArray(BatchOperator <?> batchOp, Row metaRow) {
		return unpackPipelineStages(batchOp, metaRow).toArray(new TransformerBase <?>[0]);
	}

	@Deprecated
	static List <PipelineStageBase <?>> unpackPipelineStages(BatchOperator <?> batchOp) {
		return unpackPipelineStages(batchOp, extractMetaOfPipelineStages(batchOp));
	}

	@Deprecated
	static List <PipelineStageBase <?>> unpackPipelineStages(BatchOperator <?> batchOp, Row metaRow) {
		return unpackPipelineStages(batchOp, extractMetaOfPipelineStages(metaRow));
	}

	@Deprecated
	static List <PipelineStageBase <?>> unpackPipelineStages(BatchOperator <?> batchOp, String meta) {
		String[] clazzNames = JsonConverter.fromJson(JsonPath.read(meta, "$.clazz").toString(), String[].class);
		String[] params0 = JsonConverter.fromJson(JsonPath.read(meta, "$.param").toString(), String[].class);
		String[] schemas0 = JsonConverter.fromJson(JsonPath.read(meta, "$.schema").toString(), String[].class);
		Params[] params = new Params[params0.length];
		TableSchema[] schemas = new TableSchema[schemas0.length];
		for (int i = 0; i < params0.length; i++) {
			params[i] = Params.fromJson(params0[i]);
		}
		for (int i = 0; i < schemas0.length; i++) {
			if (!StringUtils.isNullOrWhitespaceOnly(schemas0[i])) {
				schemas[i] = CsvUtil.schemaStr2Schema(schemas0[i]);
			}
		}
		return unpackPipelineStages(batchOp, clazzNames, params, schemas);
	}

	/**
	 * Unpack a BatchOperator.
	 */
	@Deprecated
	private static BatchOperator <?> unpackBatchOp(BatchOperator <?> data, TableSchema schema) {
		DataSet <Row> rows = data.getDataSet();
		final TypeInformation <?>[] types = schema.getFieldTypes();

		rows = rows.map(new RichMapFunction <Row, Row>() {
			private static final long serialVersionUID = 7791442624358724472L;
			private transient CsvParser parser;

			@Override
			public void open(Configuration parameters) throws Exception {
				parser = new CsvParser(types, "^", '\'');
			}

			@Override
			public Row map(Row value) throws Exception {
				return parser.parse((String) value.getField(1)).f1;
			}
		});

		return BatchOperator.fromTable(DataSetConversionUtil.toTable(data.getMLEnvironmentId(), rows, schema))
			.setMLEnvironmentId(data.getMLEnvironmentId());
	}

	@Deprecated
	public static Tuple2 <TableSchema, Row> loadMetaFromCsvFile(FilePath filePath) {
		boolean fileExists;
		try {
			fileExists = filePath.getFileSystem().exists(filePath.getPath());
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}

		if (!fileExists) {
			throw new IllegalArgumentException("Could not find the file: " + filePath.getPathStr());
		}

		final int idColIndex = 0;

		CsvUtil.ParseCsvFunc parseFunc = new CsvUtil.ParseCsvFunc(
			PIPELINE_MODEL_SCHEMA.getFieldTypes(),
			CsvSourceParams.FIELD_DELIMITER.getDefaultValue(),
			CsvSourceParams.QUOTE_CHAR.getDefaultValue(),
			CsvSourceParams.SKIP_BLANK_LINE.getDefaultValue(),
			CsvSourceParams.LENIENT.getDefaultValue()
		);

		try {
			parseFunc.open(new Configuration());
		} catch (Exception e) {
			throw new IllegalArgumentException("Initial csv parse func error.", e);
		}

		Row meta = null;

		try (BufferedReader bufferedReader = new BufferedReader(
			new InputStreamReader(filePath.getFileSystem().open(filePath.getPath())))) {

			CsvRowCollector rowCollector = new CsvRowCollector();
			while (bufferedReader.ready()) {
				parseFunc.flatMap(
					Row.of(bufferedReader.readLine()),
					rowCollector
				);

				if (rowCollector.getCurrent() != null
					&& ((Long) rowCollector.getCurrent().getField(idColIndex)) < 0) {
					meta = rowCollector.getCurrent();
					break;
				}

				rowCollector.close();
			}

			Preconditions.checkState(meta != null,
				"Could not find the meta row in the file: " + filePath.getPathStr());
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}

		return Tuple2.of(PIPELINE_MODEL_SCHEMA, meta);
	}

	/**
	 * Pack the pipeline model to a BatchOperator.
	 */
	@Deprecated
	public static BatchOperator <?> save(PipelineModel pipelineModel) {
		return packTransformersArray(pipelineModel.transformers);
	}

	/**
	 * Get the meta data of the serving model.
	 *
	 * @param modelSchema     Schema of model data
	 * @param inputDataSchema Schema of prediction data
	 * @return configuration string in json format
	 */
	@Deprecated
	public static String getModelServingMeta(TableSchema modelSchema, TableSchema inputDataSchema) {
		Map <String, String> config = new HashMap <>();
		config.put("modelSchema", CsvUtil.schema2SchemaStr(modelSchema));
		config.put("inputDataSchema", CsvUtil.schema2SchemaStr(inputDataSchema));
		config.put("modelVersion", "v0.2");
		return JsonConverter.toJson(config);
	}

	/**
	 * Get the meta data of pipeline stages. Such meta can guide us to reconstruct the pipeline stages.
	 */
	@Deprecated
	static String getMetaOfPipelineStages(List <PipelineStageBase <?>> stages) {
		int numStages = stages.size();
		String[] clazzNames = new String[numStages];
		Params[] params = new Params[numStages];
		TableSchema[] schemas = new TableSchema[numStages];
		for (int i = 0; i < numStages; i++) {
			clazzNames[i] = stages.get(i).getClass().getCanonicalName();
			params[i] = stages.get(i).getParams();
			schemas[i] = null;
			if (stages.get(i) instanceof PipelineModel) {
				schemas[i] = PIPELINE_MODEL_SCHEMA;
			} else if (stages.get(i) instanceof ModelBase) {
				if (((ModelBase<?>) stages.get(i)).getModelData() != null) {
					BatchOperator <?> data
						= ((ModelBase<?>) stages.get(i)).getModelData()
						.link(new VectorSerializeBatchOp())
						.link(new TensorSerializeBatchOp());

					schemas[i] = data.getSchema();
				}
			} else if (stages.get(i) instanceof Pipeline) {
				schemas[i] = PIPELINE_MODEL_SCHEMA;
			}
		}
		return getMetaOfPipelineStages(clazzNames, params, schemas);
	}

	/**
	 * Get the meta data of pipeline stages. Such meta can guide us to reconstruct the pipeline stages.
	 *
	 * @param clazzNames Class name of each stage.
	 * @param params0    Params of each stage
	 * @param schemas0   Schemas of the model table of each stage
	 * @return The meta string.
	 */
	@Deprecated
	public static String getMetaOfPipelineStages(String[] clazzNames, Params[] params0, TableSchema[] schemas0) {
		int numTransformers = clazzNames.length;
		String[] params = new String[numTransformers];
		String[] schemas = new String[numTransformers];
		for (int i = 0; i < numTransformers; i++) {
			params[i] = params0[i].toJson();
			schemas[i] = "";
			if (schemas0[i] != null) {
				schemas[i] = CsvUtil.schema2SchemaStr(schemas0[i]);
			}
		}
		Map <String, Object> config = new HashMap <>();
		config.put("clazz", clazzNames);
		config.put("param", params);
		config.put("schema", schemas);
		return JsonConverter.toJson(config);
	}

	/**
	 * Pack an array of transformers to a BatchOperator.
	 */
	@Deprecated
	static BatchOperator <?> packTransformersArray(TransformerBase <?>[] transformers) {
		return packPipelineStages(Arrays.asList(transformers));
	}

	/**
	 * Pack an array of transformers to a BatchOperator.
	 */
	@Deprecated
	static BatchOperator <?> packPipelineStages(List <PipelineStageBase <?>> stages) {
		int numStages = stages.size();
		Row row = Row.of(-1L, getMetaOfPipelineStages(stages));

		BatchOperator <?> packed = new MemSourceBatchOp(
			Collections.singletonList(row), PIPELINE_MODEL_SCHEMA)
			.setMLEnvironmentId(stages.size() > 0 ? stages.get(0).getMLEnvironmentId() :
				MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID);
		for (int i = 0; i < numStages; i++) {
			BatchOperator <?> data = null;
			final long envId = stages.get(i).getMLEnvironmentId();
			if (stages.get(i) instanceof PipelineModel) {
				data = packTransformersArray(((PipelineModel) stages.get(i)).transformers);
			} else if (stages.get(i) instanceof ModelBase) {
				if (((ModelBase<?>) stages.get(i)).getModelData() != null) {
					data = ((ModelBase<?>) stages.get(i)).getModelData()
						.setMLEnvironmentId(envId);
					data = data
						.link(new VectorSerializeBatchOp().setMLEnvironmentId(envId))
						.link(new TensorSerializeBatchOp().setMLEnvironmentId(envId));
				}
			} else if (stages.get(i) instanceof Pipeline) {
				data = packPipelineStages(((Pipeline) stages.get(i)).stages);
			}

			if (data != null) {
				packed = new UnionAllBatchOp().setMLEnvironmentId(envId).linkFrom(packed, packBatchOp(data, i));
			}
		}
		return packed;
	}

	/**
	 * Pack a BatchOperator with arbitrary schema into a BatchOperator that has two columns, one id column of Long
	 * type,
	 * and one data column of String type.
	 */
	@Deprecated
	private static BatchOperator <?> packBatchOp(BatchOperator <?> data, final long id) {
		DataSet <Row> rows = data.getDataSet();

		final TypeInformation <?>[] types = data.getColTypes();

		rows = rows.map(new RichMapFunction <Row, Row>() {
			private static final long serialVersionUID = -182592464977659219L;
			transient CsvFormatter formatter;

			@Override
			public void open(Configuration parameters) throws Exception {
				formatter = new CsvFormatter(types, "^", '\'');
			}

			@Override
			public Row map(Row value) throws Exception {
				return Row.of(id, formatter.format(value));
			}
		});

		return BatchOperator.fromTable(DataSetConversionUtil.toTable(data.getMLEnvironmentId(), rows,
			new String[] {"f1", "f2"}, new TypeInformation[] {Types.LONG, Types.STRING}))
			.setMLEnvironmentId(data.getMLEnvironmentId());
	}

	@Deprecated
	private static class CsvRowCollector implements Collector <Row> {
		Row current;

		@Override
		public void collect(Row record) {
			current = record;
		}

		@Override
		public void close() {
			current = null;
		}

		public Row getCurrent() {
			return current;
		}
	}

	@Deprecated
	public static Pipeline load(BatchOperator <?> batchOp, Row metaRow) {
		if (!LegacyModelExporterUtils.isLegacyPipelineModel(batchOp.getSchema())) {
			return new Pipeline(
				ModelExporterUtils. <TransformerBase <?>>fillPipelineStages(
					batchOp,
					ModelExporterUtils.deserializePipelineStagesFromMeta(metaRow, batchOp.getSchema()),
					batchOp.getSchema()
				).toArray(new PipelineStageBase <?>[0])
			);
		} else {
			return new Pipeline(
				LegacyModelExporterUtils.unpackPipelineStages(batchOp, metaRow).toArray(new PipelineStageBase[0]));
		}
	}

	@Deprecated
	public static Pipeline collectLoad(BatchOperator <?> batchOp) {
		if (!LegacyModelExporterUtils.isLegacyPipelineModel(batchOp.getSchema())) {
			return Pipeline.collectLoad(batchOp);
		} else {
			return new Pipeline(LegacyModelExporterUtils
				.unpackPipelineStages(batchOp).toArray(new PipelineStageBase[0])
			);
		}
	}

	@Deprecated
	public static Pipeline load(FilePath filePath, Long mlEnvId) {
		boolean isAkFile;

		try {
			isAkFile = AkUtils.isAkFile(filePath);
		} catch (IOException e) {
			throw new IllegalArgumentException("Check file error.", e);
		}

		if (isAkFile) {
			return Pipeline.load(filePath, mlEnvId);
		} else {
			// legacy model.
			return new Pipeline(
				LegacyModelExporterUtils.unpackPipelineStages(
					new CsvSourceBatchOp()
						.setMLEnvironmentId(mlEnvId)
						.setFilePath(filePath)
						.setSchemaStr(CsvUtil.schema2SchemaStr(PIPELINE_MODEL_SCHEMA)),
					LegacyModelExporterUtils.loadMetaFromCsvFile(filePath).f1
				).toArray(new PipelineStageBase[0])
			);
		}
	}

	@Deprecated
	public static PipelineModel loadPipelineModel(FilePath filePath, Long mlEnvId) {
		boolean isAkFile;

		try {
			isAkFile = AkUtils.isAkFile(filePath);
		} catch (IOException e) {
			throw new IllegalArgumentException("Check file error.", e);
		}

		if (isAkFile) {
			return PipelineModel.load(filePath, mlEnvId);
		} else {
			// legacy model.
			return new PipelineModel(LegacyModelExporterUtils.unpackTransformersArray(
				new CsvSourceBatchOp()
					.setMLEnvironmentId(mlEnvId)
					.setFilePath(filePath)
					.setSchemaStr(CsvUtil.schema2SchemaStr(LegacyModelExporterUtils.PIPELINE_MODEL_SCHEMA)),
				LegacyModelExporterUtils.loadMetaFromCsvFile(filePath).f1)
			);
		}
	}

	@Deprecated
	public static PipelineModel collectLoadPipelineModel(BatchOperator <?> batchOp) {
		if (!LegacyModelExporterUtils.isLegacyPipelineModel(batchOp.getSchema())) {
			return PipelineModel.collectLoad(batchOp);
		} else {
			return new PipelineModel(LegacyModelExporterUtils.unpackTransformersArray(batchOp));
		}
	}

	/**
	 * Load the pipeline model from a BatchOperator. The meta are provided by the caller, so there is no need to do
	 * <code>collect</code>.
	 */
	@Deprecated
	public static PipelineModel loadPipelineModel(BatchOperator <?> batchOp, String[] clazzNames, Params[] params,
												  TableSchema[] schemas) {
		return new PipelineModel(
			LegacyModelExporterUtils.unpackTransformersArray(batchOp, clazzNames, params, schemas));
	}

	@Deprecated
	public static PipelineModel loadPipelineModel(BatchOperator <?> batchOp, Row metaRow) {
		if (!LegacyModelExporterUtils.isLegacyPipelineModel(batchOp.getSchema())) {
			return new PipelineModel(
				ModelExporterUtils. <TransformerBase <?>>fillPipelineStages(
					batchOp,
					ModelExporterUtils.deserializePipelineStagesFromMeta(metaRow, batchOp.getSchema()),
					batchOp.getSchema()
				).toArray(new TransformerBase <?>[0])
			);
		} else {
			return new PipelineModel(LegacyModelExporterUtils.unpackTransformersArray(batchOp, metaRow));
		}
	}

}
