package com.alibaba.alink.pipeline;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.csv.CsvParser;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;
import com.jayway.jsonpath.JsonPath;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Deprecated
public class LocalPredictorLoader implements Serializable {

	@Deprecated
	public static LocalPredictor loadFromPipelineModelPath(String pipelineModelPath, String inputSchemaStr)
		throws Exception {
		return load(new FilePath(pipelineModelPath), TableUtil.schemaStr2Schema(inputSchemaStr));
	}

	@Deprecated
	public static LocalPredictor load(FilePath pipelineModelPath, String inputSchemaStr) throws Exception {
		return load(pipelineModelPath, TableUtil.schemaStr2Schema(inputSchemaStr));
	}

	@Deprecated
	public static LocalPredictor load(FilePath pipelineModelPath, TableSchema inputSchema) throws Exception {
		return AkUtils.isAkFile(pipelineModelPath) ?
			new LocalPredictor(pipelineModelPath, inputSchema)
			: LocalPredictorLoader
				.loadLocalPredictor(pipelineModelPath.getPathStr(), inputSchema);
	}

	@Deprecated
	public static LocalPredictor load(List <Row> pipelineModel, TableSchema modelSchema, TableSchema inputSchema)
		throws Exception {
		return !LegacyModelExporterUtils.isLegacyPipelineModel(modelSchema) ?
			new LocalPredictor(pipelineModel, modelSchema, inputSchema)
			: LocalPredictorLoader
				.loadLocalPredictor(pipelineModel, inputSchema);
	}

	@Deprecated
	public static LocalPredictor load(String modelPath, String inputSchemaStr) throws Exception {
		return loadLocalPredictor(modelPath, TableUtil.schemaStr2Schema(inputSchemaStr));
	}

	/**
	 * Create a local predictor by loading model from a CSV file, without launching Flink job.
	 *
	 * @param modelPath   Path of the model file in CSV format. Can be local file or remote file.
	 * @param inputSchema Test data schema.
	 * @return
	 * @throws Exception
	 */
	@Deprecated
	public static LocalPredictor loadLocalPredictor(String modelPath, TableSchema inputSchema) throws Exception {

		Map <Long, List <Row>> rows = readPipelineModelRowsFromCsvFile(modelPath,
			LegacyModelExporterUtils.PIPELINE_MODEL_SCHEMA);
		Preconditions.checkState(rows.containsKey(-1L), "can't find meta in model.");
		String meta = (String) rows.get(-1L).get(0).getField(1);

		PipelineStageBase[] transformers = constructPipelineStagesFromMeta(meta, 0L);
		String[] modelSchemaStr = JsonConverter.fromJson(JsonPath.read(meta, "$.schema").toString(), String[].class);

		LocalPredictor predictor = null;
		TableSchema schema = inputSchema;

		for (int i = 0; i < transformers.length; i++) {
			PipelineStageBase transformer = transformers[i];
			LocalPredictor localPredictor;

			if (transformer instanceof MapModel) {
				MapModel <?> mapModel = (MapModel) transformer;
				ModelMapper mapper = mapModel
					.mapperBuilder
					.apply(TableUtil.schemaStr2Schema(modelSchemaStr[i]), schema, mapModel.getParams());
				CsvParser csvParser = new CsvParser(TableUtil.getColTypes(modelSchemaStr[i]), "^", '\'');
				List <Row> modelRows = rows.get((long) i);
				int s = modelRows.size();
				for (int j = 0; j < s; j++) {
					Row r = modelRows.get(j);
					r = csvParser.parse((String) r.getField(1)).f1;
					modelRows.set(j, r);
				}
				mapper.loadModel(modelRows);
				localPredictor = new LocalPredictor(mapper);
			} else {
				localPredictor = ((LocalPredictable) transformer).collectLocalPredictor(schema);
			}

			schema = localPredictor.getOutputSchema();
			if (predictor == null) {
				predictor = localPredictor;
			} else {
				predictor.merge(localPredictor);
			}
		}

		return predictor;
	}

	/**
	 * Create a local predictor by loading model from a List<Row> format, without launching Flink job.
	 *
	 * @param modelRows model in list<Row></Row> format.
	 * @return
	 * @throws Exception
	 */
	@Deprecated
	public static LocalPredictor loadLocalPredictor(List <Row> modelRows, TableSchema dataSchema) throws Exception {
		Map <Long, List <Row>> mapRows = getMapModelRows(modelRows);
		String meta = (String) mapRows.get(-1L).get(0).getField(1);

		PipelineStageBase[] transformers = constructPipelineStagesFromMeta(meta, 0L);
		String[] modelSchemaStr = JsonConverter.fromJson(JsonPath.read(meta,
			"$.schema").toString(), String[].class);

		LocalPredictor predictor = null;
		TableSchema schema = dataSchema;
		for (int i = 0; i < transformers.length; i++) {
			PipelineStageBase transformer = transformers[i];
			LocalPredictor localPredictor = null;

			if (transformer instanceof MapModel) {
				MapModel <?> mapModel = (MapModel) transformer;
				ModelMapper mapper = mapModel
					.mapperBuilder
					.apply(TableUtil.schemaStr2Schema(modelSchemaStr[i]), schema, mapModel.getParams());
				CsvParser csvParser = new CsvParser(TableUtil.getColTypes(modelSchemaStr[i]), "^", '\'');
				List <Row> singleModelRows = mapRows.get((long) i);
				int s = singleModelRows.size();
				for (int j = 0; j < s; j++) {
					Row r = singleModelRows.get(j);
					r = csvParser.parse((String) r.getField(1)).f1;
					singleModelRows.set(j, r);
				}
				mapper.loadModel(singleModelRows);
				localPredictor = new LocalPredictor(mapper);
			} else {
				localPredictor = ((LocalPredictable) transformer).collectLocalPredictor(schema);
			}
			schema = localPredictor.getOutputSchema();
			if (predictor == null) {
				predictor = localPredictor;
			} else {
				predictor.merge(localPredictor);
			}
		}
		return predictor;
	}

	@Deprecated
	static Map <Long, List <Row>> readPipelineModelRowsFromCsvFile(String filePath, TableSchema schema)
		throws Exception {
		Map <Long, List <Row>> rows = new HashMap <>();
		Path path = new Path(filePath);
		FileSystem fs = FileSystem.get(path.toUri());
		FSDataInputStream stream = fs.open(path);
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
		CsvParser csvParser = new CsvParser(schema.getFieldTypes(), ",", '"');
		while (reader.ready()) {
			String line = reader.readLine();
			Tuple2 <Boolean, Row> parsed = csvParser.parse(line);
			Preconditions.checkState(parsed.f0, "Fail to parse line: " + line);
			Long id = (Long) parsed.f1.getField(0);
			if (rows.containsKey(id)) {
				rows.get(id).add(parsed.f1);
			} else {
				List <Row> r = new ArrayList <>();
				r.add(parsed.f1);
				rows.put(id, r);
			}
		}
		reader.close();
		stream.close();
		return rows;
	}

	@Deprecated
	public static PipelineStageBase[] constructPipelineStagesFromMeta(String meta, Long mlEnvId) {
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
				schemas[i] = TableUtil.schemaStr2Schema(schemas0[i]);
			}
		}

		int numPipelineStages = clazzNames.length;
		PipelineStageBase[] pipelineStageBases = new PipelineStageBase[numPipelineStages];

		for (int i = 0; i < numPipelineStages; i++) {
			try {
				Class clazz = Class.forName(clazzNames[i]);
				pipelineStageBases[i] = (PipelineStageBase) clazz.getConstructor(Params.class).newInstance(
					params[i].set(HasMLEnvironmentId.ML_ENVIRONMENT_ID, mlEnvId));
			} catch (Exception e) {
				throw new RuntimeException("Fail to re construct pipeline stage: ", e);
			}
		}
		return pipelineStageBases;
	}

	@Deprecated
	public static Map <Long, List <Row>> getMapModelRows(List <Row> modelRows) {
		Map <Long, List <Row>> rows = new HashMap <>();
		for (Row row : modelRows) {
			Long id = (Long) row.getField(0);
			if (rows.containsKey(id)) {
				rows.get(id).add(row);
			} else {
				List <Row> r = new ArrayList <>();
				r.add(row);
				rows.put(id, r);
			}
		}
		return rows;
	}
}