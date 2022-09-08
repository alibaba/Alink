package com.alibaba.alink.operator.common.utils;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.alink.common.pyrunner.PyCalcRunner;
import com.alibaba.alink.common.pyrunner.fn.BuiltInFnUtils;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.udf.BasePyBinaryFnParams;
import com.alibaba.alink.params.udf.BasePyBuiltInFnParams;
import com.alibaba.alink.params.udf.BasePyFileFnParams;
import com.alibaba.alink.params.udf.HasPythonEnvFilePath;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class UDFHelper {

	public static String generateRandomFuncName() {
		return "func_" + UUID.randomUUID().toString().replace("-", "");
	}

	public static JsonObject makeFnSpec(BasePyFileFnParams <?> params) {
		JsonObject fnSpec = new JsonObject();
		// TODO: support OSS, etc.
		Set <String> filePaths = Arrays.stream(params.getUserFilePaths())
			.map(FilePath::serialize)
			.collect(Collectors.toSet());
		JsonArray filePathsJsonArray = new JsonArray();
		for (String filePath : filePaths) {
			filePathsJsonArray.add(new JsonPrimitive(filePath));
		}
		fnSpec.add("filePaths", filePathsJsonArray);
		fnSpec.addProperty("className", params.getClassName());
		fnSpec.addProperty("pythonVersion", params.getPythonVersion());
		return fnSpec;
	}

	public static JsonObject makeFnSpec(BasePyBinaryFnParams <?> params) {
		JsonObject fnSpec = new JsonObject();
		//noinspection deprecation
		fnSpec.add("classObject", JsonConverter.gson.toJsonTree(params.getClassObject()));
		fnSpec.addProperty("classObjectType", params.getClassObjectType());
		fnSpec.addProperty("pythonVersion", params.getPythonVersion());
		return fnSpec;
	}

	public static JsonObject makeFnSpec(BasePyBuiltInFnParams <?> params) {
		JsonObject fnSpec = new JsonObject();
		fnSpec.addProperty(BuiltInFnUtils.KEY_FN_PLUGIN_NAME, params.getFnName());
		fnSpec.addProperty(BuiltInFnUtils.KEY_FN_PLUGIN_VERSION, params.getPluginVersion());
		fnSpec.addProperty(BuiltInFnUtils.KEY_FN_FACTORY_CONFIG,
			JsonConverter.toJson(new ResourcePluginFactory()));
		return fnSpec;
	}

	public static Map <String, String> makeRunConfig(BasePyFileFnParams <?> params) {
		Map <String, String> runConfig = new HashMap <>();
		// TODO: switch to UDF plugin
		if (params.getParams().contains(HasPythonEnvFilePath.PYTHON_ENV_FILE_PATH)) {
			runConfig.put(PyCalcRunner.PY_PYTHON_ENV_FILE_PATH, params.getPythonEnvFilePath().serialize());
		}
		return runConfig;
	}

	public static Map <String, String> makeRunConfig(BasePyBinaryFnParams <?> params) {
		Map <String, String> runConfig = new HashMap <>();
		if (params.getParams().contains(HasPythonEnvFilePath.PYTHON_ENV_FILE_PATH)) {
			runConfig.put(PyCalcRunner.PY_PYTHON_ENV_FILE_PATH, params.getPythonEnvFilePath().serialize());
		}
		return runConfig;
	}

	public static Map <String, String> makeRunConfig(BasePyBuiltInFnParams <?> params) {
		return new HashMap <>();
	}

	/**
	 * Generate necessary sql clauses to call Table API to process UDF
	 *
	 * @param inTableName  the registered input table name
	 * @param functionName the registered function name
	 * @param outputCol    output column
	 * @param selectedCols selected columns
	 * @param reservedCols reserved columns
	 * @return the sql clause
	 */
	public static String generateUDFClause(
		final String inTableName, final String functionName,
		final String outputCol, final String[] selectedCols, final String[] reservedCols) {

		final String selectedColsStr = TableUtil.columnsToSqlClause(selectedCols);

		// if outputCol is found in reservedCols, the one in reservedCols will not be output
		final String[] cleanedReservedCols = Arrays.stream(reservedCols)
			.filter(d -> !d.equals(outputCol))
			.toArray(String[]::new);

		StringBuilder sb = new StringBuilder();
		if (cleanedReservedCols.length > 0) {
			sb.append(TableUtil.columnsToSqlClause(cleanedReservedCols)).append(", ");
		}
		sb.append(functionName).append("(").append(selectedColsStr).append(") as `").append(outputCol).append("`");
		return String.format("SELECT %s FROM %s", sb, inTableName);
	}

	/**
	 * Generate sql clause to call sqlQuery to process UDTF
	 *
	 * @param inTableName  the registered input table name
	 * @param functionName the registered function name
	 * @param outputCols   output columns
	 * @param selectedCols selected columns
	 * @param reservedCols reserved columns
	 * @return the sql clause
	 */
	public static String generateUDTFClause(
		final String inTableName, final String functionName,
		final String[] outputCols, final String[] selectedCols, final String[] reservedCols) {

		final String selectedColsStr = TableUtil.columnsToSqlClause(selectedCols);

		// always shade outputCols to avoid potential conflicts with selectedCols
		final String[] shadedOutputCols = Arrays.stream(outputCols)
			.map(d -> d + "_" + UUID.randomUUID().toString().replace("-", ""))
			.toArray(String[]::new);
		final String shadedOutputColsStr = TableUtil.columnsToSqlClause(shadedOutputCols);

		// if one of outputCols is found in reservedCols, the one in reservedCols will not be output
		final Set <String> outputColsSet = new HashSet <>(Arrays.asList(outputCols));
		final String[] cleanedReservedCols = Arrays.stream(reservedCols)
			.filter(d -> !outputColsSet.contains(d))
			.toArray(String[]::new);

		StringBuilder sb = new StringBuilder();
		if (cleanedReservedCols.length > 0) {
			sb.append(TableUtil.columnsToSqlClause(cleanedReservedCols)).append(", ");
		}
		for (int i = 0; i < outputCols.length; i += 1) {
			sb.append("`").append(shadedOutputCols[i]).append("` as `").append(outputCols[i]).append("`");
			if (i < outputCols.length - 1) {
				sb.append(", ");
			}
		}
		String selectClause = sb.toString();

		// generate the join clause
		final String joinTemplate = "SELECT %s FROM %s, LATERAL TABLE(%s(%s)) as T(%s)";

		final String[] finalOutputCols = ArrayUtils.addAll(cleanedReservedCols, shadedOutputCols);
		final String finalOutputColsStr = TableUtil.columnsToSqlClause(finalOutputCols);

		final String joinClause = String.format(joinTemplate,
			finalOutputColsStr, inTableName, functionName, selectedColsStr, shadedOutputColsStr);

		return String.format("SELECT %s FROM (%s)", selectClause, joinClause);
	}
}
