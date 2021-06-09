package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.dataproc.JsonValueParams;
import com.jayway.jsonpath.JsonPath;

import java.lang.reflect.Type;
import java.util.Arrays;

/**
 * the mapper of json extraction transform.
 */
public class JsonPathMapper extends Mapper {

	private static final long serialVersionUID = 2298589947480476734L;

	private final String[] jsonPaths;
	private final boolean skipFailed;
	private final Type[] outputColTypes;

	public JsonPathMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		jsonPaths = params.get(JsonValueParams.JSON_PATHS);
		skipFailed = params.get(JsonValueParams.SKIP_FAILED);

		TypeInformation <?>[] outputTypes = ioSchema.f2;

		if (jsonPaths.length != outputTypes.length) {
			throw new IllegalArgumentException(
				"jsonPath and outputColName mismatch: " + jsonPaths.length + " vs " + outputTypes.length);
		}

		int numField = jsonPaths.length;
		outputColTypes = new Type[numField];

		for (int i = 0; i < numField; i++) {
			outputColTypes[i] = outputTypes[i].getTypeClass();
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {

		String[] outputColNames = params.get(JsonValueParams.OUTPUT_COLS);
		for (int i = 0; i < outputColNames.length; ++i) {
			outputColNames[i] = outputColNames[i].trim();
		}

		int numOutputField = outputColNames.length;

		TypeInformation <?>[] outputColTypes = new TypeInformation[numOutputField];

		if (params.contains(JsonValueParams.OUTPUT_COL_TYPES)) {
			String[] outputColTypeStrs = params.get(JsonValueParams.OUTPUT_COL_TYPES);

			for (int i = 0; i < numOutputField; i++) {
				outputColTypes[i] = FlinkTypeConverter.getFlinkType(
					outputColTypeStrs[i].trim().toUpperCase()
				);
			}
		} else {
			Arrays.fill(outputColTypes, Types.STRING);
		}

		return Tuple4.of(
			new String[] {params.get(JsonValueParams.SELECTED_COL)},
			outputColNames,
			outputColTypes,
			params.get(JsonValueParams.RESERVED_COLS)
		);
	}

	@Override
	public void map(SlicedSelectedSample selection, SlicedResult result) {
		String json = (String) selection.get(0);
		if (StringUtils.isNullOrWhitespaceOnly(json)) {
			if (skipFailed) {
				for (int idx = 0; idx < result.length(); ++idx) {
					result.set(idx, null);
				}
			} else {
				throw new RuntimeException("empty json string");
			}
		} else {
			for (int i = 0; i < jsonPaths.length; i++) {
				if (outputColTypes[i].equals(Types.STRING.getTypeClass())) {
					try {
						Object obj = JsonPath.read(json, jsonPaths[i]);
						if (!(obj instanceof String)) {
							obj = (obj == null) ? null : JsonConverter.toJson(obj);
						}
						result.set(i, obj);
					} catch (Exception ex) {
						if (skipFailed) {
							result.set(i, null);
						} else {
							throw new IllegalStateException("Fail to get json path: " + ex);
						}
					}
				} else {
					try {
						result.set(i,
							JsonConverter.fromJson(
								JsonConverter.toJson(JsonPath.read(json, jsonPaths[i])),
								outputColTypes[i]
							)
						);
					} catch (Exception ex) {
						if (skipFailed) {
							result.set(i, null);
						} else {
							throw new IllegalStateException("Fail to get json path: " + ex);
						}
					}
				}
			}
		}
	}
}
