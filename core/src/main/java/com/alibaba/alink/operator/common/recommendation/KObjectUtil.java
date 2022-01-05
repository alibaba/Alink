package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.utils.JsonConverter;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class KObjectUtil {
	public static final String OBJECT_NAME = "object";
	public static final String RATING_NAME = "rate";
	public static final String SCORE_NAME = "score";

	public static String serializeKObject(Map <String, List <Object>> kobject) {
		Map <String, String> result = new HashMap <>();

		for (Map.Entry <String, List <Object>> entry : kobject.entrySet()) {
			result.put(entry.getKey(), JsonConverter.toJson(entry.getValue()));
		}

		return JsonConverter.toJson(result);
	}

	public static Map <String, List <Object>> deserializeKObject(
		String json,
		String[] kObjectNames,
		Type[] kObjectTypes) {

		if (json == null || kObjectNames == null || kObjectTypes == null) {
			return null;
		}

		Map <String, String> deserializedJson;
		try {
			deserializedJson = JsonConverter.fromJson(
				json,
				new TypeReference <Map <String, String>>() {
				}.getType()
			);
		} catch (Exception e) {
			throw new IllegalStateException("Fail to deserialize json '" + json + "', please check the input!");
		}

		Map <String, String> lowerCaseDeserializedJson = new HashMap <>();

		for (Map.Entry <String, String> entry : deserializedJson.entrySet()) {
			lowerCaseDeserializedJson.put(entry.getKey().trim().toLowerCase(), entry.getValue());
		}

		Map <String, List <Object>> result = new HashMap <>();

		for (int i = 0; i < kObjectNames.length; ++i) {
			String lookUpResult = lowerCaseDeserializedJson.get(kObjectNames[i].trim().toLowerCase());

			if (lookUpResult == null) {
				result.put(kObjectNames[i], null);
			} else {
				result.put(
					kObjectNames[i],
					JsonConverter.fromJson(
						lookUpResult,
						ParameterizedTypeImpl.make(List.class, new Type[] {kObjectTypes[i]}, null)
					)
				);
			}
		}

		return result;
	}

	public static String serializeRecomm(
		String recommName,
		List <Object> recommValue,
		Map <String, List <Double>> others) {

		Map <String, String> result = new TreeMap <>((o1, o2) -> {
			if (o1.equals(recommName) && o2.equals(recommName)) {
				return 0;
			} else if (o1.equals(recommName)) {
				return -1;
			} else if (o2.equals(recommName)) {
				return 1;
			}

			return o1.compareTo(o2);
		});

		result.put(recommName, JsonConverter.toJson(recommValue));

		if (others != null) {
			for (Map.Entry <String, List <Double>> other : others.entrySet()) {
				result.put(other.getKey(), JsonConverter.toJson(other.getValue()));
			}
		}

		return JsonConverter.toJson(result);
	}

	public static MTable MergeRecommMTable(MTable recommJson, MTable initRecommJson) {
		List <Row> recommRows = recommJson.getRows();
		List <Row> initRows = initRecommJson.getRows();
		Set <Row> set = new HashSet <>(recommRows.size());
		for (Row row : recommRows) {
			set.add(Row.of(row.getField(0)));
		}
		for (Row row : initRows) {
			set.add(Row.of(row.getField(0)));
		}
		return new MTable(new ArrayList <>(set), recommJson.getSchemaStr().split(",")[0]);
	}
}
