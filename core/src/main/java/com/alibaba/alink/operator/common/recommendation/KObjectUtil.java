package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import com.alibaba.alink.common.utils.JsonConverter;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

		Map <String, String> result = new TreeMap <>(new Comparator <String>() {
			@Override
			public int compare(String o1, String o2) {
				if (o1.equals(recommName) && o2.equals(recommName)) {
					return 0;
				} else if (o1.equals(recommName)) {
					return -1;
				} else if (o2.equals(recommName)) {
					return 1;
				}

				return o1.compareTo(o2);
			}
		});

		result.put(recommName, JsonConverter.toJson(recommValue));

		if (others != null) {
			for (Map.Entry <String, List <Double>> other : others.entrySet()) {
				result.put(other.getKey(), JsonConverter.toJson(other.getValue()));
			}
		}

		return JsonConverter.toJson(result);
	}

	public static String MergeRecommJson(String recommName, String recommJson, String initRecommJson) {
		Map <String, String> initRecomm = JsonConverter.fromJson(
			initRecommJson,
			new TypeReference <Map <String, String>>() {
			}.getType()
		);
		Map <String, String> recomm = JsonConverter.fromJson(
			recommJson,
			new TypeReference <Map <String, String>>() {
			}.getType()
		);
		Map <String, String> result = new TreeMap <>();

		List <Object> initRecommList = JsonConverter.fromJson(initRecomm.get(recommName), List.class);
		List <Object> recommList = JsonConverter.fromJson(recomm.get(recommName), List.class);

		for (Object obj : initRecommList) {
			if (!recommList.contains(obj)) {
				recommList.add(obj);
			}
		}
		result.put(recommName, JsonConverter.toJson(recommList));

		return JsonConverter.toJson(result);

	}

	public static Tuple2 <List <Object>, Map <String, List <Double>>>
	deserializeRecomm(
		String recommJson, String recommKey, Type typeOfT) {

		Map <String, String> recomm = JsonConverter.fromJson(
			recommJson,
			new TypeReference <Map <String, String>>() {
			}.getType()
		);

		List <Object> recommVal = JsonConverter.fromJson(
			recomm.remove(recommKey),
			ParameterizedTypeImpl.make(List.class, new Type[] {typeOfT}, null)
		);

		Map <String, List <Double>> others = new HashMap <>();

		for (Map.Entry <String, String> entry : recomm.entrySet()) {
			others.put(
				entry.getKey(),
				JsonConverter.fromJson(
					entry.getValue(),
					new TypeReference <List <Double>>() {
					}.getType()
				)
			);
		}

		return Tuple2.of(recommVal, others);
	}
}
