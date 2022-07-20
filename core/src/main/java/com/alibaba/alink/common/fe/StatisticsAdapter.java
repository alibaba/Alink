package com.alibaba.alink.common.fe;

import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class StatisticsAdapter<T> implements JsonSerializer <T>,
	JsonDeserializer <T> {

	private static final String CLASS_NAME = "CLASS_NAME";
	private static final String INSTANCE = "INSTANCE";

	private static Gson gson = new GsonBuilder()
		.serializeNulls()
		.disableHtmlEscaping()
		.serializeSpecialFloatingPointValues()
		.create();

	@Override
	public T deserialize(JsonElement json, Type typeOfT,
						 JsonDeserializationContext context)  {
		JsonObject jsonObject = json.getAsJsonObject();
		JsonPrimitive prim = (JsonPrimitive) jsonObject.get(CLASS_NAME);
		String className = prim.getAsString();

		Class <?> klass = null;
		try {
			klass = Class.forName(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new AkIllegalStateException(e.getMessage());
		}
		return (T) gson.fromJson(gson.toJson(jsonObject.get(INSTANCE)), klass);
	}

	@Override
	public JsonElement serialize(T src, Type type,
								 JsonSerializationContext context) {
		JsonObject retValue = new JsonObject();
		String className = src.getClass().getName();

		retValue.addProperty(CLASS_NAME, className);
		JsonElement elem = gson.toJsonTree(src);
		retValue.add(INSTANCE, elem);
		return retValue;
	}
}
