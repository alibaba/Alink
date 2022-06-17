package com.alibaba.alink.common.fe;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class BaseFeaturesAdapter<T> implements JsonSerializer <T>, JsonDeserializer <T> {

	private static final String CLASS_NAME = "CLASS_NAME";
	private static final String INSTANCE = "INSTANCE";

	@Override
	public JsonElement serialize(T src, Type typeOfSrc,
								 JsonSerializationContext context) {

		JsonObject retValue = new JsonObject();
		String className = src.getClass().getName();
		retValue.addProperty(CLASS_NAME, className);
		JsonElement elem = context.serialize(src);
		retValue.add(INSTANCE, elem);
		return retValue;
	}

	@Override
	public T deserialize(JsonElement json, Type typeOfT,
										JsonDeserializationContext context) throws JsonParseException {
		JsonObject jsonObject = json.getAsJsonObject();
		JsonPrimitive prim = (JsonPrimitive) jsonObject.get(CLASS_NAME);
		String className = prim.getAsString();

		Class <?> klass = null;
		try {
			klass = Class.forName(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new JsonParseException(e.getMessage());
		}
		return context.deserialize(jsonObject.get(INSTANCE), klass);
	}
}
