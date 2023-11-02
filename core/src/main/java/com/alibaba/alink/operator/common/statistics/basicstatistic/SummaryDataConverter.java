package com.alibaba.alink.operator.common.statistics.basicstatistic;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * summary data converter.
 */
public class SummaryDataConverter extends SimpleModelDataConverter <TableSummary, TableSummary> {
	/**
	 * default constructor.
	 */
	public SummaryDataConverter() {
	}

	@Override
	public Tuple2 <Params, Iterable <String>> serializeModel(TableSummary summary) {
		List <String> data = null;
		if (summary != null) {
			data = new ArrayList <>();
			data.add(JsonConverter.toJson(summary.colNames));
			data.add(String.valueOf(summary.count));
			data.add(JsonConverter.toJson(summary.numericalColIndices));
			if (summary.count() != 0) {
				data.add(longVectorToString(summary.numMissingValue));
				data.add(VectorUtil.toString(summary.sum));
				data.add(VectorUtil.toString(summary.sum2));
				data.add(VectorUtil.toString(summary.sum3));
				data.add(VectorUtil.toString(summary.sum4));
				data.add(VectorUtil.toString(summary.minDouble));
				data.add(VectorUtil.toString(summary.maxDouble));
				data.add(VectorUtil.toString(summary.normL1));
				data.add(objectVectorToString(summary.min));
				data.add(objectVectorToString(summary.max));
			}
		}

		return Tuple2.of(new Params(), data);
	}

	/**
	 * Deserialize the model data.
	 *
	 * @param meta The model meta data.
	 * @param data The model concrete data.
	 * @return The deserialized model data.
	 */
	@Override
	public TableSummary deserializeModel(Params meta, Iterable <String> data) {
		if (data == null) {
			return null;
		}

		Iterator <String> dataIterator = data.iterator();
		TableSummary summary = new TableSummary();
		summary.colNames = JsonConverter.fromJson(dataIterator.next(), String[].class);
		summary.count = Long.parseLong(dataIterator.next());
		summary.numericalColIndices = JsonConverter.fromJson(dataIterator.next(), int[].class);
		if (summary.count != 0) {
			summary.numMissingValue = stringToLongVector(dataIterator.next());
			if (dataIterator.hasNext()) {
				summary.sum = VectorUtil.parseDense(dataIterator.next());
				summary.sum2 = VectorUtil.parseDense(dataIterator.next());
				summary.sum3 = VectorUtil.parseDense(dataIterator.next());
				summary.sum4 = VectorUtil.parseDense(dataIterator.next());
				summary.minDouble = VectorUtil.parseDense(dataIterator.next());
				summary.maxDouble = VectorUtil.parseDense(dataIterator.next());
				summary.normL1 = VectorUtil.parseDense(dataIterator.next());
				summary.min = stringToObjectVector(dataIterator.next());
				summary.max = stringToObjectVector(dataIterator.next());
			}
		}

		return summary;
	}

	/**
	 * for serialize long vector.
	 */
	private static final char ELEMENT_DELIMITER = ' ';

	/**
	 * for serialize object vector.
	 */
	private static final String CLASS_NAME = "CLASS_NAME";

	/**
	 * for serialize object vector.
	 */
	private static final String INSTANCE = "INSTANCE";

	/**
	 * for serialize object vector.
	 */
	private final static Gson gson = new GsonBuilder()
		.serializeNulls()
		.disableHtmlEscaping()
		.serializeSpecialFloatingPointValues()
		.create();

	/**
	 * serialize long vector .
	 */
	static String longVectorToString(long[] longVector) {
		StringBuilder sbd = new StringBuilder();

		for (int i = 0; i < longVector.length; i++) {
			sbd.append(longVector[i]);
			if (i < longVector.length - 1) {
				sbd.append(ELEMENT_DELIMITER);
			}
		}
		return sbd.toString();
	}

	/**
	 * deserialize long vector .
	 */
	static long[] stringToLongVector(String longVecStr) {
		String[] longSVector = StringUtils.split(longVecStr, ELEMENT_DELIMITER);
		long[] longVec = new long[longSVector.length];
		for (int i = 0; i < longSVector.length; i++) {
			longVec[i] = Long.parseLong(longSVector[i]);
		}
		return longVec;
	}

	/**
	 * serialize object vector.
	 * If JsonConvertor.toJson then JsonConvertor.fromJson(xxx, Object[].class),
	 * timestamp will convert to long, decimal will convert to double. so save class type when tojson.
	 */
	static String objectVectorToString(Object[] vec) {
		if (null != vec && vec.length == 0) {
			return "";
		}
		JsonObject[] ojs = new JsonObject[vec.length];
		for (int i = 0; i < vec.length; i++) {
			Object src = vec[i];
			if (src != null) {
				JsonObject retValue = new JsonObject();
				String className = src.getClass().getName();

				retValue.addProperty(CLASS_NAME, className);
				JsonElement elem = gson.toJsonTree(src);
				retValue.add(INSTANCE, elem);

				ojs[i] = retValue;
			}
		}

		return gson.toJson(ojs);

	}

	/**
	 * deserialize object vector.
	 */
	static Object[] stringToObjectVector(String vecJson) {
		if (null != vecJson && vecJson.isEmpty()) {
			return new Object[0];
		}
		JsonElement[] jsonElements = gson.fromJson(vecJson, JsonElement[].class);
		Object[] values = new Object[jsonElements.length];
		for (int i = 0; i < jsonElements.length; i++) {
			JsonElement jsonElement = jsonElements[i];
			if (jsonElement instanceof JsonObject) {
				JsonObject jsonObject = jsonElement.getAsJsonObject();
				JsonPrimitive prim = (JsonPrimitive) jsonObject.get(CLASS_NAME);
				String className = prim.getAsString();

				Class <?> klass = null;
				try {
					klass = Class.forName(className);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
					throw new AkIllegalStateException(e.getMessage());
				}
				values[i] = gson.fromJson(gson.toJson(jsonObject.get(INSTANCE)), klass);
			}
		}
		return values;
	}
}
