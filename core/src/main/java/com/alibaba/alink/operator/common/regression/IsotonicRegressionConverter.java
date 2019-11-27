package com.alibaba.alink.operator.common.regression;

import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.common.model.SimpleModelDataConverter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Save the boundaries and values for Isotonic Regression.
 */
public class IsotonicRegressionConverter
	extends SimpleModelDataConverter <IsotonicRegressionModelData, IsotonicRegressionModelData> {

	/**
	 * Serialize the model data to "Tuple2<Params, Iterable<String>>".
	 *
	 * @param modelData The model data to serialize.
	 * @return The serialization result.
	 */
	@Override
	public Tuple2<Params, Iterable<String>> serializeModel(IsotonicRegressionModelData modelData) {
		Double[] boundaries = modelData.boundaries;
		Double[] values = modelData.values;
		Params meta = modelData.meta;
		List <String> data = new ArrayList <>();
		data.add(JsonConverter.toJson(boundaries));
		data.add(JsonConverter.toJson(values));
		return Tuple2.of(meta, data);
	}

	/**
	 * Deserialize the model data.
	 *
	 * @param meta         The model meta data.
	 * @param data         The model concrete data.
	 * @return The deserialized model data.
	 */
	@Override
	public IsotonicRegressionModelData deserializeModel(Params meta, Iterable<String> data) {
		IsotonicRegressionModelData modelData = new IsotonicRegressionModelData();
		Iterator<String> dataIterator = data.iterator();
		Double[] boundaries = JsonConverter.fromJson(dataIterator.next(), Double[].class);
		Double[] values = JsonConverter.fromJson(dataIterator.next(), Double[].class);
		modelData.boundaries = boundaries;
		modelData.values = values;
		modelData.meta = meta;
		return modelData;
	}
}
