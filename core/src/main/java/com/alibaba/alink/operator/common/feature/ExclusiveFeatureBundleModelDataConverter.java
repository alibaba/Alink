package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.model.RichModelDataConverter;

import java.util.ArrayList;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class ExclusiveFeatureBundleModelDataConverter extends RichModelDataConverter <FeatureBundles, FeatureBundles> {

	public String[] efbColNames;
	public TypeInformation[] efbColTypes;

	/**
	 * Constructor.
	 */
	public ExclusiveFeatureBundleModelDataConverter() {
	}

	/**
	 * Get the additional column names.
	 */
	@Override
	protected String[] initAdditionalColNames() {
		return efbColNames;
	}

	/**
	 * Get the additional column types.
	 */
	@Override
	protected TypeInformation[] initAdditionalColTypes() {
		return efbColTypes;
	}

	/**
	 * Serialize the model data to "Tuple3<Params, List<String>, List<Row>>".
	 *
	 * @return The serialization result.
	 */
	@Override
	public Tuple3 <Params, Iterable <String>, Iterable <Row>> serializeModel(FeatureBundles bundles) {
		return new Tuple3 <>(
			new Params().set("bundles_json", bundles.toJson()),
			new ArrayList <String>(),
			new ArrayList <Row>()
		);
	}

	/**
	 * Deserialize the model data.
	 *
	 * @param meta         The model meta data.
	 * @param data         The model concrete data.
	 * @param additionData The additional data.
	 * @return The deserialized model data.
	 */
	@Override
	public FeatureBundles deserializeModel(Params meta, Iterable <String> data, Iterable <Row> additionData) {
		return FeatureBundles.fromJson(meta.getString("bundles_json"));
	}
}
