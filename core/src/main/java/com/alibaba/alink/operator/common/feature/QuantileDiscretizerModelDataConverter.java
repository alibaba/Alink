package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.google.common.reflect.TypeToken;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * quantile discretizer model data converter.
 */
public class QuantileDiscretizerModelDataConverter
	extends SimpleModelDataConverter<QuantileDiscretizerModelDataConverter, QuantileDiscretizerModelDataConverter> {
	public final static ParamInfo<String> VERSION = ParamInfoFactory
		.createParamInfo("version", String.class)
		.setHasDefaultValue("v1")
		.build();

	public Params meta;
	public Map<String, ContinuousRanges> data;

	public QuantileDiscretizerModelDataConverter() {
		this(new HashMap<>(), new Params());
	}

	public QuantileDiscretizerModelDataConverter(Map<String, ContinuousRanges> data, Params meta) {
		this.data = data;
		this.meta = meta;
		this.meta.set(VERSION, "v2");
	}

	@Override
	public Tuple2<Params, Iterable<String>> serializeModel(QuantileDiscretizerModelDataConverter modelData) {
		return Tuple2.of(meta, new ModelSerializeIterable());
	}

	@Override
	public QuantileDiscretizerModelDataConverter deserializeModel(Params meta, Iterable<String> data) {
		this.meta = meta;

		if (meta.get(VERSION).trim().equalsIgnoreCase("v1")) {
			String json = data.iterator().next();
			Map<String, Double[]> oldData = gson.fromJson(
				json,
				new TypeToken<HashMap<String, Double[]>>() {
				}.getType()
			);

			this.data = new HashMap<>();

			for (Map.Entry<String, Double[]> d : oldData.entrySet()) {
				this.data.put(
					d.getKey(),
					arraySplit2ContinuousRanges(d.getKey(), Types.DOUBLE, d.getValue(), true)
				);
			}

			return this;
		}

		for (String d : data) {
			ContinuousRanges featureInterval = ContinuousRanges.deSerialize(d);
			this.data.put(featureInterval.featureName, featureInterval);
		}

		return this;
	}

	public int getFeatureSize(String featureName) {
		ContinuousRanges featureInterval = this.data.get(featureName);

		if (featureInterval == null) {
			return 0;
		}

		return featureInterval.getIntervalNum();
	}

	public int missingIndex(String featureName) {
		ContinuousRanges featureInterval = this.data.get(featureName);

		if (featureInterval == null) {
			return 0;
		}

		return featureInterval.getIntervalNum();
	}

	public double getFeatureValue(String featureName, int index) {
		ContinuousRanges featureInterval = data.get(featureName);
		Preconditions.checkNotNull(featureInterval);
		return featureInterval.splitsArray[index].doubleValue();
	}

	public static ContinuousRanges arraySplit2ContinuousRanges(
		String featureName,
		TypeInformation<?> featureType,
		Number[] splits,
		boolean leftOpen) {
		return new ContinuousRanges(featureName, featureType,
			splits, leftOpen);
	}

	class ModelSerializeIterable implements Iterable<String> {

		@Override
		public Iterator<String> iterator() {
			return new ModelSerializeIterator();
		}
	}

	class ModelSerializeIterator implements Iterator<String> {
		Iterator<Map.Entry<String, ContinuousRanges>> mapIter
			= QuantileDiscretizerModelDataConverter.this.data.entrySet().iterator();

		@Override
		public boolean hasNext() {
			return mapIter.hasNext();
		}

		@Override
		public String next() {
			return ContinuousRanges.serialize(mapIter.next().getValue());
		}
	}


}