package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.operator.common.feature.binning.Bin;
import com.alibaba.alink.operator.common.feature.binning.BinTypes;
import com.alibaba.alink.operator.common.feature.binning.BinningUtil;
import com.alibaba.alink.operator.common.feature.binning.FeatureBorder;
import com.google.common.reflect.TypeToken;

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
	public Map<String, FeatureBorder> data;

	public QuantileDiscretizerModelDataConverter() {
		this(new HashMap<>(), new Params());
	}

	public QuantileDiscretizerModelDataConverter(Map<String, FeatureBorder> data, Params meta) {
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
					arraySplit2FeatureBorder(d.getKey(), Types.DOUBLE, d.getValue(), true, BinTypes.BinDivideType.QUANTILE)
				);
			}

			return this;
		}

		for (String d : data) {
			FeatureBorder border = FeatureBorder.deSerialize(d)[0];

			this.data.put(border.featureName, border);
		}

		return this;
	}

	public int getFeatureSize(String featureName) {
		FeatureBorder modelData = this.data.get(featureName);

		if (modelData == null) {
			return 0;
		}

		int ret = 0;

		for (Bin.BaseBin bin : modelData.bin.normBins) {
			ret = Math.max(bin.getIndex().intValue(), ret);
		}

		return modelData.bin.normBins.size();
	}

	public int missingIndex(String featureName) {
		FeatureBorder modelData = this.data.get(featureName);

		if (modelData == null) {
			return 0;
		}

		return modelData.bin.nullBin.getIndex().intValue();
	}

	public double getFeatureValue(String featureName, int index) {
		FeatureBorder modelData = data.get(featureName);
		Preconditions.checkNotNull(modelData);
		return ((Number) modelData.bin.normBins.get(index).right().getRecord()).doubleValue();
	}

	public static FeatureBorder arraySplit2FeatureBorder(
		String featureName,
		TypeInformation<?> featureType,
		Number[] splits,
		boolean leftOpen,
		BinTypes.BinDivideType binDivideType) {
		return new FeatureBorder(binDivideType, featureName, featureType,
			BinningUtil.createNumericBin(splits, leftOpen), leftOpen);
	}

	class ModelSerializeIterable implements Iterable<String> {

		@Override
		public Iterator<String> iterator() {
			return new ModelSerializeIterator();
		}
	}

	class ModelSerializeIterator implements Iterator<String> {
		Iterator<Map.Entry<String, FeatureBorder>> mapIter
			= QuantileDiscretizerModelDataConverter.this.data.entrySet().iterator();

		@Override
		public boolean hasNext() {
			return mapIter.hasNext();
		}

		@Override
		public String next() {
			return FeatureBorder.serialize(mapIter.next().getValue());
		}
	}


}