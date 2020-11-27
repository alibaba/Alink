package com.alibaba.alink.operator.common.classification;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.classification.NaiveBayesModelInfo;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class NaiveBayesModelDataConverter
	extends LabeledModelDataConverter <NaiveBayesModelData, NaiveBayesModelData> {

	private ParamInfo <double[]> LABEL_WEIGHTS = ParamInfoFactory
		.createParamInfo("labelWeights", double[].class)
		.setDescription("the label weights.")
		.build();

	private ParamInfo <boolean[]> IS_CATE = ParamInfoFactory
		.createParamInfo("isCate", boolean[].class)
		.setDescription("judge whether the feature columns are categorical or not")
		.build();

	private ParamInfo <Integer> STRING_INDEXER_MODEL_SIZE = ParamInfoFactory
		.createParamInfo("stringIndexerModelSize", Integer.class)
		.setDescription("stringIndexerModelSize")
		.build();

	public NaiveBayesModelDataConverter() {
	}

	public NaiveBayesModelDataConverter(TypeInformation labelType) {
		super(labelType);
	}

	/**
	 * Serialize the model data .
	 *
	 * @param modelData The model data to serialize.
	 * @return The serialization result.
	 */
	@Override
	protected Tuple3 <Params, Iterable <String>, Iterable <Object>> serializeModel(NaiveBayesModelData modelData) {
		Params meta = new Params();
		meta.set(HasFeatureColsDefaultAsNull.FEATURE_COLS, modelData.featureNames);
		meta.set(IS_CATE, modelData.isCate);
		meta.set(LABEL_WEIGHTS, modelData.labelWeights);
		NaiveBayesProbInfo data = new NaiveBayesProbInfo();
		data.pi = modelData.piArray;
		data.theta = modelData.theta;

		List <String> serialized = new ArrayList <>();
		serialized.add(JsonConverter.toJson(data));
		serialized.add(JsonConverter.toJson(modelData.weightSum));
		serialized.add(JsonConverter.toJson(modelData.featureInfo));
		if (modelData.stringIndexerModelSerialized != null) {
			for (Row row : modelData.stringIndexerModelSerialized) {
				Object[] objs = new Object[row.getArity()];

				for (int i = 0; i < row.getArity(); ++i) {
					objs[i] = row.getField(i);
				}

				serialized.add(JsonConverter.toJson(objs));
			}
		}

		meta.set(STRING_INDEXER_MODEL_SIZE, serialized.size());
		return Tuple3.of(meta, serialized, Arrays.asList(modelData.label));
	}

	/**
	 * Deserialize the model data.
	 *
	 * @param meta           The model meta data.
	 * @param stringData     The model data.
	 * @param distinctLabels The labels.
	 * @return The model data used by mapper.
	 */
	@Override
	protected NaiveBayesModelData deserializeModel(Params meta, Iterable <String> stringData,
												   Iterable <Object> distinctLabels) {
		int stringIndexerModelSize = meta.get(STRING_INDEXER_MODEL_SIZE);
		NaiveBayesModelData modelData = new NaiveBayesModelData();
		modelData.stringIndexerModelSerialized = new ArrayList <>(stringIndexerModelSize);
		int count = 0;
		for (String stringDatum : stringData) {
			if (count == 0) {
				NaiveBayesProbInfo data = JsonConverter.fromJson(stringDatum, NaiveBayesProbInfo.class);
				modelData.piArray = data.pi;
				modelData.theta = data.theta;
			} else if (count == 1) {
				modelData.weightSum = JsonConverter.fromJson(stringDatum, double[][].class);
			} else if (count == 2) {
				modelData.featureInfo = JsonConverter.fromJson(stringDatum, SparseVector[][].class);
			} else {
				Object[] deserialized = JsonConverter.fromJson(stringDatum, Object[].class);
				modelData.stringIndexerModelSerialized.add(
					Row.of(
						((Integer) deserialized[0]).longValue(),
						deserialized[1],
						deserialized[2]
					)
				);
			}
			count++;
		}
		modelData.featureNames = meta.get(HasFeatureColsDefaultAsNull.FEATURE_COLS);
		modelData.isCate = meta.get(IS_CATE);
		modelData.labelWeights = meta.get(LABEL_WEIGHTS);
		modelData.label = Iterables.toArray(distinctLabels, Object.class);
		return modelData;
	}

	private static class NaiveBayesProbInfo implements AlinkSerializable {
		public Number[][][] theta;
		public double[] pi;
	}

	public NaiveBayesModelInfo loadModelInfo(List <Row> rows) {
		NaiveBayesModelInfo modelInfo = JsonConverter.fromJson((String) rows.get(0).getField(1),
			NaiveBayesModelInfo.class);
		modelInfo.stringIndexerModelSerialized = rows.subList(1, rows.size());
		return modelInfo;
	}
}
