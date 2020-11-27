package com.alibaba.alink.operator.common.classification;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.classification.NaiveBayesTextTrainParams;
import com.alibaba.alink.params.classification.NaiveBayesTextTrainParams.ModelType;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class NaiveBayesTextModelDataConverter extends
	LabeledModelDataConverter <NaiveBayesTextModelData, NaiveBayesTextModelData> {

	public NaiveBayesTextModelDataConverter() {
	}

	public NaiveBayesTextModelDataConverter(TypeInformation labelType) {
		super(labelType);
	}

	/**
	 * Deserialize the model data.
	 *
	 * @param meta           The model meta data.
	 * @param data           The model data.
	 * @param distinctLabels The labels.
	 * @return The model data used by mapper.
	 */
	@Override
	public NaiveBayesTextModelData deserializeModel(Params meta, Iterable <String> data,
													Iterable <Object> distinctLabels) {
		NaiveBayesTextModelData modelData = new NaiveBayesTextModelData();
		String json = data.iterator().next();
		NaiveBayesTextProbInfo dataInfo = JsonConverter.fromJson(json, NaiveBayesTextProbInfo.class);
		modelData.pi = dataInfo.piArray;
		modelData.theta = dataInfo.theta;
		modelData.vectorSize = dataInfo.vectorSize;
		modelData.labels = Iterables.toArray(distinctLabels, Object.class);
		modelData.vectorColName = meta.get(NaiveBayesTextTrainParams.VECTOR_COL);
		modelData.modelType = meta.get(NaiveBayesTextTrainParams.MODEL_TYPE);
		int featLen = modelData.theta.numCols();
		modelData.featureCols = meta.get(HasFeatureCols.FEATURE_COLS);

		int rowSize = modelData.theta.numRows();
		modelData.phi = new double[rowSize];
		modelData.minMat = new DenseMatrix(rowSize, featLen);
		//construct special model data for the bernoulli model.
		if (ModelType.Bernoulli.equals(modelData.modelType)) {
			for (int i = 0; i < rowSize; ++i) {
				for (int j = 0; j < featLen; ++j) {
					double tmp = Math.log(1 - Math.exp(modelData.theta.get(i, j)));
					modelData.phi[i] += tmp;
					modelData.minMat.set(i, j, modelData.theta.get(i, j) - tmp);
				}
			}
		}
		return modelData;
	}

	/**
	 * Serialize the model data to "Tuple3<Params, List<String>, List<Object>>".
	 *
	 * @param modelData The model data to serialize.
	 * @return The serialization result.
	 */
	@Override
	public Tuple3 <Params, Iterable <String>, Iterable <Object>> serializeModel(NaiveBayesTextModelData modelData) {
		Params meta = new Params()
			.set(NaiveBayesTextTrainParams.MODEL_TYPE,
				ParamUtil.searchEnum(NaiveBayesTextTrainParams.MODEL_TYPE, modelData.modelType.name()))
			.set(HasVectorCol.VECTOR_COL, modelData.vectorColName)
			.set(HasFeatureCols.FEATURE_COLS, modelData.featureColNames);
		NaiveBayesTextProbInfo data = new NaiveBayesTextProbInfo();
		data.piArray = modelData.pi;
		data.theta = modelData.theta;
		data.vectorSize = modelData.vectorSize;
		return Tuple3.of(meta, Collections.singletonList(JsonConverter.toJson(data)), Arrays.asList(modelData.labels));
	}

	public static class NaiveBayesTextProbInfo implements AlinkSerializable {
		/**
		 * the pi array.
		 */
		public double[] piArray = null;
		/**
		 * the probability matrix.
		 */
		public DenseMatrix theta;
		public int vectorSize;
	}
}


