package com.alibaba.alink.operator.common.linear;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.classification.SoftmaxPredictParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * Softmax mapper maps one sample to a sample with a predicted class label.
 *
 */
public class SoftmaxModelMapper extends RichModelMapper {

	protected int vectorColIndex = -1;
	private LinearModelData model;
	private int[] featureIdx;
	private int featureN;

	public SoftmaxModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		if (null != params) {
			String vectorColName = params.get(SoftmaxPredictParams.VECTOR_COL);
			if (null != vectorColName && vectorColName.length() != 0) {
				this.vectorColIndex = TableUtil.findColIndex(dataSchema.getFieldNames(), vectorColName);
			}
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		LinearModelDataConverter softmaxConverter
			= new LinearModelDataConverter();
		model = softmaxConverter.load(modelRows);
		TableSchema dataSchema = getDataSchema();
		if (vectorColIndex == -1) {
			if (this.model.featureNames != null) {
				this.featureN = this.model.featureNames.length;
				this.featureIdx = new int[this.featureN];
				String[] predictTableColNames = dataSchema.getFieldNames();
				for (int i = 0; i < this.featureN; i++) {
					this.featureIdx[i] = TableUtil.findColIndex(predictTableColNames,
						this.model.featureNames[i]);
				}
			} else {
				vectorColIndex = TableUtil.findColIndex(dataSchema.getFieldNames(), model.vectorColName);
			}
		}
	}

	@Override
	protected Object predictResult(Row row) throws Exception {
		Vector aVector = FeatureLabelUtil.getFeatureVector(row, model.hasInterceptItem, this.featureN,
			this.featureIdx, this.vectorColIndex, model.vectorSize);

		DenseVector[] coefVectors = model.coefVectors;
		int lableSize = model.labelValues.length;

		double maxVal = 0.0;
		int maxIdx = lableSize - 1;

		double t;
		for (int k = 0; k < lableSize - 1; k++) {
			t = MatVecOp.dot(aVector, coefVectors[k]);
			if (t > maxVal) {
				maxVal = t;
				maxIdx = k;
			}
		}
		return model.labelValues[maxIdx];
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(Row row) throws Exception {
		Object predResult;
		String jsonDetail;

		Vector aVector = FeatureLabelUtil.getFeatureVector(row, model.hasInterceptItem, this.featureN,
			this.featureIdx,
			this.vectorColIndex, model.vectorSize);
		Tuple2 <Object, Double[]> result = predictSoftmaxWithProb(aVector);
		predResult = result.f0;
		Map <String, String> detail = new HashMap <>(0);
		int labelSize = model.labelValues.length;
		for (int i = 0; i < labelSize; ++i) {
			detail.put(model.labelValues[i].toString(), result.f1[i].toString());
		}
		jsonDetail = gson.toJson(detail);

		return new Tuple2 <>(predResult, jsonDetail);
	}

	private Tuple2 <Object, Double[]> predictSoftmaxWithProb(Vector vector) {
		DenseVector[] coefVectors = model.coefVectors;
		int k1 = model.labelValues.length;
		Double[] probs = new Double[k1];

		double maxVal = Double.NEGATIVE_INFINITY;
		int maxIdx = -1;

		double s = 1;
		for (int k = 0; k < k1 - 1; k++) {
			probs[k] = Math.exp(MatVecOp.dot(vector, coefVectors[k]));
			s += probs[k];
		}
		probs[k1 - 1] = 1.0;
		for (int k = 0; k < k1; k++) {
			probs[k] /= s;
			if (probs[k] > maxVal) {
				maxVal = probs[k];
				maxIdx = k;
			}
		}
		return new Tuple2 <>(model.labelValues[maxIdx], probs);
	}
}
