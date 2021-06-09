package com.alibaba.alink.operator.common.linear;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
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
 */
public class SoftmaxModelMapper extends RichModelMapper {

	private static final long serialVersionUID = -4309479266141950255L;
	protected int vectorColIndex = -1;
	private LinearModelData model;
	private String vectorColName;
	private int[] featureIdx;
	private transient ThreadLocal <DenseVector> threadLocalVec;

	public SoftmaxModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		if (null != params) {
			this.vectorColName = params.get(SoftmaxPredictParams.VECTOR_COL);
			if (null != vectorColName && vectorColName.length() != 0) {
				this.vectorColIndex = TableUtil.findColIndexWithAssert(dataSchema.getFieldNames(), vectorColName);
			}
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		LinearModelDataConverter softMaxConverter
			= new LinearModelDataConverter();
		model = softMaxConverter.load(modelRows);
		TableSchema dataSchema = getDataSchema();
		if (vectorColIndex == -1) {
			if (this.model.featureNames != null) {
				int featureN = this.model.featureNames.length;
				this.featureIdx = new int[featureN];
				String[] predictTableColNames = dataSchema.getFieldNames();
				for (int i = 0; i < featureN; i++) {
					this.featureIdx[i] = TableUtil.findColIndexWithAssert(predictTableColNames,
						this.model.featureNames[i]);
				}
				threadLocalVec =
					ThreadLocal.withInitial(() -> new DenseVector(featureN + (model.hasInterceptItem ? 1 : 0)));
			} else {
				this.vectorColName = model.vectorColName;
				vectorColIndex = TableUtil.findColIndexWithAssert(dataSchema.getFieldNames(), model.vectorColName);
			}
		}
	}

	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		return predictResultDetail(selection).f0;
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception {
		Object predResult;
		String jsonDetail;

		Vector vec;
		if (vectorColIndex != -1) {
			vec = FeatureLabelUtil.getVectorFeature(selection.get(vectorColIndex), model.hasInterceptItem, model.vectorSize);
		} else {
			vec = threadLocalVec.get();
			selection.fillDenseVector((DenseVector) vec, model.hasInterceptItem, featureIdx);
		}


		Tuple2 <Object, Double[]> result = predictSoftmaxWithProb(vec);
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
			probs[k] = Math.exp(FeatureLabelUtil.dot(vector, coefVectors[k]));
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
