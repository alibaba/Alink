package com.alibaba.alink.operator.common.linear;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.classification.LinearModelMapperParams;
import org.apache.commons.lang.NotImplementedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This mapper maps one sample to a sample with a predicted class label.
 */
public class LinearModelMapper extends RichModelMapper {

	private static final long serialVersionUID = -1820786486066749971L;
	private int vectorColIndex = -1;
	private LinearModelData model;
	private int[] featureIdx;
	private int featureN;
	private String vectorColName;
	private transient ThreadLocal <DenseVector> threadLocalVec;

	/**
	 * Constructor function.
	 *
	 * @param modelSchema the model schema.
	 * @param dataSchema  the data schema.
	 * @param params      the params.
	 */
	public LinearModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		if (null != params) {
			this.vectorColName = params.get(LinearModelMapperParams.VECTOR_COL);
			if (null != vectorColName && vectorColName.length() != 0) {
				this.vectorColIndex = TableUtil.findColIndexWithAssert(dataSchema.getFieldNames(), vectorColName);
			}
		}
	}

	/**
	 * Load model from the list of Row type data.
	 *
	 * @param modelRows the list of Row type data.
	 */
	@Override
	public void loadModel(List <Row> modelRows) {
		LinearModelDataConverter linearModelDataConverter
			= new LinearModelDataConverter(LinearModelDataConverter.extractLabelType(super.getModelSchema()));
		this.model = linearModelDataConverter.load(modelRows);
		if (vectorColIndex == -1) {
			TableSchema dataSchema = getDataSchema();
			if (this.model.featureNames != null) {
				this.featureN = this.model.featureNames.length;
				this.featureIdx = new int[this.featureN];
				String[] predictTableColNames = dataSchema.getFieldNames();
				for (int i = 0; i < this.featureN; i++) {
					this.featureIdx[i] = TableUtil.findColIndexWithAssert(predictTableColNames,
						this.model.featureNames[i]);
				}
				threadLocalVec =
					ThreadLocal.withInitial(() -> new DenseVector(featureN + (model.hasInterceptItem ? 1 : 0)));
			} else {
				this.vectorColName = model.vectorColName;
				vectorColIndex = TableUtil.findColIndexWithAssert(dataSchema.getFieldNames(), this.vectorColName);
			}
		}
	}

	public void loadModel(LinearModelData linearModelData) {
		this.model = new LinearModelData(linearModelData);
		if (vectorColIndex == -1) {
			TableSchema dataSchema = getDataSchema();
			if (linearModelData.featureNames != null) {
				this.featureN = this.model.featureNames.length;
				this.featureIdx = new int[this.featureN];
				String[] predictTableColNames = dataSchema.getFieldNames();
				for (int i = 0; i < this.featureN; i++) {
					this.featureIdx[i] = TableUtil.findColIndexWithAssert(predictTableColNames,
						this.model.featureNames[i]);
				}
			} else {
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
		String jsonDetail = null;

		Vector vec;
		if (vectorColIndex != -1) {
			vec = FeatureLabelUtil.getVectorFeature(selection.get(vectorColIndex), model.hasInterceptItem,
				model.vectorSize);
		} else {
			vec = threadLocalVec.get();
			selection.fillDenseVector((DenseVector) vec, model.hasInterceptItem, featureIdx);
		}

		if (model.linearModelType == LinearModelType.LR || model.linearModelType == LinearModelType.SVM) {
			Tuple2 <Object, Double[]> result = predictWithProb(vec);
			predResult = result.f0;
			Map <String, String> detail = new HashMap <>(0);
			int labelSize = model.labelValues.length;
			for (int i = 0; i < labelSize; ++i) {
				detail.put(model.labelValues[i].toString(), result.f1[i].toString());
			}
			jsonDetail = JsonConverter.toJson(detail);
		} else {
			predResult = predict(vec);
		}

		return new Tuple2 <>(predResult, jsonDetail);
	}

	/**
	 * Predict the label information.
	 */
	public Object predict(Vector vector) throws Exception {
		double dotValue = FeatureLabelUtil.dot(vector, model.coefVector);

		switch (model.linearModelType) {
			case LR:
			case SVM:
			case Perceptron:
				return dotValue >= 0 ? model.labelValues[0] : model.labelValues[1];
			case LinearReg:
			case SVR:
				return dotValue;
			default:
				throw new NoSuchMethodException("Not supported yet!");
		}
	}

	/**
	 * Predict the label information with the probability of each label.
	 */
	public Tuple2 <Object, Double[]> predictWithProb(Vector vector) {
		double dotValue = FeatureLabelUtil.dot(vector, model.coefVector);

		switch (model.linearModelType) {
			case LR:
			case SVM:
				double prob = sigmoid(dotValue);
				return new Tuple2 <>(dotValue >= 0 ? model.labelValues[0] : model.labelValues[1],
					new Double[] {prob, 1 - prob});
			default:
				throw new NotImplementedException("not support score or detail yet!");
		}
	}

	private double sigmoid(double val) {
		return 1 - 1.0 / (1.0 + Math.exp(val));
	}
}
