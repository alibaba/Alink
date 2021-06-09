package com.alibaba.alink.operator.common.regression;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.linear.AftRegObjFunc;
import com.alibaba.alink.operator.common.linear.FeatureLabelUtil;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.params.classification.LinearModelMapperParams;
import com.alibaba.alink.params.regression.AftRegPredictParams;

import java.util.List;

/**
 * Accelerated Failure Time Survival Regression. Based on the Weibull distribution of the survival time.
 * <p>
 * (https://en.wikipedia.org/wiki/Accelerated_failure_time_model)
 */
public class AFTModelMapper extends RichModelMapper {

	private static final long serialVersionUID = 984867877738156476L;
	private int vectorColIndex = -1;
	private final double[] quantileProbabilities;
	private LinearModelData model;
	private int[] featureIdx;
	private transient ThreadLocal <DenseVector> threadLocalVec;

	/**
	 * Constructor.
	 *
	 * @param modelSchema the model schema.
	 * @param dataSchema  the data schema.
	 * @param params      the params.
	 */
	public AFTModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		this.quantileProbabilities = params.get(AftRegPredictParams.QUANTILE_PROBABILITIES);
		String vectorColName = params.get(LinearModelMapperParams.VECTOR_COL);
		if (null != vectorColName && vectorColName.length() != 0) {
			this.vectorColIndex = TableUtil.findColIndexWithAssert(dataSchema.getFieldNames(), vectorColName);
		}

	}

	/**
	 * Load model from the row type data.
	 *
	 * @param modelRows the list of Row type data
	 */
	@Override
	public void loadModel(List <Row> modelRows) {
		this.model = new LinearModelDataConverter(LinearModelDataConverter.extractLabelType(super.getModelSchema()))
			.load(modelRows);
		if (vectorColIndex == -1) {
			TableSchema dataSchema = getDataSchema();
			if (this.model.featureNames != null) {
				int featureN = this.model.featureNames.length;
				this.featureIdx = new int[featureN];
				threadLocalVec =
					ThreadLocal.withInitial(() -> new DenseVector(featureN + (model.hasInterceptItem ? 1 : 0)));
				String[] predictTableColNames = dataSchema.getFieldNames();
				for (int i = 0; i < featureN; i++) {
					this.featureIdx[i] = TableUtil.findColIndexWithAssert(predictTableColNames,
						this.model.featureNames[i]);
				}
			} else {
				vectorColIndex = TableUtil.findColIndexWithAssert(dataSchema.getFieldNames(), model.vectorColName);
			}
		}
	}

	/**
	 * Predict the result.
	 *
	 * @param selection the predict data.
	 * @return the predict result.
	 */
	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		return predictResultDetail(selection).f0;
	}

	/**
	 * Predict the result with detailed information.
	 *
	 * @param selection the predict data.
	 * @return the predict result with detailed information.
	 */
	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception {
		Vector vec;
		if (vectorColIndex != -1) {
			vec = FeatureLabelUtil.getVectorFeature(selection.get(vectorColIndex), model.hasInterceptItem,
				model.vectorSize);
		} else {
			vec = threadLocalVec.get();
			selection.fillDenseVector((DenseVector) vec, model.hasInterceptItem, featureIdx);
		}

		double[] data = model.coefVector.getData();
		double scale = data[data.length - 1];
		double[] res = new double[quantileProbabilities.length];
		double dot = Math.exp(AftRegObjFunc.getDotProduct(vec, model.coefVector));
		if (dot == Double.POSITIVE_INFINITY) {
			dot = Double.MAX_VALUE;
		}
		for (int i = 0; i < quantileProbabilities.length; i++) {
			res[i] = dot * Math.exp(Math.log(-Math.log(1 - quantileProbabilities[i])) * scale);
		}
		return Tuple2.of(dot, new DenseVector(res).toString());
	}
}
