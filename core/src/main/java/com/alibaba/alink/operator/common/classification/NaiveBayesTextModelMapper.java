package com.alibaba.alink.operator.common.classification;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkColumnNotFoundException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.classification.NaiveBayesTextPredictParams;
import com.alibaba.alink.params.classification.NaiveBayesTextTrainParams.ModelType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This mapper predicts sample label.
 */
public class NaiveBayesTextModelMapper extends RichModelMapper {
	private static final long serialVersionUID = -1418316110985603945L;
	public String[] colNames;
	public String vectorColName = null;
	public int vectorIndex;
	public NaiveBayesTextModelData modelData;

	/**
	 * Construct function.
	 *
	 * @param modelSchema serializer schema.
	 * @param dataSchema  data schema.
	 * @param params      parameters for predict.
	 */
	public NaiveBayesTextModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		this.colNames = dataSchema.getFieldNames();
		if (params.contains(NaiveBayesTextPredictParams.VECTOR_COL)) {
			this.vectorColName = params.get(NaiveBayesTextPredictParams.VECTOR_COL);
			vectorIndex = TableUtil.findColIndex(colNames, vectorColName);
			if (vectorIndex == -1) {
				throw new AkColumnNotFoundException("the predict vector is not in the predict data schema.");
			}
		}
	}

	/**
	 * Calculate the probability of each label and return the most possible one.
	 *
	 * @param selection the input data.
	 * @return the most possible label.
	 */
	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		Vector featVec = VectorUtil.getVector(selection.get(this.vectorIndex));
		double[] prob = calculateProb(featVec);
		return findMaxProbLabel(prob, modelData.labels);
	}

	/**
	 * Calculate the probability of each label and return the most possible one and the detail.
	 *
	 * @param selection the input data.
	 * @return the most possible label and the detail.
	 */
	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception {
		Vector featVec = VectorUtil.getVector(selection.get(this.vectorIndex));
		double[] prob = calculateProb(featVec);
		Object result = findMaxProbLabel(prob, modelData.labels);
		String jsonDetail = generateDetail(prob, modelData.pi, modelData.labels);
		return new Tuple2 <>(result, jsonDetail);
	}


	/**
	 * Calculate the probability that the input data belongs to each class in multinomial method.
	 *
	 * @param vec input data.
	 * @return the probability that the input data belongs to each class.
	 */
	private double[] multinomialCalculation(Vector vec) {
		int rowSize = modelData.theta.numRows();
		DenseVector prob = DenseVector.zeros(rowSize);
		DenseVector pi = new DenseVector(modelData.pi);
		if (vec instanceof DenseVector) {
			NaiveBayesBLASUtil.gemv(1, modelData.theta, (DenseVector) vec, 0, prob);
		} else {
			NaiveBayesBLASUtil.gemv(1, modelData.theta, (SparseVector) vec, 0, prob);
		}
		BLAS.axpy(1, pi, prob);
		return prob.getData();
	}

	/**
	 * Calculate the probability that the input data belongs to each class in bernoulli method.
	 *
	 * @param vec input data.
	 * @return the probability that the input data belongs to each class.
	 */
	private double[] bernoulliCalculation(Vector vec) {
		int rowSize = modelData.theta.numRows();
		int colSize = modelData.theta.numCols();
		DenseVector prob = DenseVector.zeros(rowSize);
		DenseVector pi = new DenseVector(modelData.pi);
		DenseVector phi = new DenseVector(modelData.phi);
		if (vec instanceof DenseVector) {
			DenseVector denseVec = (DenseVector) vec;
			for (int j = 0; j < colSize; ++j) {
				double value = denseVec.get(j);
				AkPreconditions.checkArgument(value == 0. || value == 1.,
					"Bernoulli naive Bayes requires 0 or 1 feature values.");
			}
			if (colSize < denseVec.size()) {
				double[] dvData = new double[colSize];
				System.arraycopy(denseVec.getData(), 0, dvData, 0, colSize);
				denseVec.setData(dvData);
			}

			NaiveBayesBLASUtil.gemv(1, modelData.minMat, denseVec, 0, prob);
		} else {
			SparseVector sparseVec = (SparseVector) vec;
			int[] indices = sparseVec.getIndices();
			double[] values = sparseVec.getValues();
			for (int i = 0; i < indices.length; i++) {
				int index = indices[i];
				if (index >= colSize) {
					break;
				}
				double value = values[i];
				AkPreconditions.checkArgument(value == 0. || value == 1.,
					"Bernoulli naive Bayes requires 0 or 1 feature values.");
			}
			NaiveBayesBLASUtil.gemv(1, modelData.minMat, sparseVec, 0, prob);
		}
		BLAS.axpy(1, pi, prob);
		BLAS.axpy(1, phi, prob);
		return prob.getData();
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		modelData = new NaiveBayesTextModelDataConverter().load(modelRows);
		vectorColName = modelData.vectorColName;
	}


	protected static String generateDetail(double[] prob, double[] pi, Object[] labels) {
		double maxProb = prob[0];
		for (int i = 1; i < prob.length; ++i) {
			if (maxProb < prob[i]) {
				maxProb = prob[i];
			}
		}
		double sumProb = 0.0;
		for (double probVal : prob) {
			sumProb += Math.exp(probVal - maxProb);
		}
		sumProb = maxProb + Math.log(sumProb);
		for (int i = 0; i < prob.length; ++i) {
			prob[i] = Math.exp(prob[i] - sumProb);
		}

		int labelSize = pi.length;
		Map <String, Double> detail = new HashMap <>(labelSize);
		for (int i = 0; i < labelSize; ++i) {
			detail.put(labels[i].toString(), prob[i]);
		}
		return JsonConverter.toJson(detail);
	}

	protected static Object findMaxProbLabel(double[] prob, Object[] label) {
		Object result = null;
		int probSize = prob.length;
		double maxVal = Double.NEGATIVE_INFINITY;
		for (int i = 0; i < probSize; ++i) {
			if (maxVal < prob[i]) {
				maxVal = prob[i];
				result = label[i];
			}
		}
		return result;
	}

	public double[] calculateProb(Vector featVec) {

		if (ModelType.Multinomial.equals(modelData.modelType)) {
			return multinomialCalculation(featVec);
		} else {
			return bernoulliCalculation(featVec);
		}
	}
}
