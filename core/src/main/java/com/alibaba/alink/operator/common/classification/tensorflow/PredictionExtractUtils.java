package com.alibaba.alink.operator.common.classification.tensorflow;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;

import java.util.List;
import java.util.Map;

class PredictionExtractUtils {

	/**
	 * Extract probabilities and label from tensor.
	 *
	 * @param tensor tensor
	 * @param sortedLabels sorted list of all labels
	 * @param predDetail     fill this map with labels and corresponding probabilities
	 * @param isOutputLogits values in tensor are logits or probabilities
	 * @return prediction label
	 */
	static Object extractFromTensor(FloatTensor tensor, List <Object> sortedLabels, Map <Object, Double> predDetail,
									boolean isOutputLogits) {
		AkPreconditions.checkState(tensor.shape().length <= 1, "The prediction tensor must be rank-0 or rank-1");

		Object predLabel;
		// If the tensor has size 1, the model was trained for binary classification task,
		// and only output the predication probability to be positive.
		if (tensor.size() == 1) {
			double p = (tensor.shape().length == 0) ? tensor.getFloat() : tensor.getFloat(0);
			if (isOutputLogits) {
				p = 1. / (1 + Math.exp(-p));
			}
			Object negLabel = sortedLabels.get(0);
			Object posLabel = sortedLabels.get(1);
			predLabel = p >= 0.5 ? posLabel : negLabel;
			predDetail.put(posLabel, p);
			predDetail.put(negLabel, 1 - p);
			return predLabel;
		}

		int maxi = 0;
		if (isOutputLogits) {
			double[] p = new double[sortedLabels.size()];
			double sum_exp = 0.;
			for (int i = 0; i < sortedLabels.size(); i += 1) {
				p[i] = Math.exp(tensor.getFloat(i));
				sum_exp += p[i];
			}
			for (int i = 0; i < sortedLabels.size(); i += 1) {
				p[i] /= sum_exp;
				predDetail.put(sortedLabels.get(i), p[i]);
				if (p[i] > p[maxi]) {
					maxi = i;
				}
			}
		} else {
			for (int i = 0; i < sortedLabels.size(); i += 1) {
				double p = tensor.getFloat(i);
				predDetail.put(sortedLabels.get(i), p);
				if (p > tensor.getFloat(maxi)) {
					maxi = i;
				}
			}
		}
		predLabel = sortedLabels.get(maxi);
		return predLabel;
	}
}
