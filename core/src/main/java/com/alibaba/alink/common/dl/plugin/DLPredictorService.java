package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.dl.plugin.DLPredictServiceMapper.PredictorConfig;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface DLPredictorService {

	// only used by TF now
	@Deprecated
	default void open(Map <String, Object> config) {
		throw new AkUnsupportedOperationException("Not implement exception. ");
	}

	default void open(PredictorConfig config) {
		throw new AkUnsupportedOperationException("Not implement exception. ");
	}

	void close();

	/**
	 * Predict with values representing input tensors and produce values representing output tensors. Each element in
	 * the inputs and outputs represents a {@link org.tensorflow.Tensor} with the batch dimension as the 1st dimension.
	 * <p>
	 * The orders and types of elements are dependent on implementations.
	 *
	 * @param inputs values representing input tensors.
	 * @return prediction results.
	 */
	List <?> predict(List <?> inputs);

	/**
	 * Predict with values representing input tensors and produce values representing output tensors. Each element in
	 * the inputs and outputs is a list of values with every value represents a {@link org.tensorflow.Tensor} without
	 * the batch dimension, and the list together represents a tensor with batch dimension.
	 * <p>
	 * The orders and types of elements are dependent on implementations.
	 *
	 * @param inputs
	 * @param batchSize
	 * @return
	 */
	List <List <?>> predictRows(List <List <?>> inputs, int batchSize);

	/**
	 * Predict with values representing input tensors and produce values representing output tensors. Each element in
	 * the inputs and outputs represents a {@link org.tensorflow.Tensor} without the batch dimension.
	 *
	 * @param inputs
	 * @return
	 */
	default List <?> predictRow(List <?> inputs) {
		List <List <?>> valueList = inputs.stream()
			.map(Collections::singletonList)
			.collect(Collectors.toList());
		List <List <?>> outputValueList = predictRows(valueList, 1);
		return outputValueList.stream()
			.map(d -> d.get(0))
			.collect(Collectors.toList());
	}
}
