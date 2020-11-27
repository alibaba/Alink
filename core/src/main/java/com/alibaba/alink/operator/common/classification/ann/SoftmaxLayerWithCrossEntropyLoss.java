package com.alibaba.alink.operator.common.classification.ann;

/**
 * Defines a softmax layer with cross entropy loss.
 */
public class SoftmaxLayerWithCrossEntropyLoss extends Layer {
	private static final long serialVersionUID = -7613371837892201560L;

	@Override
	public LayerModel createModel() {
		return new SoftmaxLayerModelWithCrossEntropyLoss();
	}

	@Override
	public int getWeightSize() {
		return 0;
	}

	@Override
	public int getOutputSize(int inputSize) {
		return inputSize;
	}

	@Override
	public boolean isInPlace() {
		return true;
	}
}
