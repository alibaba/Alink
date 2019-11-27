package com.alibaba.alink.operator.common.classification.ann;

/**
 * Layer properties of affine transformations, that is y=A*x+b
 */
public class AffineLayer extends Layer {
	public int numIn;
	public int numOut;

	public AffineLayer(int numIn, int numOut) {
		this.numIn = numIn;
		this.numOut = numOut;
	}

	@Override
	public LayerModel createModel() {
		return new AffineLayerModel(this);
	}

	@Override
	public int getWeightSize() {
		return numIn * numOut + numOut;
	}

	@Override
	public int getOutputSize(int inputSize) {
		return numOut;
	}

	@Override
	public boolean isInPlace() {
		return false;
	}
}
