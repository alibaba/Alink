package com.alibaba.alink.common.linalg.tensor;

import org.tensorflow.ndarray.NdArray;

public abstract class NumericalTensor<T extends Number> extends Tensor <T> {

	NumericalTensor(NdArray <T> data, DataType type) {
		super(data, type);
	}

	public abstract NumericalTensor <?> min(int dim, boolean keepDim);

	public abstract NumericalTensor <?> max(int dim, boolean keepDim);

	public abstract NumericalTensor <?> sum(int dim, boolean keepDim);

	public abstract NumericalTensor <?> mean(int dim, boolean keepDim);
}
