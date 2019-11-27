package com.alibaba.alink.operator.common.linear.unarylossfunc;

import java.io.Serializable;

/**
 * Interface of unary loss function.
 * Each function has 2 inputs(eta and y), we take their difference as the unary variable of the loss function.
 * More information about loss function, please see：
 * 1. https://en.wikipedia.org/wiki/Loss_function
 * 2. https://en.wikipedia.org/wiki/Loss_functions_for_classification
 */
public interface UnaryLossFunc extends Serializable {

	/**
	 * Loss function.
	 *
	 * @param eta eta value.
	 * @param y   label value.
	 * @return loss value.
	 */
	double loss(double eta, double y);

	/**
	 * The derivative of loss function.
	 *
	 * @param eta eta value.
	 * @param y   label value.
	 * @return derivative value.
	 */
	double derivative(double eta, double y);

	/**
	 * The second derivative of the loss function.
	 *
	 * @param eta eta value.
	 * @param y   label value.
	 * @return second derivative value.
	 */
	double secondDerivative(double eta, double y);

}
