package com.alibaba.alink.operator.common.linear;

/**
 * Linear model type.
 */
public enum LinearModelType {
	LinearReg, // Linear Regression
	SVR,       // Support vector regression
	LR,        // Logistic Regression
	SVM,       // Support vector machine
	Perceptron,// Perceptron
	AFT        // Survival regression
}