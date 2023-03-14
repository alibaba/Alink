package com.alibaba.alink.operator.common.optim.local;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.linear.BaseConstrainedLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.optim.FeatureConstraint;
import com.alibaba.alink.operator.common.optim.activeSet.ConstraintObjFunc;
import com.alibaba.alink.operator.common.optim.activeSet.Sqp;
import com.alibaba.alink.params.feature.HasConstraint;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConstrainedLocalOptimizer {

	//pass the param information into this function.
	public static Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> optimizeWithHessian(
		List <Tuple3 <Double, Double, Vector>> trainData,
		LinearModelType modelType,
		Params params) {
		int dim = trainData.get(0).f2.size();//the intercept has been added.
		ConstraintObjFunc objFunc = (ConstraintObjFunc) BaseConstrainedLinearModelTrainBatchOp.getObjFunction(modelType, null);
		String constraint = "";
		if (params.contains(HasConstraint.CONSTRAINT)) {
			constraint = params.get(HasConstraint.CONSTRAINT);
		}
		extractConstraintsForFeatureAndBin(
			FeatureConstraint.fromJson(constraint),
			objFunc, null, dim, true, null, null);
		double[] coefData = Sqp.phaseOne(
			objFunc.equalityConstraint.getArrayCopy2D(), objFunc.equalityItem.getData(),
			objFunc.inequalityConstraint.getArrayCopy2D(), objFunc.inequalityItem.getData(), dim);
		DenseVector coef = new DenseVector(coefData);
		Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> coefVector =
			LocalSqp.sqpWithHessian(trainData, coef, objFunc, params);
		return coefVector;
	}

	//count zero is only for default constraint of else and null
	public static void extractConstraintsForFeatureAndBin(FeatureConstraint constraint, ConstraintObjFunc objFunc,
														  String[] featureColNames, int dim, boolean hasInterceptItem,
														  DenseVector countZero, Map <String, Boolean> hasElse) {
		int size = constraint.getBinConstraintSize();
		Tuple4 <double[][], double[], double[][], double[]> cons;
		if (size != 0) {
			//for bin, this is only for bin.
			//todo:have not consider for features and bins.
			constraint.setCountZero(countZero);
			cons = constraint.getConstraintsForFeatureWithBin();
		} else {
			if (featureColNames == null) {
				//for feature in vector form.
				//if set
				if (hasInterceptItem) {
					dim -= 1;
				}
				cons = constraint.getConstraintsForFeatures(dim);
			} else {
				//for feature in table form.
				HashMap <String, Integer> featureIndex = new HashMap <>(featureColNames.length);
				for (int i = 0; i < featureColNames.length; i++) {
					featureIndex.put(featureColNames[i], i);
				}
				cons = constraint.getConstraintsForFeatures(featureIndex);
			}
		}
		if (hasInterceptItem) {
			addIntercept(cons);
		}
		objFunc.inequalityConstraint = new DenseMatrix(cons.f0);
		objFunc.inequalityItem = new DenseVector(cons.f1);
		objFunc.equalityConstraint = new DenseMatrix(cons.f2);
		objFunc.equalityItem = new DenseVector(cons.f3);
	}

	private static void addIntercept(Tuple4 <double[][], double[], double[][], double[]> cons) {
		cons.f0 = prefixMatrix(cons.f0);
		cons.f2 = prefixMatrix(cons.f2);
	}

	private static double[][] prefixMatrix(double[][] matrix) {
		int row = matrix.length;
		if (row == 0) {return matrix;}
		int col = matrix[0].length;
		for (int i = 0; i < row; i++) {
			matrix[i] = prefixRow(matrix[i], col);
		}
		return matrix;
	}

	private static double[] prefixRow(double[] row, int length) {
		double[] r = new double[length + 1];
		if (length >= 0) {System.arraycopy(row, 0, r, 1, length);}
		return r;
	}

	//    public Tuple4<DenseVector, DenseVector, DenseMatrix, Double> trainWithHessian(List<Tuple3<Double, Object,
	// Vector>> originTrainData,
	//                                                                                  LinearModelType modelType,
	// Params params) {
	//        boolean hasIntercept = params.get(LinearTrainParams.WITH_INTERCEPT);
	//        boolean standardization = params.get(LinearTrainParams.STANDARDIZATION);
	//        if (standardization && constraints != null) {
	//            throw new RuntimeException("standardization can not be applied for linear model with constraints!");
	//        }
	//
	//        boolean lr = !modelType.equals(LinearModelType.LinearReg);
	//        Tuple2<List<Tuple3<Double, Double, Vector>>, String[]> trainDataItem = LocalLogistRegression
	// .getLabelValues(originTrainData, lr);
	//        List<Tuple3<Double, Double, Vector>> trainData = trainDataItem.f0;
	//        preProcess(trainData, hasIntercept, standardization);
	//        //labels[0] is the positive label.
	//        String[] labels = trainDataItem.f1;
	//        Tuple4<DenseVector, DenseVector, DenseMatrix, Double> coefVectorSet = optimizeWithHessian(trainData,
	// modelType, params);
	//        return coefVectorSet;
	//    }

	//    public DenseVector train(List<Tuple3<Double, Object, Vector>> originTrainData,
	//                             LinearModelType modelType, Params params) {
	//        return trainWithHessian(originTrainData, modelType, params).f0;
	//    }

	//    public Tuple4<DenseVector, DenseVector, DenseMatrix, Double> trainWithHessian(List<Row> trainData,
	//                                                                                  List<Double> sampleWeight,
	//                                                                                  List<Object> label,
	//                                                                                  int[] indices,//针对table的输入而言。
	//                                                                                  LinearModelType modelType,
	//                                                                                  OptimMethod optimMethod,
	//                                                                                  boolean hasIntercept,
	//                                                                                  boolean standardization,//先不管
	//                                                                                  double l1,//先不考虑
	//                                                                                  double l2) {
	//        Params optParams = new Params()
	//            .set(LinearTrainParams.CONS_SEL_OPTIM_METHOD, optimMethod.name())
	//            .set(LinearTrainParams.WITH_INTERCEPT, hasIntercept)
	//            .set(LinearTrainParams.STANDARDIZATION, standardization)
	//            .set(HasL1.L_1, l1)
	//            .set(HasL2.L_2, l2);
	//        List<Tuple3<Double, Object, Vector>> vectorData = concatTrainData(trainData, sampleWeight, label,
	// indices);
	//        return trainWithHessian(vectorData, modelType, optParams);
	//    }

	//
	//    private static List<Tuple3<Double, Object, Vector>> concatTrainData(List<Row> trainData,
	//                                                                        List<Double> sampleWeight,
	//                                                                        List<Object> label,
	//                                                                        int[] indices) {
	//        int sampleNum = label.size();
	//        int dim = indices.length;
	//        List<Tuple3<Double, Object, Vector>> res = new ArrayList<>(sampleNum);
	//        for (int i = 0; i < sampleNum; i++) {
	//            DenseVector data = new DenseVector(dim);
	//            for (int j = 0; j < dim; j++) {
	//                data.set(j, (double) trainData.get(i).getField(indices[j]));
	//                res.set(i, Tuple3.of(sampleWeight.get(i), label.get(i), data));
	//            }
	//        }
	//        return res;
	//    }

	@Deprecated
	public static void preProcess(List <Tuple3 <Double, Double, Vector>> trainData, boolean hasIntercept,
								  boolean standardization) {
		for (Tuple3 <Double, Double, Vector> data : trainData) {
			if (standardization) {
				//to add
				if (hasIntercept) {
					data.f2 = data.f2.prefix(1);
				}
			} else {
				//to add
				if (hasIntercept) {
					data.f2 = data.f2.prefix(1);
				}
			}
		}

	}

	//f0 of data is weight, here replace it with predict label.
	public List <Tuple3 <Double, Double, Vector>> predict(List <Tuple3 <Double, Double, Vector>> data,
														  LinearModelType modelType,
														  DenseVector coef, Params params) {
		boolean hasIntercept = params.get(LinearTrainParams.WITH_INTERCEPT);
		boolean standardization = params.get(LinearTrainParams.STANDARDIZATION);
		preProcess(data, hasIntercept, standardization);
		if (modelType.equals(LinearModelType.LinearReg)) {
			for (Tuple3 <Double, Double, Vector> value : data) {
				value.f0 = value.f2.dot(coef);
			}
		} else {
			for (Tuple3 <Double, Double, Vector> value : data) {
				value.f0 = value.f2.dot(coef) > 0 ? 1. : 0.;
			}
		}
		return data;
	}

}
