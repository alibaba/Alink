package com.alibaba.alink.operator.common.optim;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Parallel optimizer: including GD, SGD, LBFGS, OWLQN, NEWTON method. users can use any of above method by setting
 * optimizer.
 *
 */
public abstract class Optimizer {
    protected final DataSet<?> objFuncSet;
    protected final DataSet<Tuple3<Double, Double, Vector>> trainData;
    protected final Params params;
    protected DataSet<Integer> coefDim;
    protected DataSet<DenseVector> coefVec = null;

    /**
     * construct function.
     *
     * @param objFunc   object function, calc loss and grad.
     * @param trainData data for training.
     * @param coefDim   the dimension of features.
     * @param params    some parameters of optimization method.
     */
    public Optimizer(DataSet<OptimObjFunc> objFunc, DataSet<Tuple3<Double, Double, Vector>> trainData,
                     DataSet<Integer> coefDim, Params params) {
        this.objFuncSet = objFunc;
        this.trainData = trainData;
        this.coefDim = coefDim;
        this.params = params;
    }

    /**
     * optimizer api.
     *
     * @return the coefficient of problem.
     */
    public abstract DataSet<Tuple2<DenseVector, double[]>> optimize();

    /**
     * initialize coefficient with zero
     */
    public void initCoefZeros() {
        if (null != this.coefVec) {
            this.coefVec = this.coefVec.map(new MapFunction<DenseVector, DenseVector>() {
                @Override
                public DenseVector map(DenseVector denseVector) throws Exception {
                    for (int i = 0; i < denseVector.size(); i++) {
                        denseVector.set(i, 0.0);
                    }
                    denseVector.set(0, 1.0e-3);
                    return denseVector;
                }
            });
        } else if (null != coefDim) {
            this.coefVec = this.coefDim.map(new MapFunction<Integer, DenseVector>() {
                @Override
                public DenseVector map(Integer n) throws Exception {
                    DenseVector denseVector = new DenseVector(n);
                    for (int i = 0; i < denseVector.size(); i++) {
                        denseVector.set(i, 0.0);
                    }
                    denseVector.set(0, 1.0e-3);
                    return denseVector;
                }
            });
        } else {
            throw new RuntimeException("Must input the coefficients dimension or initial coefficients!");
        }
    }

    public void initCoefWith(DataSet<DenseVector> initCoef) {
        this.coefVec = initCoef;
    }
}
