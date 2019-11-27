package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * The OptimObjFunc for multilayer perceptron.
 */
public class AnnObjFunc extends OptimObjFunc {

    private Topology topology;
    private Stacker stacker;
    private transient TopologyModel topologyModel = null;

    public AnnObjFunc(Topology topology,
                      int inputSize, int outputSize, boolean oneHotLabel,
                      Params params) {
        super(params);
        this.topology = topology;
        this.stacker = new Stacker(inputSize, outputSize, oneHotLabel);
    }

    @Override
    protected double calcLoss(Tuple3<Double, Double, Vector> labledVector, DenseVector coefVector) {
        if (topologyModel == null) {
            topologyModel = topology.getModel(coefVector);
        } else {
            topologyModel.resetModel(coefVector);
        }
        Tuple2<DenseMatrix, DenseMatrix> unstacked = stacker.unstack(labledVector);
        return topologyModel.computeGradient(unstacked.f0, unstacked.f1, null);
    }

    @Override
    protected void updateGradient(Tuple3<Double, Double, Vector> labledVector, DenseVector coefVector,
                                  DenseVector updateGrad) {
        if (topologyModel == null) {
            topologyModel = topology.getModel(coefVector);
        } else {
            topologyModel.resetModel(coefVector);
        }
        Tuple2<DenseMatrix, DenseMatrix> unstacked = stacker.unstack(labledVector);
        topologyModel.computeGradient(unstacked.f0, unstacked.f1, updateGrad);
    }

    @Override
    protected void updateHessian(Tuple3<Double, Double, Vector> labledVector, DenseVector coefVector,
                                 DenseMatrix updateHessian) {
        throw new RuntimeException("not supported.");
    }

    @Override
    public boolean hasSecondDerivative() {
        return false;
    }
}