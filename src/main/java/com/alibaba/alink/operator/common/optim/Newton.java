package com.alibaba.alink.operator.common.optim;

import java.util.List;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.subfunc.IterTermination;
import com.alibaba.alink.operator.common.optim.subfunc.OptimVariable;
import com.alibaba.alink.operator.common.optim.subfunc.OutputModel;
import com.alibaba.alink.operator.common.optim.subfunc.ParseRowModel;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateCoefficient;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateLossCurve;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateMatrix;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateVector;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.linear.HasEpsilonDv0000001;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Newton method.
 *
 */
public class Newton extends Optimizer {

    private static final int MAX_FEATURE_NUM = 1000;

    /**
     * construct function.
     *
     * @param objFunc   object function, calc loss and grad.
     * @param trainData data for training.
     * @param coefDim   the dimension of features.
     * @param params    some parameters of optimization method.
     */
    public Newton(DataSet<OptimObjFunc> objFunc, DataSet<Tuple3<Double, Double, Vector>> trainData,
               DataSet<Integer> coefDim, Params params) {
        super(objFunc, trainData, coefDim, params);
    }
    /**
     * optimizer api.
     *
     * @return the coefficient of linear problem.
     */
    @Override
    public DataSet<Tuple2<DenseVector, double[]>> optimize() {
        //get parameters.
        int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER);
        double epsilon = params.get(HasEpsilonDv0000001.EPSILON);
        if (null == this.coefVec) {
            initCoefZeros();
        }

        /**
         * solve problem using iteration.
         * trainData is the distributed samples.
         * initCoef is the initial model coefficient, which will be broadcast to every worker.
         * objFuncSet is the object function in dataSet format
         *
         * .add(new PreallocateCoefficient(OptimName.currentCoef)) allocate memory for current coefficient
         * .add(new PreallocateCoefficient(OptimName.minCoef))     allocate memory for min loss coefficient
         * .add(new PreallocateLossCurve(OptimVariable.lossCurve)) allocate memory for loss values
         * .add(new PreallocateVector(OptimName.dir ...))          allocate memory for grad
         * ..add(new PreallocateMatrix(OptimName.hessian,...))     allocate memory for hessian matrix
         * .add(new CalcGradientAndHessian(objFunc))               calculate local sub gradient and hessian
         * .add(new AllReduce(OptimName.gradAllReduce))            sum all sub gradient and hessian with allReduce
         * .add(new GetGradientAndHessian())                       get summed gradient and hessian
         * .add(new UpdateModel(maxIter, epsilon ...))             update coefficient with gradient and hessian
         * .setCompareCriterionOfNode0(new IterTermination())             judge stop of iteration
         *
         */
        DataSet<Row> model = new IterativeComQueue()
            .initWithPartitionedData(OptimVariable.trainData, trainData)
            .initWithBroadcastData(OptimVariable.model, coefVec)
            .initWithBroadcastData(OptimVariable.objFunc, objFuncSet)
            .add(new PreallocateCoefficient(OptimVariable.currentCoef))
            .add(new PreallocateCoefficient(OptimVariable.minCoef))
            .add(new PreallocateLossCurve(OptimVariable.lossCurve, maxIter))
            .add(new PreallocateVector(OptimVariable.dir, new double[2]))
            .add(new PreallocateMatrix(OptimVariable.hessian, MAX_FEATURE_NUM))
            .add(new CalcGradientAndHessian())
            .add(new AllReduce(OptimVariable.gradHessAllReduce))
            .add(new GetGradeintAndHessian())
            .add(new UpdateModel(maxIter, epsilon))
            .setCompareCriterionOfNode0(new IterTermination())
            .closeWith(new OutputModel())
            .setMaxIter(maxIter)
            .exec();

        return model.mapPartition(new ParseRowModel());
    }

    /**
     * calc hessian matrix, gradient and loss.
     */
    public static class CalcGradientAndHessian extends ComputeFunction {
        private OptimObjFunc objFunc;

        @Override
        public void calc(ComContext context) {
            Iterable<Tuple3<Double, Double, Vector>> labledVectors = context.getObj(OptimVariable.trainData);
            Tuple2<DenseVector, double[]> grad = context.getObj(OptimVariable.dir);
            Tuple2<DenseVector, double[]> curCoef = context.getObj(OptimVariable.currentCoef);
            DenseMatrix hessian = context.getObj(OptimVariable.hessian);

            int size = grad.f0.size();

            if (objFunc == null) {
                objFunc = ((List<OptimObjFunc>)context.getObj(OptimVariable.objFunc)).get(0);
            }

            Tuple2<Double, Double> loss = objFunc.calcHessianGradientLoss(labledVectors, curCoef.f0, hessian, grad.f0);

            /**
             * prepare buffer vec for allReduce. the last two elements of vec are weight Sum and current loss.
             */
            double[] buffer = context.getObj("gradHessAllReduce");
            if (buffer == null) {
                buffer = new double[size + size * size + 2];
                context.putObj("gradHessAllReduce", buffer);
            }
            for (int i = 0; i < size; ++i) {
                buffer[i] = grad.f0.get(i);
                for (int j = 0; j < size; ++j) {
                    buffer[(i + 1) * size + j] = hessian.get(i, j);
                }
            }
            buffer[size + size * size] = loss.f0;
            buffer[size + size * size + 1] = loss.f1;
        }
    }

    public static class GetGradeintAndHessian extends ComputeFunction {

        @Override
        public void calc(ComContext context) {
            Tuple2<DenseVector, double[]> grad = context.getObj(OptimVariable.dir);
            DenseMatrix hessian = context.getObj(OptimVariable.hessian);
            int size = grad.f0.size();
            double[] gradarr = context.getObj(OptimVariable.gradHessAllReduce);
            grad.f1[0] = gradarr[size + size * size];
            for (int i = 0; i < size; ++i) {
                grad.f0.set(i, gradarr[i] / grad.f1[0]);
                for (int j = 0; j < size; ++j) {
                    hessian.set(i, j, gradarr[(i + 1) * size + j] / grad.f1[0]);
                }
            }
            grad.f1[0] = gradarr[size + size * size];
            grad.f1[1] = gradarr[size + size * size + 1] / grad.f1[0];
        }
    }

    public static class UpdateModel extends ComputeFunction {
        private final static Logger LOG = LoggerFactory.getLogger(UpdateModel.class);
        private DenseMatrix bMat = null;
        private double epsilon;
        private int maxIter;

        private UpdateModel(int maxIter, double epsilon) {
            this.maxIter = maxIter;
            this.epsilon = epsilon;
        }

        @Override
        public void calc(ComContext context) {
            Tuple2<DenseVector, double[]> grad = context.getObj(OptimVariable.dir);
            Tuple2<DenseVector, Double> curCoef = context.getObj(OptimVariable.currentCoef);
            Tuple2<DenseVector, Double> minCoef = context.getObj(OptimVariable.minCoef);
            DenseMatrix hessian = context.getObj(OptimVariable.hessian);
            double gradNorm = grad.f0.normL2();
            double norm = 1 / grad.f0.normL1();
            hessian.scaleEqual(norm);
            grad.f0.scaleEqual(norm);
            int vecLength = grad.f0.size();
            if (bMat == null) {
                bMat = new DenseMatrix(vecLength, 1);
            }
            for (int j = 0; j < vecLength; j++) {
                bMat.set(j, 0, grad.f0.get(j));
            }
            // solve linear system to get the descent direction.
            DenseMatrix xMat = hessian.solveLS(bMat);
            for (int j = 0; j < vecLength; j++) {
                grad.f0.set(j, xMat.get(j, 0));
            }
            // update the current coefficient and loss.
            curCoef.f0.minusEqual(grad.f0);
            curCoef.f1 = grad.f1[1];
            // if current loss < min loss, then update min loss and coefficient.
            if (curCoef.f1 < minCoef.f1) {
                minCoef.f1 = curCoef.f1;
                for (int j = 0; j < vecLength; j++) {
                    minCoef.f0.set(j, curCoef.f0.get(j));
                }
            }
            filter(grad, curCoef, minCoef, gradNorm, context);
        }

        /**
         * judge the convergence of iteration.
         */
        public void filter(Tuple2<DenseVector, double[]> grad,
                           Tuple2<DenseVector, Double> c,
                           Tuple2<DenseVector, Double> m,
                           double gradNorm, ComContext context) {
            if (c.f1 < epsilon || gradNorm < epsilon) {
                printLog(" method converged at step : ", c.f1, m.f1, gradNorm, context);
                grad.f1[0] = -1.0;
            } else if (context.getStepNo() > maxIter - 1) {
                printLog(" method stop at max step : ", c.f1, m.f1, gradNorm, context);
                grad.f1[0] = -1.0;
            } else {
                printLog(" method continue at step : ", c.f1, m.f1, gradNorm, context);
            }
        }

        private void printLog(String info, double curLoss, double minLoss, double gradNorm,
                              ComContext context) {
            if (context.getTaskId() == 0) {
                System.out.println("Newton" + info + context.getStepNo() + " cur loss : " + curLoss
                    + " min loss : " + minLoss + " grad norm : " + gradNorm);
            }
            LOG.info("Newton" + info + ": {}, cur loss: {}, min loss: {}, grad norm: {}",
                context.getStepNo(), curLoss, minLoss, gradNorm);
        }
    }
}
