package com.alibaba.alink.operator.common.optim.subfunc;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.optim.OptimMethod;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.linear.HasEpsilonDv0000001;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class do two things:
 * <p>
 * 1. update the coefficient by dir and step length.
 * <p>
 * 2. judge the convergence of iteration.
 *
 */
public class UpdateModel extends ComputeFunction {

    private static final double EPS = 1.0e-18;
    private final static Logger LOG = LoggerFactory.getLogger(UpdateModel.class);
    private int numSearchStep;
    private OptimMethod method;
    private String gradName;
    private Params params;

    /**
     * construct function.
     *
     * @param params   parameters for this class.
     * @param gradName this string is used to find grad vector.
     * @param method   optimization method.
     */
    public UpdateModel(Params params, String gradName, OptimMethod method, int numSearchStep) {
        this.method = method;
        this.gradName = gradName;
        this.params = params;
        this.numSearchStep = numSearchStep;
    }

    @Override
    public void calc(ComContext context) {
        double[] losses = context.getObj(OptimVariable.lossAllReduce);
        Tuple2<DenseVector, double[]> dir = context.getObj(OptimVariable.dir);
        Tuple2<DenseVector, double[]> pseGrad = context.getObj(OptimVariable.pseGrad);
        Tuple2<DenseVector, Double> curCoef = context.getObj(OptimVariable.currentCoef);
        Tuple2<DenseVector, Double> minCoef = context.getObj(OptimVariable.minCoef);

        double lossChangeRatio = 1.0;
        double[] lossCurve = context.getObj(OptimVariable.lossCurve);
        for (int j = 0; j < losses.length; ++j) {
            losses[j] /= dir.f1[0];
        }
        int pos = -1;
        //get the min value of losses, and remember the position.
        for (int j = 0; j < losses.length; ++j) {
            if (losses[j] < losses[0]) {
                losses[0] = losses[j];
                pos = j;
            }
        }

        // adaptive learningRate strategy
        double beta = dir.f1[1] / numSearchStep;
        double eta;
        if (pos == -1) {
            /**
             * if all losses larger than last loss value. we'll do the below things:
             * 1. reduce learning rate by multiply 1.0 / (numSearchStep*numSearchStep).
             * 2. set eta with zero.
             * 3. set current loss equals last loss value.
             */
            eta = 0;
            dir.f1[1] *= 1.0 / (numSearchStep * numSearchStep);
            curCoef.f1 = losses[0];
        } else if (pos == numSearchStep) {
            /**
             * if losses[numSearchStep] smaller than last loss value. we'll do the below things:
             * 1. enlarge learning rate by multiply numSearchStep.
             * 2. set eta with the smallest value pos.
             * 3. set current loss equals smallest loss value.
             */
            eta = beta * pos;
            dir.f1[1] *= numSearchStep;
            dir.f1[1] = Math.min(dir.f1[1], numSearchStep);
            lossChangeRatio = Math.abs((curCoef.f1 - losses[pos]) / curCoef.f1);
            curCoef.f1 = losses[numSearchStep];
        } else {
            /**
             * else :
             * 1. learning rate not changed.
             * 2. set eta with the smallest value pos.
             * 3. set current loss equals smallest loss value.
             */
            eta = beta * pos;
            lossChangeRatio = Math.abs((curCoef.f1 - losses[pos]) / curCoef.f1);
            curCoef.f1 = losses[pos];
        }

		/* update loss value in loss curve at this step */
        lossCurve[context.getStepNo() - 1] = curCoef.f1;

        if (method.equals(OptimMethod.OWLQN)) {
            /**
             * if owlqn method: we'll using the constraint line search strategy.
             */
            Tuple2<DenseVector[], DenseVector[]> sKyK = context.getObj(OptimVariable.sKyK);
            int size = dir.f0.size();
            int k = context.getStepNo() - 1;
            DenseVector[] sK = sKyK.f0;
            for (int s = 0; s < size; ++s) {
                double val = curCoef.f0.get(s);
                double newVal = val - dir.f0.get(s) * eta;
                if (Math.abs(val) > 0.0) {
                    if (newVal * val < 0) {
                        newVal = 0.0;
                    }
                } else if (newVal * pseGrad.f0.get(s) > 0) {
                    newVal = 0.0;
                }
                sK[k % OptimVariable.numCorrections].set(s, newVal - val);
                curCoef.f0.set(s, newVal);
            }
        } else if (method.equals(OptimMethod.LBFGS)) {
            Tuple2<DenseVector[], DenseVector[]> sKyK = context.getObj(OptimVariable.sKyK);
            int size = dir.f0.size();
            int k = context.getStepNo() - 1;
            DenseVector[] sK = sKyK.f0;
            for (int s = 0; s < size; ++s) {
                sK[k % OptimVariable.numCorrections].set(s, dir.f0.get(s) * (-eta));
            }
            curCoef.f0.plusScaleEqual(dir.f0, -eta);
        } else {
            curCoef.f0.plusScaleEqual(dir.f0, -eta);
        }

        /**
         * if current loss is smaller than min loss, then update the min loss and min coefficient by current.
         */
        if (curCoef.f1 < minCoef.f1) {
            minCoef.f1 = curCoef.f1;
            minCoef.f0.setEqual(curCoef.f0);
        }

        //  judge the convergence of iteration.
        filter(dir, curCoef, minCoef, context, lossChangeRatio);
    }

    /**
     * judge the convergence of iteration.
     */
    public void filter(Tuple2<DenseVector, double[]> grad,
                       Tuple2<DenseVector, Double> c,
                       Tuple2<DenseVector, Double> m,
                       ComContext context,
                       double lossChangeRatio) {
        double epsilon = params.get(HasEpsilonDv0000001.EPSILON);
        int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER);
        double gradNorm = ((Tuple2<DenseVector, double[]>)context.getObj(gradName)).f0.normL2();
        if (c.f1 < epsilon || gradNorm < epsilon) {
            printLog(" method converged at step : ", c.f1, m.f1, grad.f1[1], gradNorm, context, lossChangeRatio);
            grad.f1[0] = -1.0;
        } else if (context.getStepNo() > maxIter - 1) {
            printLog(" method stop at max step : ", c.f1, m.f1, grad.f1[1], gradNorm, context, lossChangeRatio);
            grad.f1[0] = -1.0;
        } else if (grad.f1[1] < EPS) {
            printLog(" learning rate is too small, method stops at step : ", c.f1, m.f1, grad.f1[1], gradNorm,
                context, lossChangeRatio);
            grad.f1[0] = -1.0;
        } else if (lossChangeRatio < epsilon && gradNorm < Math.sqrt(epsilon)) {
            printLog(" loss change ratio is too small, method stops at step : ", c.f1, m.f1, grad.f1[1], gradNorm,
                context, lossChangeRatio);
            grad.f1[0] = -1.0;
        } else {
            printLog(" method continue at step : ", c.f1, m.f1, grad.f1[1], gradNorm, context, lossChangeRatio);

        }
    }

    private void printLog(String info, double curLoss, double minLoss, double learnRate, double gradNorm,
                          ComContext context, double lossChangeRatio) {
        if (context.getTaskId() == 0) {
            System.out.println(method.toString() + info + context.getStepNo() + " cur loss : " + curLoss
                + " min loss : " + minLoss + " grad norm : "
                + gradNorm + " learning rate : " + learnRate + " loss change ratio : " + lossChangeRatio);
        }
        LOG.info(method.toString() + info + ": {}, cur loss: {}, min loss: {}, grad norm: {}, learning rate: {}",
            context.getStepNo(), curLoss, minLoss, gradNorm, learnRate);

    }

}
