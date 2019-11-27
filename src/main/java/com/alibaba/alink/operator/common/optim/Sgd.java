package com.alibaba.alink.operator.common.optim;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.subfunc.IterTermination;
import com.alibaba.alink.operator.common.optim.subfunc.OptimVariable;
import com.alibaba.alink.operator.common.optim.subfunc.OutputModel;
import com.alibaba.alink.operator.common.optim.subfunc.ParseRowModel;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateCoefficient;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateLossCurve;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateVector;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.params.shared.optim.SgdParams;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The optimizer of SGD.
 *
 */
public class Sgd extends Optimizer {

    /**
     * construct function.
     *
     * @param objFunc   object function, calc loss and grad.
     * @param trainData data for training.
     * @param coefDim   the dimension of features.
     * @param params    some parameters of optimization method.
     */
    public Sgd(DataSet<OptimObjFunc> objFunc, DataSet<Tuple3<Double, Double, Vector>> trainData,
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
        int maxIter = params.get(SgdParams.MAX_ITER);
        double learnRate = params.get(SgdParams.LEARNING_RATE);
        double miniBatchFraction = params.get(SgdParams.MINI_BATCH_FRACTION);
        double epsilon = params.get(SgdParams.EPSILON);
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
         * .add(new PreallocateLossCurve(OptimVariable.lossCurve)) allocate memory for loss values
         * .add(new PreallocateVector(OptimName.dir ...))          allocate memory for dir
         * .add(new CalcSubGradient(objFunc, miniBatchFraction))   calculate local sub gradient
         * .add(new AllReduce(OptimName.gradAllReduce))            sum all sub gradient with allReduce
         * .add(new GetGradient())                                 get summed gradient
         * .add(new UpdateSgdModel(maxIter, epsilon ...))          update coefficient
         * .setCompareCriterionOfNode0(new IterTermination())             judge stop of iteration
         */
        DataSet<Row> model = new IterativeComQueue()
            .initWithPartitionedData(OptimVariable.trainData, trainData)
            .initWithBroadcastData(OptimVariable.model, coefVec)
            .initWithBroadcastData(OptimVariable.objFunc, objFuncSet)
            .add(new PreallocateCoefficient(OptimVariable.minCoef))
            .add(new PreallocateLossCurve(OptimVariable.lossCurve, maxIter))
            .add(new PreallocateVector(OptimVariable.dir, new double[2]))
            .add(new CalcSubGradient(miniBatchFraction))
            .add(new AllReduce(OptimVariable.gradAllReduce))
            .add(new GetGradient())
            .add(new UpdateSgdModel(maxIter, epsilon, learnRate, OptimMethod.SGD))
            .setCompareCriterionOfNode0(new IterTermination())
            .closeWith(new OutputModel())
            .setMaxIter(maxIter)
            .exec();

        return model.mapPartition(new ParseRowModel());
    }

    public static class CalcSubGradient extends ComputeFunction {
        /**
         * object function class, it supply the functions to calc gradient (or loss) locality.
         */
        private OptimObjFunc objFunc;
        private double fraction;
        private transient List<Tuple3<Double, Double, Vector>> data = null;
        private transient List<Tuple3<Double, Double, Vector>> miniBatchData = null;

        private Random random = null;
        private int batchSize;
        private int totalSize;

        public CalcSubGradient(double fraction) {
            this.fraction = fraction;
        }

        @Override
        public void calc(ComContext context) {
            if (data == null) {
                data = context.getObj(OptimVariable.trainData);
                random = new Random();

                batchSize = Double.valueOf(data.size() * fraction).intValue();
                miniBatchData = new ArrayList<>(batchSize);
                totalSize = data.size();
                for (int i = 0; i < batchSize; ++i) {
                    int id = random.nextInt(totalSize);
                    miniBatchData.add(data.get(id));
                }
            } else {
                for (int i = 0; i < batchSize; ++i) {
                    int id = random.nextInt(totalSize);
                    miniBatchData.set(i, data.get(id));
                }
            }

            if (objFunc == null) {
                objFunc = ((List<OptimObjFunc>)context.getObj(OptimVariable.objFunc)).get(0);
            }

            // get iterative state from static memory.
            Tuple2<DenseVector, Double> coef = context.getObj(OptimVariable.minCoef);
            int size = coef.f0.size();
            // calculate local gradient
            Tuple2<DenseVector, double[]> grad = context.getObj(OptimVariable.dir);
            for (int i = 0; i < size; ++i) {
                grad.f0.set(i, 0.0);
            }
            double weightSum = objFunc.calcGradient(miniBatchData, coef.f0, grad.f0);
            double loss = objFunc.calcObjValue(miniBatchData, coef.f0).f0;

            grad.f1[0] = weightSum;
            grad.f1[1] = loss;

            // prepare buffer vec for allReduce. the last two elements of vec are the weight Sum and loss.
            double[] buffer = context.getObj(OptimVariable.gradAllReduce);
            if (buffer == null) {
                buffer = new double[size + 2];
                context.putObj(OptimVariable.gradAllReduce, buffer);
            }
            for (int i = 0; i < size; ++i) {
                buffer[i] = grad.f0.get(i) * grad.f1[0];
            }
            buffer[size] = grad.f1[0];
            buffer[size + 1] = grad.f1[1];
        }
    }

    public static class UpdateSgdModel extends ComputeFunction {

        private final static Logger LOG = LoggerFactory.getLogger(UpdateSgdModel.class);
        private double epsilon;
        private int maxIter;
        private double learnRate;
        private OptimMethod method;

        public UpdateSgdModel(int maxIter, double epsilon, double learnRate, OptimMethod method) {
            this.maxIter = maxIter;
            this.epsilon = epsilon;
            this.method = method;
            this.learnRate = learnRate;
        }

        @Override
        public void calc(ComContext context) {
            Tuple2<DenseVector, double[]> grad = context.getObj(OptimVariable.dir);
            Tuple2<DenseVector, Double> m = context.getObj(OptimVariable.minCoef);

            double eta = learnRate / (grad.f0.normInf() + Math.sqrt(context.getStepNo()));
            m.f0.plusScaleEqual(grad.f0, -eta);
            filter(grad, context, eta);
        }

        /**
         * judge the convergence of iteration.
         *
         * @param grad    gradient.
         * @param context context of iteration.
         * @param eta     current learningRate
         */
        public void filter(Tuple2<DenseVector, double[]> grad,
                           ComContext context, double eta) {
            double gradNorm = ((Tuple2<DenseVector, double[]>)context.getObj(OptimVariable.dir)).f0.normL2();
            if (context.getTaskId() == 0) {
                System.out.println(
                    method.toString() + " method continue at step : " + context.getStepNo() + " cur loss : "
                        + grad.f1[1] / grad.f1[0] + " grad norm : " + gradNorm + " learning rate : " + eta);
            }
            if (gradNorm < epsilon) {
                LOG.info(method.toString() + " method converged at step : "
                    + ": {}, grad norm: {}", context.getStepNo(), gradNorm);
                grad.f1[0] = -1.0;
            } else if (context.getStepNo() > maxIter - 1) {
                LOG.info(method.toString() + " method stop at max step : "
                    + ": {}, grad norm: {}", context.getStepNo(), gradNorm);
                grad.f1[0] = -1.0;
            }
        }
    }

    public static class GetGradient extends ComputeFunction {
        @Override
        public void calc(ComContext context) {
            Tuple2<DenseVector, double[]> grad = context.getObj(OptimVariable.dir);
            int size = grad.f0.size();
            double[] gradarr = context.getObj(OptimVariable.gradAllReduce);

            for (int i = 0; i < size; ++i) {
                grad.f0.set(i, gradarr[i] / gradarr[size]);
            }
            grad.f1[0] = gradarr[size];
            grad.f1[1] = gradarr[size + 1];
        }
    }
}
