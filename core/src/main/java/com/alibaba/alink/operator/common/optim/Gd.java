package com.alibaba.alink.operator.common.optim;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.common.optim.subfunc.CalcGradient;
import com.alibaba.alink.operator.common.optim.subfunc.CalcLosses;
import com.alibaba.alink.operator.common.optim.subfunc.IterTermination;
import com.alibaba.alink.operator.common.optim.subfunc.OptimVariable;
import com.alibaba.alink.operator.common.optim.subfunc.OutputModel;
import com.alibaba.alink.operator.common.optim.subfunc.ParseRowModel;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateCoefficient;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateLossCurve;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateVector;
import com.alibaba.alink.operator.common.optim.subfunc.UpdateModel;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.optim.HasNumSearchStepDv4;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * The algorithm of GD.
 *
 */
public class Gd extends Optimizer {

    /**
     * construct function.
     *
     * @param objFunc   object function, calc loss and grad.
     * @param trainData data for training.
     * @param coefDim   the dimension of features.
     * @param params    some parameters of optimization method.
     */
    public Gd(DataSet<OptimObjFunc> objFunc, DataSet<Tuple3<Double, Double, Vector>> trainData,
                  DataSet<Integer> coefDim, Params params) {
        super(objFunc, trainData, coefDim, params);
    }

    /**
     * optimizer api.
     *
     * @return the coefficient of linear problem and loss curve values.
     */
    @Override
    public DataSet<Tuple2<DenseVector, double[]>> optimize() {
        //get parameters.
        int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER);
        if (null == this.coefVec) {
            initCoefZeros();
        }
        int numSearchStep = params.get(HasNumSearchStepDv4.NUM_SEARCH_STEP);

        /**
         * solve problem using iteration.
         * trainData is the distributed samples.
         * initCoef is the initial model coefficient, which will be broadcast to every worker.
         * objFuncSet is the object function in dataSet format
         *
         * .add(new PreallocateCoefficient(OptimName.currentCoef)) allocate memory for current coefficient
         * .add(new PreallocateCoefficient(OptimName.minCoef))     allocate memory for min loss coefficient
         * .add(new PreallocateVector(OptimName.dir ...))          allocate memory for grad
         * .add(new CalcGradient(objFunc))                         calculate local sub gradient
         * .add(new AllReduce(OptimName.gradAllReduce))            sum all sub gradient with allReduce
         * .add(new GetGradient())                                 get summed gradient
         * .add(new CalcLosses(objFunc, OptimMethod.GD))           calculate local losses for line search
         * .add(new AllReduce(OptimName.lossAllReduce))            sum all losses with allReduce
         * .add(new UpdateModel(maxIter, epsilon ...))             update coefficient
         * .setCompareCriterionOfNode0(new IterTermination())             judge stop of iteration
         */
        DataSet<Row> model = new IterativeComQueue()
            .initWithPartitionedData(OptimVariable.trainData, trainData)
            .initWithBroadcastData(OptimVariable.model, coefVec)
            .initWithBroadcastData(OptimVariable.objFunc, objFuncSet)
            .add(new PreallocateCoefficient(OptimVariable.currentCoef))
            .add(new PreallocateCoefficient(OptimVariable.minCoef))
            .add(new PreallocateLossCurve(OptimVariable.lossCurve, maxIter))
            .add(new PreallocateVector(OptimVariable.dir, new double[] {0.0, OptimVariable.learningRate}))
            .add(new CalcGradient())
            .add(new AllReduce(OptimVariable.gradAllReduce))
            .add(new GetGradient())
            .add(new CalcLosses(OptimMethod.GD, numSearchStep))
            .add(new AllReduce(OptimVariable.lossAllReduce))
            .add(new UpdateModel(params, OptimVariable.dir, OptimMethod.GD, numSearchStep))
            .setCompareCriterionOfNode0(new IterTermination())
            .closeWith(new OutputModel())
            .setMaxIter(maxIter)
            .exec();

        return model.mapPartition(new ParseRowModel());
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
        }
    }
}
