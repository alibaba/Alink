package com.alibaba.alink.operator.batch.linearprogramming;

import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.comqueue.communication.AllReduce.SerializableBiConsumer;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linearprogramming.InteriorPoint.*;
import com.alibaba.alink.params.linearprogramming.LPParams;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * The *interior-point* method uses the primal-dual path following algorithm
 * outlined in [1]_. Interior point methods are a type of algorithm that are
 * used in solving both linear and nonlinear convex optimization problems
 * that contain inequalities as constraints. The LP Interior-Point method
 * relies on having a linear programming model with the objective function
 * and all constraints being continuous and twice continuously differentiable.
 * <p>
 * References
 * ----------
 * .. [1] Andersen, Erling D., and Knud D. Andersen. "The MOSEK interior point
 * optimizer for linear programming: an implementation of the
 * homogeneous algorithm." High performance optimization. Springer US,
 * 2000. 197-232.
 */
public class InteriorPointBatchOp extends BatchOperator<InteriorPointBatchOp> implements LPParams<InteriorPointBatchOp> {
    public static final String MATRIX = "matrix";
    public static final String VECTOR = "vector";
    public static final String UPPER_BOUNDS = "upperBounds";
    public static final String LOWER_BOUNDS = "lowerBounds";
    public static final String UN_BOUNDS = "unBounds";
    public static final String STATIC_A = "staticA";// (m,n)
    public static final String STATIC_B = "staticB";// (m,)
    public static final String STATIC_C = "staticC";// (n,)
    public static final String STATIC_C0 = "staticC0";// constant
    public static final String LOCAL_X = "localX";// (n+2,)
    public static final String LOCAL_Y = "localY";// (m+2,)
    public static final String LOCAL_Z = "localZ";// (n+2,)
    public static final String LOCAL_MU = "localMu";// constant
    public static final String LOCAL_TAU = "localTau";// constant
    public static final String LOCAL_KAPPA = "localKappa";// constant
    public static final String LOCAL_GAMMA = "localGamma";// constant
    public static final String LOCAL_M = "localM";// (m*m+2,) row major 1D matrix
    public static final String LOCAL_M_INV = "localMInv";//
    public static final String LOCAL_X_DIV_Z = "localXDivZ";// (n,)
    public static final String LOCAL_R_HAT_XS = "localRHatXs";
    public static final String LOCAL_X_HAT = "localXHat";
    public static final String R_P = "r_P";// (m+2,)
    public static final String R_D = "r_D";// (n+2,)
    public static final String R_G = "r_G";// constant
    public static final String D_X = "d_x";
    public static final String D_Y = "d_y";
    public static final String D_Z = "d_z";
    public static final String D_TAU = "d_tau";
    public static final String D_KAPPA = "d_kappa";
    public static final String N = "n";
    public static final String R_P0 = "r_p0";
    public static final String R_D0 = "r_d0";
    public static final String R_G0 = "r_g0";
    public static final String MU_0 = "mu_0";
    public static final String CONDITION_GO = "go";

    /**
     * null constructor.
     */
    public InteriorPointBatchOp() {
        this(null);
    }

    /**
     * constructor.
     * * @param params the parameters set.
     */
    public InteriorPointBatchOp(Params params) {
        super(params);
    }

    static DataSet<Row> iterateICQ(DataSet<Row> inputMatrix,
                                   DataSet<Row> inputVec,
                                   DataSet<Row> upperBounds,
                                   DataSet<Row> lowerBounds,
                                   DataSet<Row> unBounds,
                                   int iter) {
        return new IterativeComQueue()
                .initWithBroadcastData(MATRIX, inputMatrix)
                .initWithBroadcastData(VECTOR, inputVec)
                .initWithBroadcastData(UPPER_BOUNDS, upperBounds)
                .initWithBroadcastData(LOWER_BOUNDS, lowerBounds)
                .initWithBroadcastData(UN_BOUNDS, unBounds)
                .add(new GetDelta())
                .add(new AllReduce(R_D, null, new mergeVectorReduceFunc()))
                .add(new AllReduce(R_P, null, new mergeVectorReduceFunc()))
                .add(new AllReduce(LOCAL_M, null, new mergeVectorReduceFunc()))
                .add(new GetAlpha(0))
                .add(new GetAlpha(1))
                .add(new UpdateData())
                .setCompareCriterionOfNode0(new InteriorPointIterTermination())
                .closeWith(new InteriorPointComplete())
                .setMaxIter(iter)
                .exec();
    }

    @Override
    public InteriorPointBatchOp linkFrom(BatchOperator<?>... inputs) {
        DataSet<Row> inputM = inputs[0].getDataSet();
        DataSet<Row> inputV = inputs[1].getDataSet();
        DataSet<Row> UpperBoundsDataSet = inputs[2].getDataSet();
        DataSet<Row> LowerBoundsDataSet = inputs[3].getDataSet();
        DataSet<Row> UnBoundsDataSet = inputs[4].getDataSet();
        int iter = getMaxIter();
        DataSet<Row> Input = iterateICQ(inputM, inputV,
                UpperBoundsDataSet,
                LowerBoundsDataSet,
                UnBoundsDataSet,
                iter)
                .map((MapFunction<Row, Row>) row -> {
                    return row;
                })
                .returns(new RowTypeInfo(Types.DOUBLE));
        this.setOutput(Input, new String[]{"result"});

        return this;
    }

    private static class mergeVectorReduceFunc implements SerializableBiConsumer<double[], double[]> {
        @Override
        public void accept(double[] doubles, double[] doubles2) {
            int s = (int) doubles2[0];
            int t = (int) doubles2[1];
            System.arraycopy(doubles2, s + 2, doubles, s + 2, t - s);
        }
    }
}

