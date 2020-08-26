package com.alibaba.alink.operator.common.linearprogramming.InteriorPoint;

import com.alibaba.alink.operator.batch.linearprogramming.InteriorPointBatchOp;
import com.alibaba.alink.operator.common.linearprogramming.InteriorPointUtil;
import com.alibaba.alink.operator.common.linearprogramming.AppendSlack;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

/**
 * Given standard form problem defined by ``A``, ``b``, and ``c``;
 * current variable estimates ``x``, ``y``, ``z``, ``tau``, and ``kappa``;
 * algorithmic parameters ``gamma and ``eta;
 * get the search direction for increments to the variable estimates.
 */
public class GetDelta extends ComputeFunction {
    @Override
    public void calc(ComContext context) {
        int m;
        int n;
        int[] rangeM;
        int[] rangeN;
        //calculation resources
        DenseMatrix matrix_A;
        double[] vector_b;
        double[] vector_c;
        double c0;
        double[] vector_x;
        double[] vector_y;
        double[] vector_z;
        double tau;
        double kappa;
        //calculation result
        double[] r_P;
        double[] r_D;
        double[] M;
        double[] x_div_z;
        double mu = 0.0;
        double r_G = 0.0;

        /**
         * init tableau in the first step.
         */
        if (context.getStepNo() == 1) {
            List<Row> fullMatrixListRow = context.getObj(InteriorPointBatchOp.MATRIX);
            List<Row> coefficientListRow = context.getObj(InteriorPointBatchOp.VECTOR);
            List<Row> upperBoundsListRow = context.getObj(InteriorPointBatchOp.UPPER_BOUNDS);
            List<Row> lowerBoundsListRow = context.getObj(InteriorPointBatchOp.LOWER_BOUNDS);
            List<Row> unBoundsListRow = context.getObj(InteriorPointBatchOp.UN_BOUNDS);

            //need varNum when completed
            int varNum = fullMatrixListRow.get(0).getArity() - 2;
            context.putObj(InteriorPointBatchOp.N, varNum);
            //process original input to standard form
            List<Tuple2<Integer, DenseVector>> fullMatrixTupleIntVec = null;
            DenseVector objectiveRow = null;
            try {
                Tuple2<List<Tuple2<Integer, DenseVector>>, DenseVector> data0 = AppendSlack.append(fullMatrixListRow, coefficientListRow, upperBoundsListRow, lowerBoundsListRow, unBoundsListRow);
                fullMatrixTupleIntVec = data0.f0;
                objectiveRow = data0.f1;
            } catch (Exception e) {
                e.printStackTrace();
            }

            m = fullMatrixTupleIntVec.size();
            n = objectiveRow.size() - 1;

            //A, b, c are constant
            matrix_A = new DenseMatrix(m, n);
            vector_b = new double[m];
            vector_c = new double[n];
            for (int i = 0; i < m; i++) {
                vector_b[i] = fullMatrixTupleIntVec.get(i).f1.get(0);
                for (int j = 0; j < n; j++)
                    matrix_A.set(i, j, fullMatrixTupleIntVec.get(i).f1.get(j + 1));
            }
            for (int j = 0; j < n; j++)
                vector_c[j] = objectiveRow.get(j + 1);
            c0 = objectiveRow.get(0);

            //initialize x, y, z, tau, kappa, then putObj
            double[][] initXYZ = InteriorPointUtil.getBlindStart(m + 2, n + 2);//first two elements to store start tail info
            rangeM = InteriorPointUtil.getStartTail(m, context.getTaskId(), context.getNumTask());
            rangeN = InteriorPointUtil.getStartTail(n, context.getTaskId(), context.getNumTask());
            vector_x = DenseVector.ones(n + 2).getData();
            vector_y = DenseVector.zeros(m + 2).getData();
            vector_z = DenseVector.ones(n + 2).getData();
            tau = 1.0;
            kappa = 1.0;
            //vector_x[0] =   rangeM[0];
            //vector_x[1] =   rangeM[1];
            vector_x[0] = rangeN[0];
            vector_x[1] = rangeN[1];
            vector_y[0] = rangeM[0];
            vector_y[1] = rangeM[1];
            vector_z[0] = rangeN[0];
            vector_z[1] = rangeN[1];

            context.putObj(InteriorPointBatchOp.STATIC_A, matrix_A);
            context.putObj(InteriorPointBatchOp.STATIC_B, vector_b);
            context.putObj(InteriorPointBatchOp.STATIC_C, vector_c);
            context.putObj(InteriorPointBatchOp.STATIC_C0, c0);
            context.putObj(InteriorPointBatchOp.LOCAL_X, vector_x);
            context.putObj(InteriorPointBatchOp.LOCAL_Y, vector_y);
            context.putObj(InteriorPointBatchOp.LOCAL_Z, vector_z);
            context.putObj(InteriorPointBatchOp.LOCAL_TAU, tau);
            context.putObj(InteriorPointBatchOp.LOCAL_KAPPA, kappa);
            context.putObj(InteriorPointBatchOp.R_P0,
                    (new DenseVector(vector_b))
                            .minus(matrix_A.multiplies(DenseVector.ones(n)))
            );
            context.putObj(InteriorPointBatchOp.R_D0,
                    (new DenseVector(vector_c)
                            .minus(DenseVector.ones(n)))
            );
            double tmp = 0;
            for (int i = 0; i < vector_c.length; i++)
                tmp += vector_c[i];
            context.putObj(InteriorPointBatchOp.R_G0, tmp + 1);
            context.putObj(InteriorPointBatchOp.MU_0, (n + 1) / (m + 1));

        } else {
            matrix_A = context.getObj(InteriorPointBatchOp.STATIC_A);
            vector_x = context.getObj(InteriorPointBatchOp.LOCAL_X);
            vector_y = context.getObj(InteriorPointBatchOp.LOCAL_Y);
            vector_z = context.getObj(InteriorPointBatchOp.LOCAL_Z);
            vector_b = context.getObj(InteriorPointBatchOp.STATIC_B);
            vector_c = context.getObj(InteriorPointBatchOp.STATIC_C);
            tau = context.getObj(InteriorPointBatchOp.LOCAL_TAU);
            kappa = context.getObj(InteriorPointBatchOp.LOCAL_KAPPA);
            m = matrix_A.numRows();
            n = matrix_A.numCols();
            rangeM = InteriorPointUtil.getStartTail(m, context.getTaskId(), context.getNumTask());
            rangeN = InteriorPointUtil.getStartTail(n, context.getTaskId(), context.getNumTask());
            vector_x[0] = rangeN[0];
            vector_x[1] = rangeN[1];
            vector_y[0] = rangeM[0];
            vector_y[1] = rangeM[1];
            vector_z[0] = rangeN[0];
            vector_z[1] = rangeN[1];
        }

        //to calculate r_P, r_D, r_G
        r_P = new double[m + 2];
        r_D = new double[n + 2];
        Arrays.fill(r_P, 0.0);
        Arrays.fill(r_D, 0.0);

        r_P[0] = (double) rangeM[0];
        r_P[1] = (double) rangeM[1];
        r_D[0] = (double) rangeN[0];
        r_D[1] = (double) rangeN[1];

        // r_P = b * tau - A.dot(x)
        for (int i = rangeM[0]; i < rangeM[1]; i++) {
            for (int j = 0; j < n; j++)
                r_P[i + 2] -= matrix_A.get(i, j) * vector_x[j + 2];
            r_P[i + 2] += vector_b[i] * tau;
        }
        for (int i = rangeN[0]; i < rangeN[1]; i++) {
            for (int j = 0; j < m; j++)
                r_D[i + 2] -= matrix_A.get(j, i) * vector_y[j + 2];
            r_D[i + 2] += vector_c[i] * tau - vector_z[i + 2];
        }
        for (int i = 0; i < n; i++) {
            mu += vector_x[i + 2] * vector_z[i + 2];
            r_G += vector_x[i + 2] * vector_c[i];
        }
        for (int i = 0; i < m; i++)
            r_G -= vector_y[i + 2] * vector_b[i];
        mu = (mu + (tau * kappa)) / (n + 1);
        r_G += kappa;
        context.putObj(InteriorPointBatchOp.R_P, r_P);
        context.putObj(InteriorPointBatchOp.R_D, r_D);
        context.putObj(InteriorPointBatchOp.LOCAL_MU, mu);
        context.putObj(InteriorPointBatchOp.R_G, r_G);

        //to calculate M, 2D square matrix
        M = new double[m * m + 2];
        x_div_z = new double[n];
        Arrays.fill(M, 0.0);
        M[0] = rangeM[0] * m;
        M[1] = rangeM[1] * m;

        //M = A.dot( x/z * A.T)
        DenseMatrix A_T = matrix_A.transpose();
        for (int i = 0; i < n; i++) {
            double scale = vector_x[i + 2] / vector_z[i + 2];
            x_div_z[i] = scale;
            for (int j = 0; j < m; j++)
                A_T.set(i, j, A_T.get(i, j) * scale);
        }
        //M_sub shape is (part_m, n)
        double[] M_sub = matrix_A.getSubMatrix(rangeM[0], rangeM[1], 0, n)
                //.multiplies(A_T.getSubMatrix(0, n,rangeM[0], rangeM[1]))
                .multiplies(A_T)
                .getArrayCopy1D(true);
        System.arraycopy(M_sub, 0, M, (int) M[0] + 2, (int) (M[1] - M[0]));
        context.putObj(InteriorPointBatchOp.LOCAL_M, M);
        context.putObj(InteriorPointBatchOp.LOCAL_X_DIV_Z, x_div_z);
    }
}
