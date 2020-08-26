package com.alibaba.alink.operator.common.linearprogramming.InteriorPoint;

import com.alibaba.alink.operator.batch.linearprogramming.InteriorPointBatchOp;
import com.alibaba.alink.operator.common.linearprogramming.InteriorPointUtil;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

import static com.alibaba.alink.operator.common.linearprogramming.InteriorPointUtil.*;

/**
 * Sub step of get delta, use "predictor-corrector" to improve performance substantially.
 * Solve the matrix, calculate delta and alpha two times, i_correction represents the phase.
 */
public class GetAlpha extends ComputeFunction {
    int i_correction;

    public GetAlpha(int i) {
        i_correction = i;
    }

    @Override
    public void calc(ComContext context) {
        double gamma;
        DenseMatrix M_inv;
        DenseMatrix matrix_A = context.getObj(InteriorPointBatchOp.STATIC_A);
        DenseVector b = Obj2DenseVector(InteriorPointBatchOp.STATIC_B, context, false);
        DenseVector c = Obj2DenseVector(InteriorPointBatchOp.STATIC_C, context, false);
        DenseVector M1D = Obj2DenseVector(InteriorPointBatchOp.LOCAL_M, context, true);
        DenseVector x = Obj2DenseVector(InteriorPointBatchOp.LOCAL_X, context, true);
        DenseVector y = Obj2DenseVector(InteriorPointBatchOp.LOCAL_Y, context, true);
        DenseVector z = Obj2DenseVector(InteriorPointBatchOp.LOCAL_Z, context, true);
        double mu = context.getObj(InteriorPointBatchOp.LOCAL_MU);
        double tau = context.getObj(InteriorPointBatchOp.LOCAL_TAU);
        double kappa = context.getObj(InteriorPointBatchOp.LOCAL_KAPPA);
        DenseVector r_P = Obj2DenseVector(InteriorPointBatchOp.R_P, context, true);
        DenseVector r_D = Obj2DenseVector(InteriorPointBatchOp.R_D, context, true);
        DenseVector x_div_z = Obj2DenseVector(InteriorPointBatchOp.LOCAL_X_DIV_Z, context, false);
        int m = b.size();
        int n = c.size();
        DenseMatrix M = new DenseMatrix(m, m, M1D.getData(), true);

        if (i_correction == 0) {
            gamma = 0;
            M_inv = M.inverse();
            context.putObj(InteriorPointBatchOp.LOCAL_M_INV, M_inv);
        } else {
            gamma = context.getObj(InteriorPointBatchOp.LOCAL_GAMMA);
            M_inv = context.getObj(InteriorPointBatchOp.LOCAL_M_INV);
        }

        DenseVector r;
        DenseVector r1;
        DenseVector v;
        DenseVector r_hat_xs;
        r = b.plus(matrix_A.multiplies(VectorTimes(x_div_z, c)));
        DenseVector q = M_inv.multiplies(r);
        DenseVector p;
        p = matrix_A.transpose().multiplies(q).minus(c);
        p = VectorTimes(p, x_div_z);

        if (i_correction == 0) {
            r_hat_xs = VectorPlusConst(VectorTimes(x, z).scale(-1), gamma * mu);
            r1 = r_D.minus(VectorDivs(r_hat_xs, x));
            r = r_P.plus(matrix_A.multiplies(VectorTimes(x_div_z, r1)));
            context.putObj(InteriorPointBatchOp.LOCAL_R_HAT_XS, r_hat_xs);
        } else {
            DenseVector r_hat_p = r_P.scale(1 - gamma);
            DenseVector r_hat_d = r_D.scale(1 - gamma);
            DenseVector d_x = context.getObj(InteriorPointBatchOp.D_X);
            DenseVector d_z = context.getObj(InteriorPointBatchOp.D_Z);
            r_hat_xs = VectorPlusConst(VectorTimes(x, z).scale(-1), gamma * mu);
            r_hat_xs.minusEqual(VectorTimes(d_x, d_z));
            r1 = r_hat_d.minus(VectorDivs(r_hat_xs, x));
            r = r_hat_p.plus(matrix_A.multiplies(VectorTimes(x_div_z, r1)));
        }
        v = M_inv.multiplies(r);
        DenseVector u;
        u = matrix_A.transpose().multiplies(v).minus(r1);
        u = VectorTimes(u, x_div_z);

        double r_G = c.dot(x) - b.dot(y) + kappa;
        double r_hat_g = (1 - gamma) * r_G;
        double r_hat_tk = gamma * mu - tau * kappa;

        double d_tau;
        DenseVector d_x;
        DenseVector d_y;
        DenseVector d_z;
        double d_kappa;
        if (i_correction == 1) {
            d_tau = context.getObj(InteriorPointBatchOp.D_TAU);
            d_kappa = context.getObj(InteriorPointBatchOp.D_KAPPA);
            r_hat_tk -= d_tau * d_kappa;
        }

        d_tau = ((r_hat_g + 1 / tau * r_hat_tk - (-c.dot(u) + b.dot(v))) /
                (1 / tau * kappa + (-c.dot(p) + b.dot(q))));
        d_x = p.scale(d_tau).plus(u);
        d_y = q.scale(d_tau).plus(v);
        d_z = VectorTimes(VectorDivs(DenseVector.ones(n), x), r_hat_xs.minus(VectorTimes(z, d_x)));
        d_kappa = 1 / tau * (r_hat_tk - kappa * d_tau);
        double alpha = InteriorPointUtil.getStep(x, d_x, z, d_z, tau, d_tau, kappa, d_kappa, 1.0);
        gamma = (1 - alpha) * (1 - alpha) * Math.min(0.1, 1 - alpha);
        context.putObj(InteriorPointBatchOp.LOCAL_GAMMA, gamma);
        context.putObj(InteriorPointBatchOp.D_X, d_x);
        context.putObj(InteriorPointBatchOp.D_Y, d_y);
        context.putObj(InteriorPointBatchOp.D_Z, d_z);
        context.putObj(InteriorPointBatchOp.D_TAU, d_tau);
        context.putObj(InteriorPointBatchOp.D_KAPPA, d_kappa);
    }

}
