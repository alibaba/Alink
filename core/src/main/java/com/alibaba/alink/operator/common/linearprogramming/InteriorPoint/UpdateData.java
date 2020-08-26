package com.alibaba.alink.operator.common.linearprogramming.InteriorPoint;

import com.alibaba.alink.operator.batch.linearprogramming.InteriorPointBatchOp;
import com.alibaba.alink.operator.common.linearprogramming.InteriorPointUtil;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.linearprogramming.LinearProgrammingUtil;
import org.apache.flink.api.java.tuple.Tuple5;

import static com.alibaba.alink.operator.common.linearprogramming.InteriorPointUtil.Obj2DenseVector;

/**
 *  Calculate alpha then multiply it with delta to update points.
 */
public class UpdateData extends ComputeFunction {
    @Override
    public void calc(ComContext context) {
        DenseVector x = Obj2DenseVector(InteriorPointBatchOp.LOCAL_X, context, true);
        DenseVector y = Obj2DenseVector(InteriorPointBatchOp.LOCAL_Y, context, true);
        DenseVector z = Obj2DenseVector(InteriorPointBatchOp.LOCAL_Z, context, true);
        DenseVector d_x = context.getObj(InteriorPointBatchOp.D_X);
        DenseVector d_y = context.getObj(InteriorPointBatchOp.D_Y);
        DenseVector d_z = context.getObj(InteriorPointBatchOp.D_Z);
        double tau = context.getObj(InteriorPointBatchOp.LOCAL_TAU);
        double d_tau = context.getObj(InteriorPointBatchOp.D_TAU);
        double kappa = context.getObj(InteriorPointBatchOp.LOCAL_KAPPA);
        double d_kappa = context.getObj(InteriorPointBatchOp.D_KAPPA);
        double alpha0 = 0.99995;
        double alpha = InteriorPointUtil.getStep(x, d_x, z, d_z, tau, d_tau, kappa, d_kappa, alpha0);
        Tuple5<DenseVector, DenseVector, DenseVector, Double, Double> data = InteriorPointUtil.doStep
                (x, d_x, y, d_y, z, d_z, tau, d_tau, kappa, d_kappa, alpha);
        context.putObj(InteriorPointBatchOp.LOCAL_X, InteriorPointUtil.Vector2List(data.f0));
        context.putObj(InteriorPointBatchOp.LOCAL_Y, InteriorPointUtil.Vector2List(data.f1));
        context.putObj(InteriorPointBatchOp.LOCAL_Z, InteriorPointUtil.Vector2List(data.f2));
        context.putObj(InteriorPointBatchOp.LOCAL_TAU, data.f3);
        context.putObj(InteriorPointBatchOp.LOCAL_KAPPA, data.f4);
        context.putObj(InteriorPointBatchOp.CONDITION_GO, indicators(data.f0, data.f1, data.f2, data.f3, data.f4, context));
        //linprogUtil.LPPrintVector(data.f0.scale(1/data.f3));
    }

    /**
     * Judge the stop condition of the loop.
     */
    static public boolean indicators(DenseVector x, DenseVector y, DenseVector z,
                                     double tau, double kappa,
                                     ComContext context) {
        DenseVector r_p0_vec = context.getObj(InteriorPointBatchOp.R_P0);
        DenseVector r_d0_vec = context.getObj(InteriorPointBatchOp.R_D0);
        double r_p0 = r_p0_vec.normL2();
        double r_d0 = r_d0_vec.normL2();
        double r_g0 = context.getObj(InteriorPointBatchOp.R_G0);
        //double mu_0 = context.getObj(LPInnerPointBatchOp.MU_0);
        //double c0   = context.getObj(LPInnerPointBatchOp.STATIC_C0);
        DenseMatrix A = context.getObj(InteriorPointBatchOp.STATIC_A);
        DenseVector b = Obj2DenseVector(InteriorPointBatchOp.STATIC_B, context, false);
        DenseVector c = Obj2DenseVector(InteriorPointBatchOp.STATIC_C, context, false);
        double rho_A = Math.abs(c.dot(x) - b.dot(y)) / (tau + Math.abs(b.dot(y)));
        double rho_p = b.scale(tau).minus(A.multiplies(x)).normL2();
        rho_p = r_p0 > 1 ? rho_p / r_p0 : rho_p;
        double rho_d = c.scale(tau).minus(A.transpose().multiplies(y)).minus(z).normL2();
        rho_d = r_d0 > 1 ? rho_d / r_d0 : rho_d;
        double rho_g = kappa + c.dot(x) - b.dot(y);
        rho_g = r_g0 > 1 ? rho_g / r_g0 : rho_g;
        //double rho_mu= (x.dot(z) + tau * kappa) / ((x.size()+1)*mu_0);
        //double obj   = x.scale(1/tau).dot(c) + c0;
        double tol = 1e-8;
        if (rho_p > tol || rho_d > tol || rho_A > tol) {
            context.putObj(InteriorPointBatchOp.LOCAL_X_HAT, x.scale(1 / tau));
            if (context.getTaskId() == 0) {
                System.out.println("x array:");
                LinearProgrammingUtil.LPPrintVector(x.scale(1 / tau));
            }
            return false;
        }
        return true;
    }
}
