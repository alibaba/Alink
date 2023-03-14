package com.alibaba.alink.operator.common.optim.barrierIcq;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

public class BarrierOpt {
	public boolean hasConst;
	public int hessianDim;
	public int dimension;//opt.dimension
	public boolean hasIntercept;
	public final double minL2Weight = 1e-5;
	public DenseVector icb;
	public DenseMatrix icm;
	public DenseMatrix ecm;
	public DenseVector ecb;
	public boolean phaseOne;
	public double l2Weight;
	public BarrierData data;
	public final double convergence_tolerance = 1e-6;
	public final int lineSearchRetryTimes = 40;
}
