package com.alibaba.alink.operator.common.outlier;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.params.outlier.HaskernelType.KernelType;

import java.io.Serializable;

public class OcsvmModelData implements Serializable {
	public SvmModelData[] models;
	public String[] featureColNames;
	public String vectorCol;
	public int baggingNumber;

	/** for poly */
	public int degree;
	/** for poly/rbf/sigmoid */
	public double gamma;
	/** for poly/sigmoid */
	public double coef0;

	public double nu;
	public KernelType kernelType;

	public static class SvmModelData implements Serializable
	{
		/** Total #SupportVec */
		public int numSupportVec;
		/** Sparse SupportVec */
		public SparseVector[] supportSparseVec;
		/** Dense SupportVec */
		public DenseVector[] supportDenseVec;
		/** coefficients for SVs in decision functions */
		public double[][] svCoef;
		/** constants in decision functions (rho[k*(k-1)/2]) */
		public double[] rho;
	}
}
