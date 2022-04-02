package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.outlier.OcsvmModelData.SvmModelData;
import com.alibaba.alink.params.outlier.HaskernelType.KernelType;
import com.alibaba.alink.params.outlier.OcsvmDetectorParams;

public class OcsvmKernel {
	private final Vector[] sample;
	private DenseVector sampleSquare = null;
	private final KernelType kernelType;
	private final int degree;
	private final double gamma;
	private final double coefficient;
	private final CacheUtil cacheUtil;
	private final DenseVector DialValues;

	public OcsvmKernel(Vector[] sample, Params param) {
		this.kernelType = param.get(OcsvmDetectorParams.KERNEL_TYPE);
		this.degree = param.get(OcsvmDetectorParams.DEGREE);
		this.gamma = param.get(OcsvmDetectorParams.GAMMA);
		this.coefficient = param.get(OcsvmDetectorParams.COEF0);
		this.sample = sample;
		if (this.kernelType == KernelType.RBF) {
			this.sampleSquare = new DenseVector(sample.length);
			for (int i = 0; i < sample.length; ++i) {
				this.sampleSquare.set(i, this.sample[i].dot(this.sample[i]));
			}
		}
		cacheUtil = new CacheUtil(sample.length, (100 * (1 << 20)));
		DialValues = new DenseVector(sample.length);
		for (int i = 0; i < sample.length; i++) {
			DialValues.set(i, kernelFunction(i, i));
		}
	}

	double[] getData(int i, int len) {
		Tuple2 <Integer, double[]> t2 = cacheUtil.getData(i, len);
		if (t2.f0 < len) {
			for (int j = t2.f0; j < len; j++) {
				t2.f1[j] = kernelFunction(i, j);
			}
		}
		return t2.f1;
	}

	DenseVector getDial() {
		return DialValues;
	}

	void swap(int i, int j) {
		Vector tmp = this.sample[i];
		this.sample[i] = this.sample[j];
		this.sample[j] = tmp;
		if (this.sampleSquare != null) {
			double tmpVal = this.sampleSquare.get(i);
			this.sampleSquare.set(i, this.sampleSquare.get(j));
			this.sampleSquare.set(j, tmpVal);
		}
	}

	double kernelFunction(int i, int j) {
		double dotValue = this.sample[i].dot(this.sample[j]);
		switch (this.kernelType) {
			case LINEAR:
				return dotValue;
			case POLY:
				return Math.pow(this.gamma * dotValue + this.coefficient, this.degree);
			case RBF:
				return Math.exp(-this.gamma * (this.sampleSquare.get(i) + this.sampleSquare.get(j) - 2.0 * dotValue));
			case SIGMOID:
				return Math.tanh(this.gamma * dotValue + this.coefficient);
			default:
				return 0.0;
		}
	}

	void swapIndex(int i, int j) {
		this.cacheUtil.swapIndex(i, j);
		swap(i, j);
		double tmp = this.DialValues.get(i);
		this.DialValues.getData()[i] = this.DialValues.get(j);
		this.DialValues.set(j, tmp);
	}

	/**
	 * numSample is the number of total data sample and totalSize is the cache size limit in bytes
	 */
	static class CacheUtil {
		private long totalSize;
		private final Element[] samples;
		private final Element lruHead;

		CacheUtil(int numSample, long totalSize) {
			this.totalSize = totalSize;
			this.samples = new Element[numSample];

			for (int i = 0; i < numSample; ++i) {
				this.samples[i] = new Element();
			}

			this.totalSize /= 4;
			this.totalSize -= numSample * 4;
			this.totalSize = Math.max(this.totalSize, 2L * (long) numSample);
			this.lruHead = new Element();
			this.lruHead.next = this.lruHead.prev = this.lruHead;
		}

		private void lruDelete(Element h) {
			h.prev.next = h.next;
			h.next.prev = h.prev;
		}

		private void lruInsert(Element element) {
			element.next = this.lruHead;
			element.prev = this.lruHead.prev;
			element.prev.next = element;
			element.next.prev = element;
		}

		Tuple2 <Integer, double[]> getData(int index, int len) {
			Element element = this.samples[index];
			if (element.len > 0) {
				this.lruDelete(element);
			}

			int more = len - element.len;
			if (more > 0) {
				while (this.totalSize < (long) more) {
					Element old = this.lruHead.next;
					this.lruDelete(old);
					this.totalSize += old.len;
					old.data = null;
					old.len = 0;
				}

				double[] newData = new double[len];
				if (element.data != null) {
					System.arraycopy(element.data, 0, newData, 0, element.len);
				}

				element.data = newData;
				this.totalSize -= more;
				int tmp = element.len;
				element.len = len;
				len = tmp;
			}

			this.lruInsert(element);
			return Tuple2.of(len, element.data);
		}

		void swapIndex(int i, int j) {
			if (i != j) {
				if (this.samples[i].len > 0) {
					this.lruDelete(this.samples[i]);
				}

				if (this.samples[j].len > 0) {
					this.lruDelete(this.samples[j]);
				}

				double[] tmp = this.samples[i].data;
				this.samples[i].data = this.samples[j].data;
				this.samples[j].data = tmp;
				int tmpVal = this.samples[i].len;
				this.samples[i].len = this.samples[j].len;
				this.samples[j].len = tmpVal;
				if (this.samples[i].len > 0) {
					this.lruInsert(this.samples[i]);
				}

				if (this.samples[j].len > 0) {
					this.lruInsert(this.samples[j]);
				}

				if (i > j) {
					tmpVal = i;
					i = j;
					j = tmpVal;
				}

				for (Element element = this.lruHead.next; element != this.lruHead; element = element.next) {
					if (element.len > i) {
						if (element.len > j) {
							double tmpData = element.data[i];
							element.data[i] = element.data[j];
							element.data[j] = tmpData;
						} else {
							this.lruDelete(element);
							this.totalSize += element.len;
							element.data = null;
							element.len = 0;
						}
					}
				}
			}
		}

		private static final class Element {
			private Element prev;
			private Element next;
			private double[] data;
			private int len;
		}
	}

	public static double svmPredict(SvmModelData model, Vector x,
									KernelType kernelType, double gamma, double coef0, int degree) {
		double[] svCoef = model.svCoef[0];
		double sum = 0;
		Vector[] supportVec = model.supportDenseVec != null ? model.supportDenseVec : model.supportSparseVec;
		for (int i = 0; i < model.numSupportVec; i++) {
			switch (kernelType) {
				case LINEAR:
					sum += svCoef[i] * x.dot(supportVec[i]);
					break;
				case POLY:
					sum += svCoef[i] * Math.pow(gamma * x.dot(supportVec[i]) + coef0, degree);
					break;
				case RBF:
					sum += svCoef[i] * Math.exp(-gamma * x.minus(supportVec[i]).normL2Square());
					break;
				case SIGMOID:
					sum += svCoef[i] * Math.tanh(gamma * x.dot(supportVec[i]) + coef0);
					break;
				default:
			}
		}
		sum -= model.rho[0];
		return sum;
	}

	public static SvmModelData svmTrain(Vector[] sample, Params param) {
		SvmModelData model = new SvmModelData();
		model.svCoef = new double[1][];
		double[] alpha = new double[sample.length];
		/* <rho, alpha> */
		model.rho = new double[1];
		model.rho[0] = solveOneClass(sample, param, alpha);

		int nsv = 0;
		for (int i = 0; i < sample.length; i++) {
			if (Math.abs(alpha[i]) > 0) {
				++nsv;
			}
		}
		model.numSupportVec = nsv;
		if (sample[0] instanceof SparseVector) {
			model.supportSparseVec = new SparseVector[nsv];
		} else {
			model.supportDenseVec = new DenseVector[nsv];
		}
		model.svCoef[0] = new double[nsv];
		int j = 0;
		for (int i = 0; i < sample.length; i++) {
			if (Math.abs(alpha[i]) > 0) {
				if (sample[i] instanceof DenseVector) {
					model.supportDenseVec[j] = (DenseVector) sample[i];
				} else if (sample[i] instanceof SparseVector) {
					model.supportSparseVec[j] = (SparseVector) sample[i];
				}
				model.svCoef[0][j] = alpha[i];
				++j;
			}
		}
		return model;
	}

	private static double solveOneClass(Vector[] sample, Params param, double[] alpha) {
		int numSample = sample.length;

		double nu = param.get(OcsvmDetectorParams.NU);
		int n = (int) (nu * sample.length);

		for (int i = 0; i < n; i++) {
			alpha[i] = 1.0;
		}
		if (n < sample.length) {
			alpha[n] = nu * sample.length - n;
		}
		for (int i = n + 1; i < numSample; i++) {
			alpha[i] = 0.0;
		}

		OcsvmKernel kernel = new OcsvmKernel(sample, param);
		Tuple2 <Double, Double> rhoAndObj
			= new SmoSolver(numSample, kernel, alpha, param.get(OcsvmDetectorParams.EPSILON), 1).solve();
		int numSupportVec = 0;

		for (int i = 0; i < numSample; ++i) {
			if (Math.abs(alpha[i]) > 0.0) {
				++numSupportVec;
			}
		}
		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			System.out.println("num support vec = " + numSupportVec);
			System.out.println("object = " + rhoAndObj.f1 + ", rhoVal = " + rhoAndObj.f0);
		}
		return rhoAndObj.f0;
	}
}

