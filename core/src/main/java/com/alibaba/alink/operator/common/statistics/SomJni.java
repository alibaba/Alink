package com.alibaba.alink.operator.common.statistics;

public class SomJni {

	public static int getNeuronPos(int x, int y, int xdim, int ydim, int vdim) {
		return (y * xdim + x) * vdim;
	}

	public void updateBatchJava(float[] w, float[] batch, int cnt, double lr, double sig,
								int xdim, int ydim, int vdim) {
		float[] d2 = new float[xdim * ydim];
		int[] bmu = new int[2];
		float[] v = new float[vdim];

		double[] xGaussian = new double[xdim];
		double[] yGaussian = new double[ydim];

		double d = 2.0 * Math.PI * sig * sig;
		for (int i = 0; i < xdim; i++) {
			xGaussian[i] = Math.exp(-1.0 * i * i / d);
		}
		for (int i = 0; i < ydim; i++) {
			yGaussian[i] = Math.exp(-1.0 * i * i / d);
		}

		for (int c = 0; c < cnt; c++) {
			int p = c * vdim;
			for (int j = 0; j < vdim; j++) {
				v[j] = batch[p + j];
			}
			findBmuJava(w, d2, v, bmu, xdim, ydim, vdim);

			// update neurons one by one
			for (int i = 0; i < xdim; i++) {
				for (int j = 0; j < ydim; j++) {
					int ii = Math.abs(i - bmu[0]);
					int jj = Math.abs(j - bmu[1]);
					double g = lr * xGaussian[ii] * yGaussian[jj];
					int pos = getNeuronPos(i, j, xdim, ydim, vdim);
					for (int k = 0; k < vdim; k++) {
						float delta = batch[p + k] - w[pos + k];
						w[pos + k] = w[pos + k] + delta * (float) g;
					}
				}
			}
		}
	}

	public float findBmuJava(float[] w, float[] d2, float[] v, int[] bmu, int xdim, int ydim, int vdim) {
		for (int i = 0; i < xdim; i++) {
			for (int j = 0; j < ydim; j++) {
				int pos = (j * xdim + i) * vdim;
				int pos2 = j * xdim + i;
				d2[pos2] = 0.F;
				for (int k = 0; k < vdim; k++) {
					float delta = v[k] - w[pos + k];
					d2[pos2] += delta * delta;
				}
			}
		}

		float minValue = Float.MAX_VALUE;
		int x = -1;
		int y = -1;

		for (int i = 0; i < xdim; i++) {
			for (int j = 0; j < ydim; j++) {
				int pos2 = j * xdim + i;
				float d = d2[pos2];
				if (d < minValue) {
					minValue = d;
					x = i;
					y = j;
				}
			}
		}

		bmu[0] = x;
		bmu[1] = y;
		return minValue;
	}
}
