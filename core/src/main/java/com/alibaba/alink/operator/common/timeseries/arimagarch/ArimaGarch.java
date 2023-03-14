package com.alibaba.alink.operator.common.timeseries.arimagarch;

import com.alibaba.alink.operator.common.timeseries.TsMethod;
import com.alibaba.alink.operator.common.timeseries.teststatistics.KPSS;
import com.alibaba.alink.operator.common.timeseries.teststatistics.StationaryTest;
import com.alibaba.alink.params.timeseries.HasArimaGarchMethod;
import com.alibaba.alink.params.timeseries.HasArimaGarchMethod.ArimaGarchMethod;
import com.alibaba.alink.params.timeseries.HasIcType;
import com.alibaba.alink.params.timeseries.HasIcType.IcType;

public class ArimaGarch {

	public static ArimaGarchModel fit(double[] data, int[] order1, int[] order2, int ifIntercept) {
		if (data == null) {
			throw new RuntimeException("Data is not valid");
		}

		if (order1[0] < 0 || order1[1] < 0 || order1[2] < 0) {
			throw new RuntimeException("Order for arima must equal to or Greater than 0");
		}
		if (order2[0] < 0 || order2[1] < 0) {
			throw new RuntimeException("Order for garch must equal to or Greater than 0");
		}
		if (order2[0] == 0 && order2[1] == 0) {
			throw new RuntimeException("Order for garch can not be 0 simultaneously");
		}
		if (data.length - order1[0] - order1[1] - order2[1]
			< order1[0] + order1[2] + order2[0] + order2[1] + 1 + ifIntercept) {
			throw new RuntimeException("Do not have enough data");
		}

		ArimaGarchModel model = new ArimaGarchModel();
		model.data = data.clone();
		int d = order1[1];
		double[] dData;
		if (d == 0) {
			dData = data.clone();
			model.dData = new double[][] {};
		} else {
			double[][] difference = new double[d + 1][data.length];
			difference[0] = model.data.clone();

			//d part
			for (int i = 1; i <= d; i++) {
				difference[i] = TsMethod.diff(i, difference[i - 1]);
			}

			model.dData = difference.clone();
			dData = new double[data.length - d];
			for (int i = 0; i < dData.length; i++) {
				dData[i] = difference[d][i + d];
			}
		}

		int[] orderARIMA = {order1[0], order1[2]};
		int[] orderGARCH = order2;
		ConsistEstimate cEst = new ConsistEstimate();
		cEst.bfgsEstimate(dData, orderARIMA, orderGARCH, ifIntercept);

		model.mi = cEst.mi.copy();
		model.mi.order = new int[] {order1[0], order1[1], order1[2], order2[0], order2[1]};

		return model;
	}

	/**
	 * order1: order of arima part
	 * order2: order of garch part
	 *
	 * method: "SEPARATE": (first fit arima and then use residual to fit garch)
	 * "CONSIST":(fit arima and garch simultaneously)
	 */
	public static ArimaGarchModel autoFit(double[] data,
										  IcType ic,
										  ArimaGarchMethod method,
										  int maxARIMA,
										  int maxGARCH,
										  boolean ifGARCH11) {

		ArimaGarchModel arimaGarchModel = new ArimaGarchModel();
		switch (method) {
			case SEPARATE:
				SeparateEstimate sEst = new SeparateEstimate();
				sEst.bfgsEstimate(data, ic, maxARIMA, maxGARCH, ifGARCH11);
				arimaGarchModel.mi = sEst.mi;
				arimaGarchModel.arima = sEst.arima;
				arimaGarchModel.garch = sEst.garch;
				break;
			case CONSIST:
				arimaGarchModel = consistAutoSelection(data, ic, maxARIMA, maxGARCH, ifGARCH11);
				break;
		}
		return arimaGarchModel;
	}

	private static ArimaGarchModel consistAutoSelection(double[] data,
												IcType ic,
												int maxArima,
												int maxGarch,
												boolean ifGarch11) {
		//step 1: find d
		int diff = 0;
		double[] data1 = data.clone();
		StationaryTest st = new StationaryTest();
		for (int i = 0; i < 6; i++) {
			KPSS kpss = st.kpss(data1);
			if (kpss.cnValue < kpss.cnCritic) {
				break;
			}
			if (kpss.cnValue >= kpss.cnCritic) {
				diff += 1;
				data1 = st.differenceFromZero(data1);
			}
		}

		if (diff == 6) {
			throw new RuntimeException("1-lag difference can not change data to stationary series.");
		}
		int d = diff;

		double bestIC = Double.MAX_VALUE;
		int ifIntercept = d > 0 ? 0 : 1;

		ArimaGarchModel bestAGM = new ArimaGarchModel();

		if (ifGarch11) {
			for (int p = 0; p <= maxArima; p++) {
				for (int q = 0; q <= maxArima; q++) {
					ArimaGarchModel agm = ArimaGarch.fit(data,
						new int[] {p, d, q},
						new int[] {1, 1},
						ifIntercept);
					double thisIC = TsMethod.icCompute(agm.mi.loglike,
						p + q + 3 + ifIntercept,
						data.length - Math.max(agm.mi.order[0], agm.mi.order[2]),
						ic, Boolean.TRUE);
					if (thisIC < bestIC) {
						bestIC = thisIC;
						bestAGM = agm;
					}
				}
			}
		} else {
			for (int p = 0; p < maxArima; p++) {
				for (int q = 0; q < maxArima; q++) {
					for (int a = 0; a < maxGarch; a++) {
						for (int b = 0; b < maxGarch; b++) {
							if (a == 0 && b == 0) {
								continue;
							}
							ArimaGarchModel agm = ArimaGarch.fit(data,
								new int[] {p, d, q},
								new int[] {a, b},
								ifIntercept);
							double thisIC = TsMethod.icCompute(agm.mi.loglike, p + q + a + b + 1 + ifIntercept,
								data.length - Math.max(agm.mi.order[0], agm.mi.order[2]), ic, Boolean.TRUE);
							if (thisIC < bestIC) {
								bestIC = thisIC;
							}
							bestAGM = agm;
						}
					}
				}
			}
		}

		bestAGM.mi.ic = bestIC;
		return bestAGM;
	}

}
