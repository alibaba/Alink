/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.alink.operator.common.regression;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.common.statistics.statistics.SummaryResultTable;
import com.alibaba.alink.params.regression.LinearRegStepwiseTrainParams.Method;

import java.util.ArrayList;

/**
 * @author yangxu
 */
public class LinearRegressionStepwise {

	public static LinearRegressionStepwiseModel step(SummaryResultTable srt, String nameY, String[] nameX,
													 Method selection) throws Exception {
		return step(srt, nameY, nameX, selection, Double.NaN, Double.NaN);
	}

	public static LinearRegressionStepwiseModel step(SummaryResultTable srt, String nameY, String[] nameX,
													 Method selection, double alphaEntry, double alphaStay)
		throws Exception {
		if (null == nameX || null == nameY) {
			throw new RuntimeException();
		}
		LinearRegressionStepwiseModel lrsr = null;
		switch (selection) {
			case Forward:
				if (Double.isNaN(alphaEntry)) {
					alphaEntry = 0.2;
				}
				lrsr = stepForward(srt, nameY, nameX, alphaEntry, alphaStay);
				break;
			case Backward:
				if (Double.isNaN(alphaStay)) {
					alphaStay = 0.1;
				}
				lrsr = stepBackward(srt, nameY, nameX, alphaEntry, alphaStay);
				break;
			case Stepwise:
				if (Double.isNaN(alphaEntry)) {
					alphaEntry = 0.1;
				}
				if (Double.isNaN(alphaStay)) {
					alphaStay = 0.15;
				}
				lrsr = stepStepwise(srt, nameY, nameX, alphaEntry, alphaStay);
				break;
			default:
				throw new RuntimeException("Not implemented yet!");
		}

		return lrsr;
	}

	public static LinearRegressionStepwiseModel step(SummaryResultTable srt, String nameY, String[] nameX,
													 StepCriterion crit) throws Exception {
		java.io.CharArrayWriter cw = new java.io.CharArrayWriter();
		java.io.PrintWriter pw = new java.io.PrintWriter(cw, true);
		pw.println("\nSelected Criterion : " + crit + "\n");
		int nx = nameX.length;
		if (nx > 13) {
			throw new RuntimeException("Not implemented yet!");
		}
		int nSet = (1 << nx) - 1;

		LinearRegressionModel lrrall = LinearReg.train(srt, nameY, nameX);

		double maxValue = Double.NEGATIVE_INFINITY;
		switch (crit) {
			//下面两个准则的值，希望达到最大
			case R2:
			case AdjR2:
				maxValue = Double.NEGATIVE_INFINITY;
				break;
			//以下准则的值，希望达到最小
			case SSE:
			case MSE:
			case AIC:
			case Cp:
				maxValue = Double.POSITIVE_INFINITY;
				break;
			default:
				throw new RuntimeException("Not implemented yet!");
		}

		LinearRegressionModel lrrbest = null;
		for (int k = 1; k <= nSet; k++) {
			ArrayList <String> cols = new ArrayList <String>();
			int subk = k;
			for (int i = 0; i < nx; i++) {
				if (subk % 2 == 1) {
					cols.add(nameX[i]);
				}
				subk = subk >> 1;
			}
			LinearRegressionModel lrrcur = LinearReg.train(srt, nameY, cols.toArray(new String[0]));
			double valCrit = Double.NaN;
			switch (crit) {
				case SSE:
					valCrit = lrrcur.SSE;
					break;
				case MSE:
					valCrit = lrrcur.MSE;
					break;
				case R2:
					valCrit = lrrcur.R2;
					break;
				case AdjR2:
					valCrit = lrrcur.Ra2;
					break;
				case AIC:
					valCrit = lrrcur.AIC;
					break;
				case Cp:
					valCrit = lrrcur.getCp(nx, lrrall.SSE);
					break;
				default:
					throw new RuntimeException("Not implemented yet!");
			}
			pw.println(cols + " : " + valCrit);
			switch (crit) {
				//下面两个准则的值，希望达到最大
				case R2:
				case AdjR2:
					if (valCrit > maxValue) {
						maxValue = valCrit;
						lrrbest = lrrcur;
					}
					break;
				//以下准则的值，希望达到最小
				case SSE:
				case MSE:
				case AIC:
				case Cp:
					if (valCrit < maxValue) {
						maxValue = valCrit;
						lrrbest = lrrcur;
					}
					break;
				default:
					throw new RuntimeException("Not implemented yet!");
			}
		}

		return new LinearRegressionStepwiseModel(lrrbest, cw.toString());
	}

	private static LinearRegressionStepwiseModel stepForward(SummaryResultTable srt, String nameY, String[] nameX,
															 double alphaEntry, double alphaStay) throws Exception {
		java.io.CharArrayWriter cw = new java.io.CharArrayWriter();
		java.io.PrintWriter pw = new java.io.PrintWriter(cw, true);
		int nx = nameX.length;
		double maxValue = Double.NEGATIVE_INFINITY;
		LinearRegressionModel lrrbest = null;
		ArrayList <String> selected = new ArrayList <String>();
		ArrayList <String> rest = new ArrayList <String>();
		for (int k = 0; k < nx; k++) {
			rest.add(nameX[k]);
		}
		for (int k = 0; k < nx; k++) {
			int indexRestset = -1;
			for (int i = 0; i < rest.size(); i++) {
				ArrayList <String> cols = new ArrayList <String>();
				cols.addAll(selected);
				cols.add(rest.get(i));
				LinearRegressionModel lrrcur = LinearReg.train(srt, nameY, cols.toArray(new String[0]));
				pw.println(cols + " : " + k + " : " + i + " : " + lrrcur.F);
				if (lrrcur.F > maxValue && lrrcur.pEquation < alphaEntry) {
					maxValue = lrrcur.F;
					lrrbest = lrrcur;
					indexRestset = i;
				}
			}
			if (indexRestset >= 0) {
				selected.add(rest.get(indexRestset));
				rest.remove(indexRestset);
			} else {
				break;
			}
		}

		return new LinearRegressionStepwiseModel(lrrbest, cw.toString());
	}

	private static LinearRegressionStepwiseModel stepBackward(SummaryResultTable srt, String nameY, String[] nameX,
															  double alphaEntry, double alphaStay) throws Exception {
		java.io.CharArrayWriter cw = new java.io.CharArrayWriter();
		java.io.PrintWriter pw = new java.io.PrintWriter(cw, true);
		int nx = nameX.length;
		double maxValue = Double.NEGATIVE_INFINITY;
		LinearRegressionModel lrrbest = null;
		ArrayList <String> selected = new ArrayList <String>();
		ArrayList <String> rest = new ArrayList <String>();
		for (int k = 0; k < nx; k++) {
			selected.add(nameX[k]);
		}
		for (int k = 0; k < nx; k++) {
			int indexRestset = -1;
			for (int i = 0; i < selected.size(); i++) {
				ArrayList <String> cols = new ArrayList <String>();
				cols.addAll(selected);
				cols.remove(i);
				LinearRegressionModel lrrcur = LinearReg.train(srt, nameY, cols.toArray(new String[0]));
				pw.println(cols + " : " + k + " : " + i + " : " + lrrcur.F);
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println(cols + " : " + k + " : " + i + " : " + lrrcur.F);
				}
				if (lrrcur.F > maxValue && lrrcur.pEquation < alphaStay) {
					maxValue = lrrcur.F;
					lrrbest = lrrcur;
					indexRestset = i;
				}
			}
			if (indexRestset >= 0) {
				rest.add(selected.get(indexRestset));
				selected.remove(indexRestset);
			} else {
				break;
			}
		}

		return new LinearRegressionStepwiseModel(lrrbest, cw.toString());
	}

	private static LinearRegressionStepwiseModel stepStepwise(SummaryResultTable srt, String nameY, String[] nameX,
															  double alphaEntry, double alphaStay) throws Exception {
		java.io.CharArrayWriter cw = new java.io.CharArrayWriter();
		java.io.PrintWriter pw = new java.io.PrintWriter(cw, true);
		int nx = nameX.length;
		double maxValue = Double.NEGATIVE_INFINITY;
		LinearRegressionModel lrrbest = null;
		ArrayList <String> selected = new ArrayList <String>();
		ArrayList <String> rest = new ArrayList <String>();
		for (int k = 0; k < nx; k++) {
			rest.add(nameX[k]);
		}
		for (int k = 0; k < nx; k++) {
			int indexRestset = -1;
			LinearRegressionModel lrr = null;
			for (int i = 0; i < rest.size(); i++) {
				ArrayList <String> cols = new ArrayList <String>();
				cols.addAll(selected);
				cols.add(rest.get(i));
				LinearRegressionModel lrrcur = LinearReg.train(srt, nameY, cols.toArray(new String[0]));
				pw.println(cols + " : " + k + " : " + i + " : " + lrrcur.F);
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println(cols + " : " + k + " : " + i + " : " + lrrcur.F + " " + lrrcur.pEquation);
				}
				if (lrrcur.F > maxValue && lrrcur.pEquation < alphaEntry) {
					maxValue = lrrcur.F;
					lrr = lrrcur;
					indexRestset = i;
				}
			}
			if (indexRestset >= 0) {
				String coladd = rest.get(indexRestset);
				selected.add(coladd);
				rest.remove(indexRestset);
				ArrayList <String> deleted = new ArrayList <String>();
				for (int i = 0; i < lrr.nameX.length; i++) {
					if (lrr.pX[i] > alphaStay) {
						String colrmv = lrr.nameX[i];
						deleted.add(colrmv);
						selected.remove(colrmv);
						rest.add(colrmv);
					}
				}
				if (deleted.size() == 1 && deleted.get(0).equals(coladd)) {
					break;
				} else {
					lrrbest = lrr;
				}
			} else {
				break;
			}
		}

		return new LinearRegressionStepwiseModel(lrrbest, cw.toString());
	}

	public enum StepCriterion {

		/***
		 * 多重判定系数, 该值最大为准则
		 */
		R2,
		/***
		 * 调整的多重判定系数, 该值最大为准则
		 */
		AdjR2,
		/***
		 * 误差平方和, 该值最小为准则
		 */
		SSE,
		/***
		 * 误差均方，, 该值最小为准则
		 */
		MSE,
		/***
		 * AIC（Akaike Information Criterion）信息统计量, 该值最小为准则
		 */
		AIC,
		/***
		 * Cp统计量, 该值最小为准则
		 */
		Cp;
	}
}
