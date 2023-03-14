package com.alibaba.alink.operator.common.finance.stepwiseSelector;

import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.linear.LogistRegressionSummary;

public class ClassificationSelectorResult extends SelectorResult {
	public ClassificationSelectorStep[] entryVars;
	public LogistRegressionSummary modelSummary;

	@Override
	public String toVizData() {
		if (selectedIndices == null) {
			selectedIndices = new int[modelSummary.beta.size() - 1];
			for (int i = 0; i < selectedIndices.length; i++) {
				selectedIndices[i] = i;
			}
		}

		int featureSize = selectedIndices.length;

		VizData data = new VizData();
		data.entryVars = entryVars;

		if (data.entryVars == null) {
			data.entryVars = new ClassificationSelectorStep[0];
		}

		if (selectedCols != null && selectedCols.length != 0) {
			for (int i = 0; i < data.entryVars.length; i++) {
				data.entryVars[i].enterCol = selectedCols[i];
			}
		}

		//weight
		data.weights = new Weight[featureSize + 1];
		for (int i = 0; i < featureSize + 1; i++) {
			data.weights[i] = new Weight();
			data.weights[i].variable = getColName(selectedCols, selectedIndices, i);
			data.weights[i].weight = modelSummary.beta.get(i);
		}

		//model summary
		data.summary = new ModelSummary[3];
		data.summary[0] = new ModelSummary();
		data.summary[0].criterion = "AIC";
		data.summary[0].value = modelSummary.aic;

		data.summary[1] = new ModelSummary();
		data.summary[1].criterion = "SC";
		data.summary[1].value = modelSummary.sc;

		data.summary[2] = new ModelSummary();
		data.summary[2].criterion = "-2* LL";
		data.summary[2].value = 2 * modelSummary.loss;

		//para est
		data.paramEsts = new ParamEst[featureSize + 1];
		for (int i = 0; i < featureSize + 1; i++) {
			data.paramEsts[i] = new ParamEst();
			data.paramEsts[i].variable = getColName(selectedCols, selectedIndices, i);
			data.paramEsts[i].weight = String.valueOf(modelSummary.beta.get(i));
			data.paramEsts[i].stdEsts = String.valueOf(modelSummary.stdEsts[i]);
			data.paramEsts[i].stdErrs = String.valueOf(modelSummary.stdErrs[i]);
			data.paramEsts[i].chiSquareValue = String.valueOf(modelSummary.waldChiSquareValue[i]);
			data.paramEsts[i].pValue = String.valueOf(modelSummary.waldPValues[i]);
			data.paramEsts[i].lowerConfidence = String.valueOf(modelSummary.lowerConfidence[i]);
			data.paramEsts[i].uperConfidence = String.valueOf(modelSummary.uperConfidence[i]);
		}

		return JsonConverter.toJson(data);
	}

	static String getColName(String[] selectedCols, int[] selectedIndices, int id) {
		if (id == 0) {
			return "Intercept";
		} else {
			if (selectedCols == null || selectedCols.length == 0) {
				return String.valueOf(selectedIndices[id - 1]);
			} else {
				return selectedCols[id - 1];
			}
		}
	}

	private static class Weight implements AlinkSerializable {
		public String variable;
		public double weight;
	}

	private static class ParamEst implements AlinkSerializable {
		public String variable;
		public String weight;
		public String stdEsts;
		public String stdErrs;
		public String chiSquareValue;
		public String pValue;
		public String lowerConfidence;
		public String uperConfidence;
	}

	private static class ModelSummary implements AlinkSerializable {
		public String criterion;
		public double value;
	}

	private static class VizData implements AlinkSerializable {
		public Weight[] weights;
		public ParamEst[] paramEsts;
		public ModelSummary[] summary;
		public ClassificationSelectorStep[] entryVars;
	}

}
