package com.alibaba.alink.operator.common.finance.stepwiseSelector;

import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.linear.LinearRegressionSummary;

public class RegressionSelectorResult extends SelectorResult {
	public RegressionSelectorStep[] entryVars;
	public LinearRegressionSummary modelSummary;
	public int[] deleteIndices;

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
			data.entryVars = new RegressionSelectorStep[0];
		}

		if (selectedCols != null && selectedCols.length != 0) {
			for (int i = 0; i < data.entryVars.length; i++) {
				data.entryVars[i].enterCol = selectedCols[i];
			}
		}

		//weight
		data.weights = new Weight[featureSize + 1];

		data.weights[0] = new Weight();
		data.weights[0].variable = "Intercept";
		data.weights[0].weight = modelSummary.beta.get(0);

		for (int i = 0; i < featureSize; i++) {
			data.weights[i + 1] = new Weight();
			if (selectedCols != null && selectedCols.length != 0) {
				data.weights[i + 1].variable = selectedCols[i];
			} else {
				data.weights[i + 1].variable = String.valueOf(selectedIndices[i]);
			}
			data.weights[i + 1].weight = modelSummary.beta.get(i + 1);
		}

		//model summary
		data.summary = new ModelSummary[2];
		data.summary[0] = new ModelSummary();
		data.summary[0].criterion = "r2";
		data.summary[0].value = modelSummary.r2;

		data.summary[1] = new ModelSummary();
		data.summary[1].criterion = "ra2";
		data.summary[1].value = modelSummary.ra2;

		//para est
		data.paramEsts = new ParamEst[featureSize + 1];
		data.paramEsts[0] = new ParamEst();

		data.paramEsts[0].variable = "Intercept";
		data.paramEsts[0].weight = String.valueOf(modelSummary.beta.get(0));
		data.paramEsts[0].stdEsts = "-";
		data.paramEsts[0].stdErrs = "-";
		data.paramEsts[0].tValues = "-";
		data.paramEsts[0].pValue = "-";
		data.paramEsts[0].lowerConfidence = "-";
		data.paramEsts[0].uperConfidence = "-";

		for (int i = 0; i < featureSize; i++) {
			data.paramEsts[i + 1] = new ParamEst();
			if (selectedCols != null && selectedCols.length != 0) {
				data.paramEsts[i + 1].variable = selectedCols[i];
			} else {
				data.paramEsts[i + 1].variable = String.valueOf(selectedIndices[i]);
			}
			data.paramEsts[i + 1].weight = String.valueOf(modelSummary.beta.get(i + 1));
			data.paramEsts[i + 1].stdEsts = String.valueOf(modelSummary.stdEsts[i]);
			data.paramEsts[i + 1].stdErrs = String.valueOf(modelSummary.stdErrs[i]);
			data.paramEsts[i + 1].tValues = String.valueOf(modelSummary.tValues[i]);
			data.paramEsts[i + 1].pValue = String.valueOf(modelSummary.tPVaues[i]);
			data.paramEsts[i + 1].lowerConfidence = String.valueOf(modelSummary.lowerConfidence[i]);
			data.paramEsts[i + 1].uperConfidence = String.valueOf(modelSummary.uperConfidence[i]);
		}

		return JsonConverter.toJson(data);
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
		public String tValues;
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
		public RegressionSelectorStep[] entryVars;
	}

}
