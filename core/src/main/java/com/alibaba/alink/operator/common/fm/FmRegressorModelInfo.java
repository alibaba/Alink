package com.alibaba.alink.operator.common.fm;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.FmDataFormat;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Model info of FmRegressor.
 */
public class FmRegressorModelInfo implements Serializable {

	private static final long serialVersionUID = 2561232397722095888L;
	protected boolean hasIntercept;
	protected boolean hasLinearItem;
	protected int numFactor;
	protected String task;
	protected int numFeature;
	protected FmDataFormat factors;
	protected String[] featureColNames;
	protected Object[] labelValues;

	public boolean hasIntercept() {
		return hasIntercept;
	}

	public boolean hasLinearItem() {
		return hasLinearItem;
	}

	public int getNumFactor() {
		return numFactor;
	}

	public String getTask() {
		return task;
	}

	public int getNumFeature() {
		return numFeature;
	}

	public double[][] getFactors() {
		return factors.factors;
	}

	public String[] getFeatureColNames() {
		return featureColNames;
	}

	public FmRegressorModelInfo(List <Row> rows) {
		FmModelData modelData = new FmModelDataConverter().load(rows);
		this.hasIntercept = modelData.dim[0] == 1;
		this.hasLinearItem = modelData.dim[1] == 1;
		this.numFactor = modelData.dim[2];
		this.task = modelData.task.toString();
		this.numFeature = modelData.vectorSize;
		this.featureColNames = modelData.featureColNames;
		this.factors = modelData.fmModel;
		processLabelValues(modelData);
	}

	protected void processLabelValues(FmModelData modelData) {
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder();
		DecimalFormat df = new DecimalFormat("#0.00000000");

		sbd.append(PrettyDisplayUtils.displayHeadline("model meta info", '-'));
		Map <String, String> map = new HashMap <>();
		map.put("numFeature", String.valueOf(numFeature));
		map.put("hasIntercept", String.valueOf(hasIntercept));
		map.put("hasLinearItem", String.valueOf(hasLinearItem));
		map.put("numFactor", String.valueOf(numFactor));
		double bias = factors.bias;
		map.put("bias", String.valueOf(bias));
		sbd.append(PrettyDisplayUtils.displayMap(map, 3, false) + "\n");

		if (labelValues != null && labelValues.length > 1) {
			sbd.append(PrettyDisplayUtils.displayHeadline("model label values", '-'));
			sbd.append(PrettyDisplayUtils.displayList(java.util.Arrays.asList(labelValues)) + "\n");
		}

		sbd.append(PrettyDisplayUtils.displayHeadline("model info", '-'));
		int k = numFactor;
		if (featureColNames != null) {
			int printSize = featureColNames.length < 10 ? featureColNames.length : 10;
			Object[][] out = new Object[printSize + 1][3];

			for (int i = 0; i < printSize; ++i) {
				out[i][0] = featureColNames[i];
				if (hasLinearItem) {
					out[i][1] = df.format(factors.factors[i][k]);
				} else {
					out[i][1] = df.format(0.0);
				}
				String factor = "";

				for (int j = 0; j < k; ++j) {
					factor += df.format(factors.factors[i][j]) + " ";
				}
				out[i][2] = factor;
			}
			if (featureColNames.length >= 10) {
				for (int i = 0; i < 3; ++i) {
					out[printSize - 1][i] = "... ...";
				}
			}

			sbd.append(PrettyDisplayUtils.displayTable(out, printSize, 3, null,
				new String[] {"colName", "linearItem", "factor"}, null,
				printSize, 3));
		} else {
			int printSize = numFeature < 10 ? numFeature : 10;
			Object[][] out = new Object[printSize + 1][3];

			for (int i = 0; i < printSize; ++i) {
				out[i][0] = String.valueOf(i);
				if (hasLinearItem) {
					out[i][1] = df.format(factors.factors[i][k]);
				} else {
					out[i][1] = df.format(0.0);
				}
				String factor = "";

				for (int j = 0; j < k; ++j) {
					factor += df.format(factors.factors[i][j]) + " ";
				}
				out[i][2] = factor;
			}
			if (numFeature >= 10) {
				for (int i = 0; i < 3; ++i) {
					out[printSize - 1][i] = "... ...";
				}
			}

			sbd.append(PrettyDisplayUtils.displayTable(out, printSize, 3, null,
				new String[] {"colName", "linearItem", "factor"}, null,
				printSize, 3));
		}

		return sbd.toString();
	}
}
