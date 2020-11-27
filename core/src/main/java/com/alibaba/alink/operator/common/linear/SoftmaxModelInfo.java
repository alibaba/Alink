package com.alibaba.alink.operator.common.linear;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Softmax model info.
 */
public class SoftmaxModelInfo implements Serializable {
	private static final long serialVersionUID = 1587799722352066332L;
	private String[] featureNames;
	private String vectorColName;
	private int vectorSize;
	private String modelName;
	private Object[] labelValues;
	private boolean hasInterceptItem = true;
	private static final int WIDTH = 10;
	private static final int DEPTH = 5;
	private DenseVector[] coefVectors;

	public SoftmaxModelInfo(List <Row> rows) {
		LinearModelData modelData = new LinearModelDataConverter().load(rows);
		featureNames = modelData.featureNames;
		vectorColName = modelData.vectorColName;
		coefVectors = modelData.coefVectors;
		vectorSize = modelData.vectorSize;
		modelName = modelData.modelName;
		labelValues = modelData.labelValues;
		hasInterceptItem = modelData.hasInterceptItem;
	}

	public DenseVector[] getWeights() {
		return coefVectors;
	}

	public String[] getFeatureNames() {
		return featureNames;
	}

	public String getVectorColName() {
		return vectorColName;
	}

	public int getVectorSize() {
		return vectorSize;
	}

	public String getModelName() {
		return modelName;
	}

	public Object[] getLabelValues() {
		return labelValues;
	}

	public boolean hasInterceptItem() {
		return hasInterceptItem;
	}

	@Override
	public String toString() {
		DecimalFormat df = new DecimalFormat("#0.00000000");
		StringBuilder sbd = new StringBuilder();
		Map <String, String> map = new HashMap <>();
		map.put("model name", modelName);
		map.put("num feature", String.valueOf(vectorSize));
		if (vectorColName != null) {
			map.put("vector colName", vectorColName);
		}
		map.put("hasInterception", String.valueOf(hasInterceptItem));
		sbd.append(PrettyDisplayUtils.displayHeadline("model meta info", '-'));
		sbd.append(PrettyDisplayUtils.displayMap(map, WIDTH, false) + "\n");

		if (labelValues != null && labelValues.length > 1) {
			sbd.append(PrettyDisplayUtils.displayHeadline("model label values", '-'));
			sbd.append(PrettyDisplayUtils.displayList(java.util.Arrays.asList(labelValues)) + "\n");
		}

		sbd.append(PrettyDisplayUtils.displayHeadline("model weight info", '-'));
		if (coefVectors.length < DEPTH) {
			if (coefVectors[0].size() < WIDTH) {
				Object[][] out = new Object[coefVectors.length][coefVectors[0].size()];
				String[] tableColNames = new String[coefVectors[0].size()];
				int startIdx = 0;
				if (hasInterceptItem) {
					startIdx = 1;
					tableColNames[0] = "intercept";
					for (int i = 0; i < coefVectors.length; ++i) {
						out[i][0] = coefVectors[i].get(0);
					}
				}
				for (int i = startIdx; i < coefVectors[0].size(); ++i) {
					tableColNames[i] = featureNames != null ? featureNames[i - startIdx] : String.valueOf(i);
					for (int j = 0; j < coefVectors.length; ++j) {
						out[j][i] = df.format(coefVectors[j].get(i));
					}
				}
				sbd.append(PrettyDisplayUtils.displayTable(out, coefVectors.length, coefVectors[0].size(),
					null, tableColNames, null, coefVectors.length, coefVectors[0].size()));
			} else {
				Object[][] out = new Object[coefVectors.length][WIDTH];
				String[] tableColNames = new String[WIDTH];
				int startIdx = 0;
				if (hasInterceptItem) {
					startIdx = 1;
					tableColNames[0] = "intercept";
					for (int i = 0; i < coefVectors.length; ++i) {
						out[i][0] = coefVectors[i].get(0);
					}
				}
				for (int i = startIdx; i < WIDTH - 1; ++i) {
					tableColNames[i] = featureNames != null ? featureNames[i - startIdx] : String.valueOf(i);
					for (int j = 0; j < coefVectors.length; ++j) {
						out[j][i] = df.format(coefVectors[j].get(i));
					}
				}
				tableColNames[WIDTH - 1] = "... ...";
				for (int j = 0; j < coefVectors.length; ++j) {
					out[j][WIDTH - 1] = "... ...";
				}
				sbd.append(PrettyDisplayUtils.displayTable(out, coefVectors.length, WIDTH,
					null, tableColNames, null, coefVectors.length, WIDTH));
			}
		} else {
			if (coefVectors[0].size() < WIDTH) {
				Object[][] out = new Object[DEPTH][coefVectors[0].size()];
				String[] tableColNames = new String[coefVectors[0].size()];
				int startIdx = 0;
				if (hasInterceptItem) {
					startIdx = 1;
					tableColNames[0] = "intercept";
					for (int i = 0; i < DEPTH - 1; ++i) {
						out[i][0] = coefVectors[i].get(0);
					}
					out[DEPTH - 1][0] = "... ...";
				}
				for (int i = startIdx; i < coefVectors[0].size(); ++i) {
					tableColNames[i] = featureNames != null ? featureNames[i - startIdx] : String.valueOf(i);
					for (int j = 0; j < DEPTH - 1; ++j) {
						out[j][i] = df.format(coefVectors[j].get(i));
					}
					out[DEPTH - 1][i] = "... ...";
				}

				sbd.append(PrettyDisplayUtils.displayTable(out, DEPTH, coefVectors[0].size(),
					null, tableColNames, null, DEPTH, coefVectors[0].size()));
			} else {
				Object[][] out = new Object[DEPTH][WIDTH];
				String[] tableColNames = new String[WIDTH];
				int startIdx = 0;
				if (hasInterceptItem) {
					startIdx = 1;
					tableColNames[0] = "intercept";
					for (int i = 0; i < DEPTH - 1; ++i) {
						out[i][0] = coefVectors[i].get(0);
					}
					out[DEPTH - 1][0] = "... ...";
				}
				for (int i = startIdx; i < WIDTH - 1; ++i) {
					tableColNames[i] = featureNames != null ? featureNames[i - startIdx] : String.valueOf(i);
					for (int j = 0; j < DEPTH - 1; ++j) {
						out[j][i] = df.format(coefVectors[j].get(i));
					}
					out[DEPTH - 1][i] = "... ...";
				}
				tableColNames[WIDTH - 1] = "... ...";
				for (int j = 0; j < DEPTH; ++j) {
					out[j][WIDTH - 1] = "... ...";
				}
				sbd.append(PrettyDisplayUtils.displayTable(out, DEPTH, WIDTH,
					null, tableColNames, null, DEPTH, WIDTH));
			}
		}
		return sbd.toString();
	}
}
