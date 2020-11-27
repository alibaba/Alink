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
 * Linear Regressor (lasso, ridge, linearReg) model info.
 */
public class LinearRegressorModelInfo implements Serializable {
	private static final long serialVersionUID = 1587799722352066332L;
	private String[] featureNames;
	private String vectorColName;
	private DenseVector coefVector;
	private int vectorSize;
	private String modelName;
	private boolean hasInterceptItem;
	private static final int WIDTH = 10;
	private static final int NAX_NUM_LAYER = 5;

	protected Object[] labelValues;

	public String[] getFeatureNames() {
		return featureNames;
	}

	public String getVectorColName() {
		return vectorColName;
	}

	public DenseVector getWeight() {
		return coefVector;
	}

	public int getVectorSize() {
		return vectorSize;
	}

	public String getModelName() {
		return modelName;
	}

	public boolean hasInterceptItem() {
		return hasInterceptItem;
	}

	public LinearRegressorModelInfo(List <Row> rows) {
		LinearModelData modelData = new LinearModelDataConverter().load(rows);
		featureNames = modelData.featureNames;
		vectorColName = modelData.vectorColName;
		coefVector = modelData.coefVector;
		vectorSize = modelData.vectorSize;
		modelName = modelData.modelName;
		hasInterceptItem = modelData.hasInterceptItem;
	}

	protected void processLabelValues(LinearModelData modelData) {
	}

	@Override
	public String toString() {
		DecimalFormat df = new DecimalFormat("#0.00000000");
		StringBuilder ret = new StringBuilder();
		Map <String, String> map = new HashMap <>();
		map.put("model name", modelName);
		map.put("num feature", String.valueOf(vectorSize));
		if (vectorColName != null) {
			map.put("vector colName", vectorColName);
		}
		map.put("hasInterception", String.valueOf(hasInterceptItem));
		ret.append(PrettyDisplayUtils.displayHeadline("model meta info", '-'));
		ret.append(PrettyDisplayUtils.displayMap(map, WIDTH, false) + "\n");

		if (labelValues != null && labelValues.length > 1) {
			ret.append(PrettyDisplayUtils.displayHeadline("model label values", '-'));
			ret.append(PrettyDisplayUtils.displayList(java.util.Arrays.asList(labelValues)) + "\n");
		}

		ret.append(PrettyDisplayUtils.displayHeadline("model weight info", '-'));
		if (coefVector.size() <= 10) {
			Object[][] out = new Object[1][coefVector.size()];
			String[] tableColNames = new String[coefVector.size()];
			int startIdx = 0;
			if (hasInterceptItem) {
				startIdx = 1;
				tableColNames[0] = "intercept";
				out[0][0] = coefVector.get(0);
			}
			for (int i = startIdx; i < coefVector.size(); ++i) {
				tableColNames[i] = featureNames != null ? featureNames[i - startIdx] : String.valueOf(i);
				out[0][i] = df.format(coefVector.get(i));
			}
			ret.append(PrettyDisplayUtils.displayTable(out, 1, coefVector.size(),
				null, tableColNames, null, 1, coefVector.size()));
		} else if (coefVector.size() <= WIDTH * NAX_NUM_LAYER) {
			int numLayer = coefVector.size() / WIDTH + (coefVector.size() % WIDTH == 0 ? 0 : 1);
			Object[][] out = new Object[numLayer * 2][WIDTH];
			String[] tableColNames = new String[numLayer * 2];
			for (int i = 0; i < numLayer; ++i) {
				int endidx = Math.min(((i + 1) * WIDTH - 1), coefVector.size() - 1);
				tableColNames[2 * i] = "colName[" + (i * WIDTH) + "," + endidx + "]";
				tableColNames[2 * i + 1] = "weight[" + (i * WIDTH) + "," + endidx + "]";
			}
			int startIdx = 0;
			for (int l = 0; l < numLayer; ++l) {
				int endIdx = Math.min(WIDTH, coefVector.size() - l * WIDTH);
				if (l == 0) {
					if (hasInterceptItem) {
						startIdx = 1;
						out[0][0] = "intercept";
						out[1][0] = coefVector.get(0);
					}
					for (int i = startIdx; i < endIdx; ++i) {
						out[0][i] = featureNames != null ? featureNames[i - startIdx] : String.valueOf(i - startIdx);
						out[1][i] = df.format(coefVector.get(i));
					}
				} else {
					for (int i = 0; i < endIdx; ++i) {
						out[2 * l][i] = featureNames != null ? featureNames[l * WIDTH + i - startIdx]
							: String.valueOf(l * WIDTH + i - startIdx);
						out[2 * l + 1][i] = df.format(coefVector.get(l * WIDTH + i));
					}
				}
				for (int i = endIdx; i < WIDTH; ++i) {
					out[2 * l][i] = "";
					out[2 * l + 1][i] = "";
				}
			}
			ret.append(PrettyDisplayUtils.displayTable(out, numLayer * 2, WIDTH,
				tableColNames, null, null, numLayer * 2, WIDTH));
		} else {
			int startIdx = 0;
			Object[][] out = new Object[NAX_NUM_LAYER * 2][WIDTH];
			String[] tableColNames = new String[NAX_NUM_LAYER * 2];
			for (int i = 0; i < NAX_NUM_LAYER; ++i) {
				int endidx = Math.min(((i + 1) * WIDTH - 1), coefVector.size() - 1);
				tableColNames[2 * i] = "colName[" + (i * WIDTH) + "," + endidx + "]";
				tableColNames[2 * i + 1] = "weight[" + (i * WIDTH) + "," + endidx + "]";
			}
			for (int l = 0; l < NAX_NUM_LAYER; ++l) {
				if (l == 0) {
					if (hasInterceptItem) {
						startIdx = 1;
						out[0][0] = "intercept";
						out[1][0] = coefVector.get(0);
					}
					for (int i = startIdx; i < WIDTH - 1; ++i) {
						out[0][i] = featureNames != null ? featureNames[i - startIdx] : String.valueOf(i - startIdx);
						out[1][i] = df.format(coefVector.get(i));
					}
				} else {
					for (int i = 0; i < WIDTH - 1; ++i) {
						out[2 * l][i] = featureNames != null ? featureNames[l * WIDTH + i - startIdx]
							: String.valueOf(l * WIDTH + i - startIdx);
						out[2 * l + 1][i] = df.format(coefVector.get(l * WIDTH + i));
					}
				}
				if (l == NAX_NUM_LAYER - 1) {
					out[l * 2][WIDTH - 1] = "... ...";
					out[l * 2 + 1][WIDTH - 1] = "... ...";
				} else if (l == 0) {
					out[0][WIDTH - 1] = featureNames != null ? featureNames[WIDTH - 1 - startIdx]
						: String.valueOf(WIDTH - 1 - startIdx);
					out[1][WIDTH - 1] = df.format(coefVector.get(WIDTH - 1));
				} else {
					out[2 * l][WIDTH - 1] = featureNames != null ? featureNames[l * WIDTH + WIDTH - 1 - startIdx]
						: String.valueOf(l * WIDTH + WIDTH - 1 - startIdx);
					out[2 * l + 1][WIDTH - 1] = df.format(coefVector.get(l * WIDTH + WIDTH - 1));
				}
			}
			ret.append(PrettyDisplayUtils.displayTable(out, NAX_NUM_LAYER * 2, WIDTH,
				tableColNames, null, null, NAX_NUM_LAYER * 2, WIDTH));
		}
		return ret.toString();
	}
}
