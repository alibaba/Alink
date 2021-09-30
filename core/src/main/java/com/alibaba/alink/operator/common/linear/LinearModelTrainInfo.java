package com.alibaba.alink.operator.common.linear;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Linear model train info.
 */
public final class LinearModelTrainInfo implements Serializable {

	private static final long serialVersionUID = 7999201781768270042L;
	private String[] convInfo;
	private Params meta;
	private String[] colNames;
	private double[] weight;
	private double[] importance;

	public LinearModelTrainInfo(List <Row> rows) {
		DecimalFormat df = new DecimalFormat("#0.00000000");
		for (Row r : rows) {
			if ((int) r.getField(0) == 0) {
				this.meta = JsonConverter.fromJson((String) r.getField(1), Params.class);
			} else if ((int) r.getField(0) == 1) {
				colNames = JsonConverter.fromJson((String) r.getField(1), String[].class);
			} else if ((int) r.getField(0) == 2) {
				weight = JsonConverter.fromJson((String) r.getField(1), double[].class);
			} else if ((int) r.getField(0) == 3) {
				importance = JsonConverter.fromJson((String) r.getField(1), double[].class);
			} else if ((int) r.getField(0) == 4) {
				double[] cinfo = JsonConverter.fromJson((String) r.getField(1), double[].class);
				int size = cinfo.length / 3;
				this.convInfo = new String[size];
				for (int i = 0; i < size; ++i) {
					this.convInfo[i] = "step:" + i + " loss:" + df.format(cinfo[3 * i])
						+ " gradNorm:" + df.format(cinfo[3 * i + 1]) + " learnRate:" + df.format(cinfo[3 * i + 2]);
				}
			}
		}
	}

	public String[] getConvInfo() {
		return convInfo;
	}

	public Params getMeta() {
		return meta;
	}

	public String[] getColNames() {
		return colNames;
	}

	public double[] getWeight() {
		return weight;
	}

	public double[] getImportance() {
		return importance;
	}

	private List <Tuple2 <String, Double>> getWeightList() {
		List <Tuple2 <String, Double>> weightList = new ArrayList <>();
		if (weight.length == importance.length) {
			for (int i = 0; i < weight.length; ++i) {
				weightList.add(Tuple2.of(colNames[i], weight[i]));
			}
		} else {
			for (int i = 0; i < importance.length; ++i) {
				weightList.add(Tuple2.of(colNames[i], weight[i + 1]));
			}
		}
		weightList.sort(compare);
		return weightList;
	}

	private List <Tuple2 <String, Double>> getImportanceList() {
		List <Tuple2 <String, Double>> importanceList = new ArrayList <>();
		if (weight.length == importance.length) {
			for (int i = 0; i < weight.length; ++i) {
				importanceList.add(Tuple2.of(colNames[i], importance[i]));
			}
		} else {
			for (int i = 0; i < importance.length; ++i) {
				importanceList.add(Tuple2.of(colNames[i], importance[i]));
			}
		}
		importanceList.sort(compare);
		return importanceList;
	}

	private static Comparator compare = new Comparator <Tuple2 <String, Double>>() {
		@Override
		public int compare(Tuple2 <String, Double> o1, Tuple2 <String, Double> o2) {
			if (o1.f1 < o2.f1) {
				return 1;
			} else if (o1.f1 > o2.f1) {
				return -1;
			} else {
				return 0;
			}
		}
	};

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder();

		sbd.append(PrettyDisplayUtils.displayHeadline("train meta info", '-'));
		Map <String, String> map = new HashMap <>();
		map.put("model name", meta.get(ModelParamName.MODEL_NAME));
		map.put("num feature", meta.get(ModelParamName.VECTOR_SIZE).toString());
		sbd.append(PrettyDisplayUtils.displayMap(map, 2, false) + "\n");

		if (!(meta.get(ModelParamName.MODEL_NAME).equals("softmax"))) {
			sbd.append(PrettyDisplayUtils.displayHeadline("train importance info", '-'));
			DecimalFormat df = new DecimalFormat("#0.00000000");
			List <Tuple2 <String, Double>> weightList = getWeightList();
			List <Tuple2 <String, Double>> importanceList = getImportanceList();

			if (importanceList.size() < 6) {
				Object[][] out = new Object[importanceList.size()][4];
				for (int i = 0; i < importanceList.size(); ++i) {
					out[i][0] = importanceList.get(i).f0;
					out[i][1] = df.format(importanceList.get(i).f1);
					out[i][2] = weightList.get(i).f0;
					out[i][3] = df.format(weightList.get(i).f1);
				}
				sbd.append(PrettyDisplayUtils.displayTable(out, importanceList.size(), 4, null,
					new String[] {"colName", "importanceValue", "colName", "weightValue"}, null,
					importanceList.size(), 4));
			} else {
				Object[][] out = new Object[7][4];
				for (int i = 0; i < 3; ++i) {
					out[i][0] = importanceList.get(i).f0;
					out[i][1] = df.format(importanceList.get(i).f1);
					out[i][2] = weightList.get(i).f0;
					out[i][3] = df.format(weightList.get(i).f1);
				}
				for (int i = 0; i < 4; ++i) {
					out[3][i] = "... ...";
				}
				for (int i = 3; i > 0; --i) {
					int idx = importanceList.size() - i;
					out[7 - i][0] = importanceList.get(idx).f0;
					out[7 - i][1] = df.format(importanceList.get(idx).f1);
					out[7 - i][2] = weightList.get(idx).f0;
					out[7 - i][3] = df.format(weightList.get(idx).f1);
				}
				sbd.append(PrettyDisplayUtils.displayTable(out, 7, 4, null,
					new String[] {"colName", "importanceValue", "colName", "weightValue"}, null, 7, 4));
			}
		}
		sbd.append(PrettyDisplayUtils.displayHeadline("train convergence info", '-'));
		if (convInfo.length < 6) {
			for (int i = 0; i < convInfo.length; ++i) {
				sbd.append("" + convInfo[i] + "\n");
			}
		} else {
			for (int i = 0; i < 3; ++i) {
				sbd.append("" + convInfo[i] + "\n");
			}
			sbd.append("" + "... ... ... ..." + "\n");
			for (int i = convInfo.length - 3; i < convInfo.length; ++i) {
				sbd.append("" + convInfo[i] + "\n");
			}
		}

		return sbd.toString();
	}
}
