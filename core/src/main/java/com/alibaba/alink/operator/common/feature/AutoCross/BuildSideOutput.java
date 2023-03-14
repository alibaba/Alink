package com.alibaba.alink.operator.common.feature.AutoCross;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelData;
import com.alibaba.alink.operator.common.feature.OneHotModelDataConverter;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class BuildSideOutput extends RichMapPartitionFunction <Row, Row> {
	int numericalSize;

	public BuildSideOutput(int numericalSize) {
		this.numericalSize = numericalSize;
	}

	public static void buildModel(List <Row> oneHotModelRow, List <Row> autoCrossModelRow,
								  Collector <Row> out) {
		final String LOW_FREQUENCY_VALUE = "lowFrequencyValue";
		final String NULL_VALUE = "null";

		String jsonStr = autoCrossModelRow.stream().filter(row -> row.getField(0).equals(0L))
			.map(row -> (String) row.getField(1))
			.collect(Collectors.toList()).get(0);
		AutoCrossAlgoModelMapper.FeatureSet fs = JsonConverter.fromJson(jsonStr, AutoCrossAlgoModelMapper.FeatureSet.class);
		List <int[]> crossFeatureSet = fs.crossFeatureSet;
		boolean hasDiscrete = fs.hasDiscrete;
		int numericalSize = fs.numericalCols.length;

		MultiStringIndexerModelData data = new OneHotModelDataConverter().load(oneHotModelRow).modelData;
		String[] featureCols = data.meta.get(HasSelectedCols.SELECTED_COLS);
		int featureNumber = data.tokenNumber.size();
		int[] featureSize = new int[featureNumber];
		int[] cunsum = new int[featureNumber + 1];
		for (int i = 0; i < featureNumber; i++) {
			featureSize[i] = (int) (data.tokenNumber.get(i) + (hasDiscrete ? 2 : 1));
			cunsum[i + 1] = cunsum[i] + featureSize[i];
		}

		//前面是正常的，后面是低频的
		Map <Integer, Tuple2 <String[], String[]>> featureValueMap = new HashMap <>();
		Set <Integer> crossSingleFeature = new HashSet <>();
		for (int[] ints : crossFeatureSet) {
			for (int i : ints) {
				crossSingleFeature.add(i);
			}
		}

		//HashMap存tuple2，另一个存低频的。
		//构造的时候，将低频的映射到相应的index中。
		//feature index, feature value, feature value index.
		if (hasDiscrete) {
			for (Tuple3 <Integer, String, Long> tokens : data.tokenAndIndex) {
				int featureIndex = tokens.f2.intValue();
				Tuple2 <String[], String[]> featureValues;
				if (crossSingleFeature.contains(tokens.f0)) {
					if (!featureValueMap.containsKey(tokens.f0)) {
						featureValues = Tuple2.of(new String[featureSize[tokens.f0]], new String[0]);
					} else {
						featureValues = featureValueMap.get(tokens.f0);
					}
					featureValues.f0[featureIndex] = tokens.f1;
					featureValueMap.put(tokens.f0, featureValues);
				}
				out.collect(Row.of(cunsum[tokens.f0] + featureIndex + numericalSize, featureCols[tokens.f0], tokens.f1));
			}
			//写null和低频的。
			for (int key = 0; key < featureSize.length; key++) {
				out.collect(Row.of(cunsum[key + 1] - 2 + numericalSize, featureCols[key], NULL_VALUE));
				Row rareData = Row.of(cunsum[key + 1] - 1 + numericalSize, featureCols[key], LOW_FREQUENCY_VALUE);
				out.collect(rareData);
				if (featureValueMap.containsKey(key)) {
					Tuple2 <String[], String[]> featureValues = featureValueMap.get(key);
					featureValues.f1 = new String[] {LOW_FREQUENCY_VALUE};
					//save the rare feature values.
					//                    for (String s : featureValues.f1) {
					//                        Row rareData = Row.of(cunsum[key + 1] - 1, featureCols[key], s);
					//                        out.collect(rareData);
					//                    }
					featureValueMap.put(key, featureValues);
				}
			}

			//cross feature
			//先cross高频的，再加上低频的。
			int startIndex = cunsum[featureNumber] + numericalSize;
			for (int[] crossFeature : crossFeatureSet) {
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < crossFeature.length; i++) {
					if (i == 0) {
						sb.append(featureCols[crossFeature[i]]);
					} else {
						sb.append(" and " + featureCols[crossFeature[i]]);
					}
				}
				String crossedFeatureName = sb.toString();

				int totalSize = 1;
				for (int i : crossFeature) {
					totalSize *= (featureValueMap.get(i).f0.length);
				}
				//count是进制。
				int[] count = new int[crossFeature.length + 1];
				count[0] = -1;
				for (int i = 0; i < totalSize; i++) {
					//控制进位
					int start = 0;
					count[start]++;
					while (true) {
						if (count[start] == featureValueMap.get(crossFeature[start]).f0.length) {
							count[start++] = 0;
							count[start] += 1;
						} else {
							break;
						}
					}

					String[][] crossValues = new String[crossFeature.length][];
					boolean toCalc = true;
					for (int j = 0; j < crossFeature.length; j++) {
						int crossFeatureIndex = crossFeature[j];
						if (count[j] == featureSize[crossFeatureIndex] - 1) {
							crossValues[j] = featureValueMap.get(crossFeature[j]).f1;
							if (crossValues[j].length == 0) {
								toCalc = false;
								break;
							}
						} else if (count[j] == featureSize[crossFeatureIndex] - 2) {
							crossValues[j] = new String[] {NULL_VALUE};
						} else {
							crossValues[j] = new String[] {featureValueMap.get(crossFeature[j]).f0[count[j]]};
						}
					}
					if (toCalc) {
						startIndex = concatValue(out, startIndex, crossedFeatureName, crossValues);
					} else {
						++startIndex;
					}
				}
			}
		} else {
			for (Tuple3 <Integer, String, Long> tokens : data.tokenAndIndex) {
				int featureIndex = tokens.f2.intValue();
				Tuple2 <String[], String[]> featureValues;
				if (crossSingleFeature.contains(tokens.f0)) {
					if (!featureValueMap.containsKey(tokens.f0)) {
						featureValues = Tuple2.of(new String[featureSize[tokens.f0]], new String[0]);
					} else {
						featureValues = featureValueMap.get(tokens.f0);
					}
					featureValues.f0[featureIndex] = tokens.f1;
					featureValueMap.put(tokens.f0, featureValues);
				}
				out.collect(Row.of(cunsum[tokens.f0] + featureIndex + numericalSize, featureCols[tokens.f0], tokens.f1));
			}
			for (int key = 0; key < featureSize.length; key++) {
				out.collect(Row.of(cunsum[key + 1] - 1 + numericalSize, featureCols[key], NULL_VALUE));
			}

			int startIndex = cunsum[featureNumber] + numericalSize;
			for (int[] crossFeature : crossFeatureSet) {
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < crossFeature.length; i++) {
					if (i == 0) {
						sb.append(featureCols[crossFeature[i]]);
					} else {
						sb.append(" and " + featureCols[crossFeature[i]]);
					}
				}
				String crossedFeatureName = sb.toString();

				int[] count = new int[crossFeature.length + 1];
				count[0] = -1;
				int totalSize = 1;
				for (int i : crossFeature) {
					totalSize *= featureValueMap.get(i).f0.length;
				}

				for (int i = 0; i < totalSize; i++) {
					int start = 0;
					count[start]++;
					while (true) {
						if (count[start] == featureValueMap.get(crossFeature[start]).f0.length) {
							count[start++] = 0;
							count[start] += 1;
						} else {
							break;
						}
					}
					sb = new StringBuilder();
					for (int j = 0; j < crossFeature.length; j++) {
						String value = "null";

						if (featureValueMap.get(crossFeature[j]).f0[count[j]] != null) {
							value = featureValueMap.get(crossFeature[j]).f0[count[j]];
						}

						if (j == 0) {
							sb.append(value);
						} else {
							sb.append(", " + value);
						}
					}
					Row res = Row.of(startIndex++, crossedFeatureName, sb.toString());
					out.collect(res);
				}
			}
		}
	}

	@Override
	public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
		List <Row> oneHotModelRow = Lists.newArrayList(values);

		List <Row> autoCrossModelRow = getRuntimeContext().getBroadcastVariable("autocrossModel");

		buildModel(oneHotModelRow, autoCrossModelRow, out);

	}

	private static int concatValue(Collector <Row> out, int startIndex,
								   String crossedFeatureName, String[][] values) {
		int valueSize = values.length;
		int[] maxSize = new int[valueSize];
		int[] countSize = new int[valueSize];
		countSize[0] = -1;
		int allNumber = 1;
		for (int i = 0; i < valueSize; i++) {
			maxSize[i] = values[i].length;
			allNumber *= maxSize[i];
		}
		for (int i = 0; i < allNumber; i++) {
			int start = 0;
			countSize[start]++;
			while (true) {
				if (countSize[start] == maxSize[start]) {
					countSize[start++] = 0;
					countSize[start] += 1;
				} else {
					break;
				}
			}
			String res = concat(values, countSize);
			out.collect(Row.of(startIndex, crossedFeatureName, res));
		}
		return ++startIndex;
	}

	private static String concat(String[][] values, int[] countSize) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < values.length; i++) {
			if (i == 0) {
				sb.append(values[i][countSize[i]]);
			} else {
				sb.append(", " + values[i][countSize[i]]);
			}
		}
		return sb.toString();
	}
}