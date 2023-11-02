package com.alibaba.alink.operator.common.statistics.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.metadata.def.v0.BytesStatistics;
import com.alibaba.alink.metadata.def.v0.CommonStatistics;
import com.alibaba.alink.metadata.def.v0.DatasetFeatureStatistics;
import com.alibaba.alink.metadata.def.v0.DatasetFeatureStatisticsList;
import com.alibaba.alink.metadata.def.v0.FeatureNameStatistics;
import com.alibaba.alink.metadata.def.v0.FeatureNameStatistics.Type;
import com.alibaba.alink.metadata.def.v0.Histogram;
import com.alibaba.alink.metadata.def.v0.Histogram.HistogramType;
import com.alibaba.alink.metadata.def.v0.NumericStatistics;
import com.alibaba.alink.metadata.def.v0.RankHistogram.Bucket;
import com.alibaba.alink.metadata.def.v0.StringStatistics;
import com.alibaba.alink.metadata.def.v0.StringStatistics.FreqAndValue;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * {@link SummaryResultTable} has many abstract types and Object types in use, lots of adaptations on existing codes are
 * required to make ser/der work. Therefore, we do not use {@link SummaryResultTable} in {@link FullStatsConverter}.
 */
public class FullStats implements Serializable {
	private DatasetFeatureStatisticsList datasetFeatureStatisticsList;

	@SuppressWarnings("unused")
	private FullStats() {
	}

	public FullStats(DatasetFeatureStatisticsList datasetFeatureStatisticsList) {
		this.datasetFeatureStatisticsList = datasetFeatureStatisticsList;
	}

	public static FullStats fromSummaryResultTable(String[] tableNames, String[] colTypeStrs,
												   Iterable <Tuple2 <Integer, SummaryResultTable>> indexedSrts) {
		int n = tableNames.length;
		final TypeInformation <?>[] colTypes = FlinkTypeConverter.getFlinkType(colTypeStrs);
		DatasetFeatureStatisticsList.Builder datasetFeatureStatisticsListBuilder =
			DatasetFeatureStatisticsList.newBuilder();
		for (int i = 0; i < n; i += 1) {
			datasetFeatureStatisticsListBuilder.addDatasetsBuilder();
		}

		for (Tuple2 <Integer, SummaryResultTable> indexedSrt : indexedSrts) {
			int index = indexedSrt.f0;
			SummaryResultTable srt = indexedSrt.f1;
			DatasetFeatureStatistics.Builder builder = datasetFeatureStatisticsListBuilder
				.getDatasetsBuilder(index)
				.setName(tableNames[index])
				.setNumExamples(srt.col(0).count);
			String[] colNames = srt.colNames;
			for (int i = 0; i < colNames.length; i++) {
				String colName = colNames[i];
				if (Number.class.isAssignableFrom(colTypes[i].getTypeClass())) {
					//if (AlinkTypes.DOUBLE == colTypes[i] || AlinkTypes.FLOAT == colTypes[i]
					//	|| AlinkTypes.LONG == colTypes[i] || AlinkTypes.INT == colTypes[i]) {

					SummaryResultCol src = srt.col(colName);

					Histogram.Builder histoBuilder = Histogram.newBuilder().setType(HistogramType.STANDARD);
					Histogram.Builder percentileBuilder = Histogram.newBuilder().setType(HistogramType.QUANTILES);

					//get histogram
					IntervalCalculator ic = src.getIntervalCalculator();
					if (null != ic) {
						long[] counts = ic.getCount();

						for (int j = 0; j < counts.length; j++) {
							histoBuilder.addBuckets(Histogram.Bucket.newBuilder()
								.setLowValue(ic.getTag(j).doubleValue())
								.setHighValue(ic.getTag(j + 1).doubleValue())
								.setSampleCount(counts[j])
							);
						}

						for (int j = 0; j < 10; j++) {
							percentileBuilder.addBuckets(Histogram.Bucket.newBuilder()
								.setLowValue(((Number) src.getApproximatePercentile().getPercentile(
									10 * j)).doubleValue())
								.setHighValue(((Number) src.getApproximatePercentile().getPercentile(
									10 * (j + 1))).doubleValue())
								.setSampleCount(src.count / 10.0)
							);
						}
					}

					Type feaType =
						(AlinkTypes.DOUBLE == colTypes[i] || AlinkTypes.FLOAT == colTypes[i]
							|| AlinkTypes.BIG_DEC == colTypes[i]) ? Type.FLOAT : Type.INT;

					builder.addFeatures(
						FeatureNameStatistics.newBuilder()
							.setName(colName)
							.setType(feaType)
							.setNumStats(
								NumericStatistics.newBuilder()
									.setCommonStats(
										CommonStatistics.newBuilder()
											.setNumMissing(src.countMissValue)
											.setTotNumValues(src.countTotal)
											.setNumNonMissing(src.count)
											.setAvgNumValues(1)
											.setMinNumValues(1)
											.setMaxNumValues(1)
									)
									.setNumZeros(src.countZero)
									.setMax(src.maxDouble())
									.setMin(src.minDouble())
									.setMean(src.mean())
									.setStdDev(src.standardDeviation())
									.setMedian((0 == src.count) ? Double.NaN :
										src.hasFreq()
											? ((Number) src.getPercentile().median).doubleValue()
											: src.getApproximatePercentile().getPercentile(50)
									)
									.addHistograms(histoBuilder)
									.addHistograms(percentileBuilder)
							)
					);
				} else if (AlinkTypes.STRING == colTypes[i]) {
					SummaryResultCol src = srt.col(colName);

					StringStatistics.Builder stringBuilder = StringStatistics.newBuilder()
						.setCommonStats(
							CommonStatistics.newBuilder()
								.setNumMissing(src.countMissValue)
								.setTotNumValues(src.countTotal)
								.setNumNonMissing(src.count)
						)
						.setAvgLength((float) src.mean());

					if (src.count > 0 && src.hasFreq()) {
						TreeMap <Object, Long> freq = src.getFrequencyMap();
						stringBuilder.setUnique(freq.size());

						ArrayList <Map.Entry <Object, Long>> list = new ArrayList <>(freq.entrySet());
						Collections.sort(list, new Comparator <Entry <Object, Long>>() {
							@Override
							public int compare(Entry <Object, Long> o1, Entry <Object, Long> o2) {
								return o2.getValue().compareTo(o1.getValue());
							}
						});
						stringBuilder.addTopValues(0,
							FreqAndValue.newBuilder()
								.setValue(list.get(0).getKey().toString())
								.setFrequency(list.get(0).getValue())
						);

						for (Map.Entry <Object, Long> entry : freq.entrySet()) {
							stringBuilder.getRankHistogramBuilder().addBuckets(
								Bucket.newBuilder().setLabel(entry.getKey().toString()).setSampleCount(entry.getValue())
							);
						}
					}

					builder.addFeatures(
						FeatureNameStatistics.newBuilder()
							.setName(colName)
							.setType(Type.STRING)
							.setStringStats(stringBuilder)
					);
				} else if (AlinkTypes.BOOLEAN == colTypes[i]) {
					SummaryResultCol src = srt.col(colName);

					Long countTrue = (long) src.sum();
					Long countFalse = src.count - (long) src.sum();

					Histogram.Builder histoBuilder = Histogram.newBuilder()
						.setType(HistogramType.STANDARD);

					histoBuilder.addBuckets(Histogram.Bucket.newBuilder()
						.setLowValue(0.0)
						.setHighValue(1.0)
						.setSampleCount(countFalse)
					);
					histoBuilder.addBuckets(Histogram.Bucket.newBuilder()
						.setLowValue(1.0)
						.setHighValue(2.0)
						.setSampleCount(countTrue)
					);

					Histogram.Builder percentileBuilder = Histogram.newBuilder()
						.setType(HistogramType.QUANTILES);

					int k = (int) (countFalse * 10 / src.count);
					for (int j = 0; j < 10; j++) {
						percentileBuilder.addBuckets(Histogram.Bucket.newBuilder()
							.setLowValue((j <= k) ? 0.0 : 1.0)
							.setHighValue((j < k) ? 0.0 : 1.0)
							.setSampleCount(src.count / 10.0)
						);
					}

					builder.addFeatures(
						FeatureNameStatistics.newBuilder()
							.setName(colName)
							.setType(Type.INT)
							.setNumStats(
								NumericStatistics.newBuilder()
									.setCommonStats(
										CommonStatistics.newBuilder()
											.setNumMissing(src.countMissValue)
											.setTotNumValues(src.countTotal)
											.setNumNonMissing(src.count)
											.setAvgNumValues(1)
											.setMinNumValues(1)
											.setMaxNumValues(1)
									)
									.setNumZeros(src.countZero)
									.setMax(src.maxDouble())
									.setMin(src.minDouble())
									.setMean(src.mean())
									.setStdDev(src.standardDeviation())
									.setMedian((countTrue >= countFalse) ? 1.0 : 0.0)
									.addHistograms(histoBuilder)
									.addHistograms(percentileBuilder)
							)
					);

					StringStatistics.Builder stringBuilder = StringStatistics.newBuilder()
						.setCommonStats(
							CommonStatistics.newBuilder()
								.setNumMissing(src.countMissValue)
								.setTotNumValues(src.countTotal)
								.setNumNonMissing(src.count)
						)
						.setUnique(((countTrue > 0) ? 1 : 0) + ((countFalse > 0) ? 1 : 0));

					stringBuilder.addTopValues(0,
						FreqAndValue.newBuilder()
							.setValue((countTrue >= countFalse) ? "true" : "false")
							.setFrequency((countTrue >= countFalse) ? countTrue : countFalse)
					);

					stringBuilder.getRankHistogramBuilder().addBuckets(
						Bucket.newBuilder().setLabel("true").setSampleCount(countTrue)
					);
					stringBuilder.getRankHistogramBuilder().addBuckets(
						Bucket.newBuilder().setLabel("false").setSampleCount(countFalse)
					);

					builder.addFeatures(
						FeatureNameStatistics.newBuilder()
							.setName(colName + "_categorical")
							.setType(Type.STRING)
							.setStringStats(stringBuilder)
					);

				} else if (AlinkTypes.VARBINARY == colTypes[i]) {
					SummaryResultCol src = srt.col(colName);

					BytesStatistics.Builder bytesBuilder = BytesStatistics.newBuilder()
						.setCommonStats(
							CommonStatistics.newBuilder()
								.setNumMissing(src.countMissValue)
								.setTotNumValues(src.countTotal)
								.setNumNonMissing(src.count)
						)
						.setAvgNumBytes((float) src.mean())
						.setMinNumBytes((float) src.minDouble())
						.setMaxNumBytes((float) src.maxDouble())
						.setMaxNumBytesInt((null == src.max) ? 0L : ((Number) src.max).longValue());

					if (src.count > 0 && src.hasFreq()) {
						TreeMap <Object, Long> freq = src.getFrequencyMap();
						bytesBuilder.setUnique(freq.size());
					}

					builder.addFeatures(
						FeatureNameStatistics.newBuilder()
							.setName(colName)
							.setType(Type.BYTES)
							.setBytesStats(bytesBuilder)
					);
				}
			}
		}
		return new FullStats(datasetFeatureStatisticsListBuilder.build());
	}

	public DatasetFeatureStatisticsList getDatasetFeatureStatisticsList() {
		return datasetFeatureStatisticsList;
	}
}
