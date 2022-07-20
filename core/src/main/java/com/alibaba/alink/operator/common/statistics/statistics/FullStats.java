package com.alibaba.alink.operator.common.statistics.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.AlinkTypes;
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
import java.util.Map;
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
				if (AlinkTypes.DOUBLE == colTypes[i] || AlinkTypes.FLOAT == colTypes[i]
					|| AlinkTypes.LONG == colTypes[i] || AlinkTypes.INT == colTypes[i]) {

					SummaryResultCol src = srt.col(colName);

					//get histogram
					IntervalCalculator ic = src.getIntervalCalculator();
					long[] counts = ic.getCount();

					Histogram.Builder histoBuilder = Histogram.newBuilder()
						.setType(HistogramType.STANDARD);

					for (int j = 0; j < counts.length; j++) {
						histoBuilder.addBuckets(Histogram.Bucket.newBuilder()
							.setLowValue(ic.getTag(j).doubleValue())
							.setHighValue(ic.getTag(j + 1).doubleValue())
							.setSampleCount(counts[j])
						);
					}

					Histogram.Builder percentileBuilder = Histogram.newBuilder()
						.setType(HistogramType.QUANTILES);

					for (int j = 0; j < 10; j++) {
						percentileBuilder.addBuckets(Histogram.Bucket.newBuilder()
							.setLowValue(((Number) src.getApproximatePercentile().getPercentile(10 * j)).doubleValue())
							.setHighValue(((Number) src.getApproximatePercentile().getPercentile(
								10 * (j + 1))).doubleValue())
							.setSampleCount(src.count / 10.0)
						);
					}

					Type feaType =
						(AlinkTypes.DOUBLE == colTypes[i] || AlinkTypes.FLOAT == colTypes[i]) ? Type.FLOAT : Type.INT;

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
									)
									.setMax(src.maxDouble())
									.setMin(src.minDouble())
									.setStdDev(src.standardDeviation())
									.setMedian(
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
						);
					if (src.hasFreq()) {
						TreeMap <Object, Long> freq = src.getFrequencyMap();
						int k = 0;
						for (Map.Entry <Object, Long> entry : freq.entrySet()) {
							stringBuilder.addTopValues(k++,
								FreqAndValue.newBuilder()
									.setValue(entry.getKey().toString())
									.setFrequency(entry.getValue())
							);
						}

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
				}
			}
		}
		return new FullStats(datasetFeatureStatisticsListBuilder.build());
	}

	public DatasetFeatureStatisticsList getDatasetFeatureStatisticsList() {
		return datasetFeatureStatisticsList;
	}
}
