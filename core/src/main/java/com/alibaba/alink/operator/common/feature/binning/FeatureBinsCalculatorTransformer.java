package com.alibaba.alink.operator.common.feature.binning;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.operator.common.feature.ContinuousRanges;

/**
 * The transform functions between FeatureBinsCalculator and FeatureBins, FeatureBinsCalculator and
 * ContinuousFeatureIntervals.
 */
public class FeatureBinsCalculatorTransformer {
	public static FeatureBins toFeatureBins(FeatureBinsCalculator builder) {
		if (builder.isNumeric()) {
			builder.checkSplitsArray();
			builder.calcStatistics();
			return FeatureBins.createNumericFeatureBins(
				builder.getBinDivideType(),
				builder.getFeatureName(),
				builder.getFeatureType(),
				builder.bin,
				builder.getIv(),
				builder.getBinCount(),
				builder.getSplitsArray(),
				builder.getLeftOpen()
			);
		} else {
			builder.checkDiscreteNormBins();
			builder.calcStatistics();
			return FeatureBins.createDisreteFeatureBins(
				builder.getBinDivideType(),
				builder.getFeatureName(),
				builder.getFeatureType(),
				builder.bin,
				builder.getIv(),
				builder.getBinCount());
		}
	}

	public static FeatureBinsCalculator fromFeatureBins(FeatureBins featureBins) {
		TypeInformation <?> type = FeatureBinsUtil.getFlinkType(featureBins.getFeatureType());
		BinTypes.ColType colType = BinTypes.ColType.valueOf(type);
		FeatureBinsCalculator builder;
		if (!colType.isNumeric) {
			builder = FeatureBinsCalculator.createDiscreteCalculator(
				featureBins.getBinDivideType(),
				featureBins.getFeatureName(),
				type,
				featureBins.getBin()
			);
			builder.checkDiscreteNormBins();
			builder.calcStatistics();
		} else {
			builder = FeatureBinsCalculator.createNumericCalculator(
				featureBins.getBinDivideType(),
				featureBins.getFeatureName(),
				type,
				featureBins.getSplitsArray(),
				featureBins.getLeftOpen()
			);
			//To merge the statictics info
			builder.tryToUpdateBins(featureBins.getBin());
			builder.checkSplitsArray();
			builder.calcStatistics();
			Preconditions.checkState(null == builder.bin.elseBin, "Numeric bin should not have else bin!");
		}
		return builder;
	}

	public static FeatureBinsCalculator fromContinuousFeatureInterval(ContinuousRanges featureInterval,
																	  BinDivideType binDivideType) {
		return FeatureBinsCalculator.createNumericCalculator(binDivideType,
			featureInterval.featureName,
			FeatureBinsUtil.getFlinkType(featureInterval.featureType),
			featureInterval.splitsArray,
			featureInterval.getLeftOpen());
	}

	public static ContinuousRanges toContinuousFeatureInterval(FeatureBinsCalculator featureBinsCalculator) {
		Preconditions.checkArgument(featureBinsCalculator.isNumeric(),
			"Discrete featureBorder can not be transformed to ContinuousFeatureInterval");
		return new ContinuousRanges(
			featureBinsCalculator.getFeatureName(),
			FeatureBinsUtil.getFlinkType(featureBinsCalculator.getFeatureType()),
			featureBinsCalculator.getSplitsArray(),
			featureBinsCalculator.getLeftOpen()
		);
	}
}
