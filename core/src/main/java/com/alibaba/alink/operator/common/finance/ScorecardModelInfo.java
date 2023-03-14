package com.alibaba.alink.operator.common.finance;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.lazy.BasePMMLModelInfo;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.finance.ScorecardTrainBatchOp;
import com.alibaba.alink.operator.common.feature.BinningModelDataConverter;
import com.alibaba.alink.operator.common.feature.binning.Bins;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculator;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsUtil;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import com.alibaba.alink.params.finance.BinningPredictParams;
import com.alibaba.alink.params.finance.ScorecardTrainParams;
import com.alibaba.alink.pipeline.ModelExporterUtils;
import com.alibaba.alink.pipeline.PipelineStageBase;
import com.alibaba.alink.pipeline.feature.BinningModel;
import com.alibaba.alink.pipeline.finance.ScoreModel;
import org.dmg.pmml.Application;
import org.dmg.pmml.CompoundPredicate;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.Header;
import org.dmg.pmml.InvalidValueTreatmentMethod;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunction;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.OpType;
import org.dmg.pmml.Output;
import org.dmg.pmml.OutputField;
import org.dmg.pmml.PMML;
import org.dmg.pmml.SimplePredicate;
import org.dmg.pmml.Timestamp;
import org.dmg.pmml.True;
import org.dmg.pmml.regression.NumericPredictor;
import org.dmg.pmml.regression.RegressionModel;
import org.dmg.pmml.regression.RegressionTable;
import org.dmg.pmml.scorecard.Attribute;
import org.dmg.pmml.scorecard.Characteristic;
import org.dmg.pmml.scorecard.Characteristics;
import org.dmg.pmml.scorecard.Scorecard;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ScorecardModelInfo implements BasePMMLModelInfo, Serializable {
	private Map <String, List <Tuple2 <String, Double>>> categoricalScores;
	private Map <String, List <Tuple2 <String, Double>>> numericScores;
	private Map <String, Number[]> splitArrays;
	private boolean isLeftOpen;
	private Double intercept;
	private String modelName;

	private LinearRegressorModelInfo linearModelInfo;

	public ScorecardModelInfo(List <Row> modelRows, TableSchema modelSchema) {
		List <Row> binningModelRows = null;
		List <Row> linearModelRows = null;

		List <Tuple3 <PipelineStageBase <?>, TableSchema, List <Row>>> deserialized =
			ModelExporterUtils.loadStagesFromPipelineModel(modelRows, modelSchema);

		Params scoreCardParams = deserialized.get(0).f0.getParams();

		for (Tuple3 <PipelineStageBase <?>, TableSchema, List <Row>> d : deserialized) {
			if (d.f0 instanceof BinningModel) {
				binningModelRows = d.f2;
			}
			if (d.f0 instanceof ScoreModel) {
				linearModelRows = d.f2;
			}
		}

		if (null == binningModelRows) {
			linearModelInfo = new LinearRegressorModelInfo(linearModelRows);
		} else {
			categoricalScores = new HashMap <>();
			numericScores = new HashMap <>();
			splitArrays = new HashMap <>();
			Preconditions.checkNotNull(linearModelRows);
			LinearModelData linearModelData = new LinearModelDataConverter().load(linearModelRows);
			modelName = linearModelData.modelName;
			intercept = linearModelData.coefVector.get(0);

			List <FeatureBinsCalculator> featureBinsCalculatorList
				= new BinningModelDataConverter().load(binningModelRows);
			ScorecardTrainBatchOp.ScorecardTransformData transformData
				= new ScorecardTrainBatchOp.ScorecardTransformData(
				true,
				scoreCardParams.get(ScorecardTrainParams.SELECTED_COLS),
				BinningPredictParams.Encode.WOE.equals(scoreCardParams.get(BinningPredictParams.ENCODE)),
				scoreCardParams.get(ScorecardTrainParams.DEFAULT_WOE),
				true);

			transformData.scaledModel = ScorecardTrainBatchOp.FeatureBinsToScorecard.getModelData(linearModelData,
				transformData);
			transformData.unscaledModel = transformData.scaledModel;
			if (!transformData.isWoe) {
				Map <String, Integer> nameBinCountMap = new HashMap <>();
				featureBinsCalculatorList.forEach(featureBinsCalculator ->
					nameBinCountMap.put(featureBinsCalculator.getFeatureName(),
						FeatureBinsUtil.getBinEncodeVectorSize(featureBinsCalculator)));
				ScorecardTrainBatchOp.FeatureBinsToScorecard.initializeStartIndex(transformData, nameBinCountMap);
			}
			if (null != linearModelData.featureNames) {
				transformData.stepwiseSelectedCols = ScorecardTrainBatchOp.trimCols(linearModelData.featureNames,
					ScorecardTrainBatchOp.BINNING_OUTPUT_COL);
			} else {
				transformData.stepwiseSelectedCols = transformData.selectedCols;
			}

			for (FeatureBinsCalculator featureBinsCalculator : featureBinsCalculatorList) {
				transformData.featureIndex = TableUtil.findColIndex(transformData.stepwiseSelectedCols,
					featureBinsCalculator.getFeatureName());
				if (transformData.featureIndex < 0) {
					continue;
				}
				featureBinsCalculator.splitsArrayToInterval();
				List <Tuple2 <String, Double>> map = new ArrayList <>();
				if (null != featureBinsCalculator.bin.normBins) {
					for (Bins.BaseBin bin : featureBinsCalculator.bin.normBins) {
						map.add(Tuple2.of(bin.getValueStr(featureBinsCalculator.getColType()),
							getBinScore(transformData, bin, featureBinsCalculator)));
					}
				}
				if (null != featureBinsCalculator.bin.nullBin) {
					map.add(Tuple2.of(FeatureBinsUtil.NULL_LABEL, getBinScore(transformData,
						featureBinsCalculator.bin.nullBin, featureBinsCalculator)));
				}
				if (null != featureBinsCalculator.bin.elseBin) {
					map.add(Tuple2.of(FeatureBinsUtil.ELSE_LABEL, getBinScore(transformData,
						featureBinsCalculator.bin.elseBin, featureBinsCalculator)));
				}
				if (featureBinsCalculator.isNumeric()) {
					numericScores.put(featureBinsCalculator.getFeatureName(), map);
					splitArrays.put(featureBinsCalculator.getFeatureName(), featureBinsCalculator.getSplitsArray());
					isLeftOpen = featureBinsCalculator.getLeftOpen();
				} else {
					categoricalScores.put(featureBinsCalculator.getFeatureName(), map);
				}
			}
		}
	}

	private static double getBinScore(ScorecardTrainBatchOp.ScorecardTransformData transformData,
									  Bins.BaseBin bin,
									  FeatureBinsCalculator featureBinsCalculator) {
		int binIndex = bin.getIndex().intValue();
		String featureColName = featureBinsCalculator.getFeatureName();

		int linearModelCoefIdx = ScorecardTrainBatchOp.FeatureBinsToScorecard.findLinearModelCoefIdx(featureColName,
			binIndex, transformData);
		Preconditions.checkArgument(linearModelCoefIdx >= 0);
		return transformData.isScaled ? FeatureBinsUtil.keepGivenDecimal(
			ScorecardTrainBatchOp.FeatureBinsToScorecard
				.getModelValue(linearModelCoefIdx, bin.getWoe(), transformData.scaledModel, transformData.isWoe,
					transformData.defaultWoe), 3) : null;
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder(PrettyDisplayUtils.displayHeadline("ScorecardModelInfo", '-'));
		if (null != linearModelInfo) {
			sbd.append(linearModelInfo.toString());
		} else {
			int size = categoricalScores.values().stream().mapToInt(m -> m.size()).sum() +
				numericScores.values().stream().mapToInt(m -> m.size()).sum() + 1;
			String[][] table = new String[size][3];
			int cnt = 0;
			for (Map.Entry <String, List <Tuple2 <String, Double>>> entry : categoricalScores.entrySet()) {
				table[cnt][0] = entry.getKey();
				for (Tuple2 <String, Double> entry1 : entry.getValue()) {
					if (table[cnt][0] == null) {
						table[cnt][0] = "";
					}
					table[cnt][1] = entry1.f0;
					table[cnt++][2] = entry1.f1.toString();
				}
			}
			for (Map.Entry <String, List <Tuple2 <String, Double>>> entry : numericScores.entrySet()) {
				table[cnt][0] = entry.getKey();
				for (Tuple2 <String, Double> entry1 : entry.getValue()) {
					if (table[cnt][0] == null) {
						table[cnt][0] = "";
					}
					table[cnt][1] = entry1.f0;
					table[cnt++][2] = entry1.f1.toString();
				}
			}
			table[cnt][0] = "intercept";
			table[cnt][1] = "";
			table[cnt][2] = intercept.toString();
			sbd.append(PrettyDisplayUtils.displayTable(table, table.length, 3, null,
				new String[] {"featureName", "FeatureBin", "BinScore"}, null, 20, 3));
		}
		return sbd.toString();
	}

	@Override
	public PMML toPMML() {
		String version = this.getClass().getPackage().getImplementationVersion();
		Application app = new Application().setVersion(version);
		Timestamp timestamp = new Timestamp()
			.addContent(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US).format(new Date()));
		Header header = new Header()
			.setApplication(app)
			.setTimestamp(timestamp);
		PMML pmml = new PMML("4.2", header, null);
		String targetName = "prediction_score";
		FieldName outputField = FieldName.create(targetName);
		Output output = new Output().addOutputFields(new OutputField(outputField, OpType.CONTINUOUS, DataType.DOUBLE)
			.setTargetField(outputField));

		if (null != linearModelInfo) {
			String description = "Linear Model";
			String modelName = linearModelInfo.getModelName();
			pmml.getHeader().setDescription(description);
			double[] weights = linearModelInfo.getWeight().getData();
			String[] featureNames = linearModelInfo.getFeatureNames();
			FieldName[] fields = new FieldName[weights.length - 1];
			DataDictionary dataDictionary = new DataDictionary();
			MiningSchema miningSchema = new MiningSchema();

			RegressionTable regressionTable = new RegressionTable(weights[0]);
			RegressionModel regressionModel = new RegressionModel()
				.setMiningFunction(MiningFunction.REGRESSION)
				.setMiningSchema(miningSchema)
				.setModelName(modelName)
				//.setOutput(output)
				.addRegressionTables(regressionTable);

			for (int i = 0; i < weights.length - 1; i++) {
				fields[i] = FieldName.create(featureNames[i]);
				dataDictionary.addDataFields(new DataField(fields[i], OpType.CONTINUOUS, DataType.DOUBLE));
				miningSchema.addMiningFields(new MiningField(fields[i])
					.setUsageType(MiningField.UsageType.ACTIVE)
					.setInvalidValueTreatment(InvalidValueTreatmentMethod.AS_MISSING));
				regressionTable.addNumericPredictors(new NumericPredictor(fields[i], weights[i + 1]).setExponent(1));
			}

			// for completeness add target field
			dataDictionary.addDataFields(new DataField(outputField, OpType.CONTINUOUS, DataType.DOUBLE));
			miningSchema.addMiningFields(new MiningField(outputField).setUsageType(MiningField.UsageType.TARGET));

			dataDictionary.setNumberOfFields(dataDictionary.getDataFields().size());

			pmml.setDataDictionary(dataDictionary);
			pmml.addModels(regressionModel);
		} else {
			String description = "Scorecard Model";
			pmml.getHeader().setDescription(description);
			DataDictionary dataDictionary = new DataDictionary();
			MiningSchema miningSchema = new MiningSchema();
			Characteristics characteristics = new Characteristics();

			Scorecard scorecard = new Scorecard()
				.setMiningFunction(MiningFunction.REGRESSION)
				.setMiningSchema(miningSchema)
				.setModelName("Scorecard")
				.setAlgorithmName(modelName)
				//.setOutput(output)
				.setUseReasonCodes(false)
				.setInitialScore(intercept)
				.setCharacteristics(characteristics);

			for (Map.Entry <String, List <Tuple2 <String, Double>>> entry : categoricalScores.entrySet()) {
				FieldName fieldName = FieldName.create(entry.getKey());
				dataDictionary.addDataFields(new DataField(fieldName, OpType.CATEGORICAL, DataType.STRING));
				miningSchema.addMiningFields(new MiningField(fieldName)
					.setUsageType(MiningField.UsageType.ACTIVE)
					.setInvalidValueTreatment(InvalidValueTreatmentMethod.AS_MISSING));
				Characteristic characteristic = new Characteristic().setName(entry.getKey() + "_score");
				for (Tuple2 <String, Double> t : entry.getValue()) {
					Attribute attribute = new Attribute().setPartialScore(t.f1);
					if (t.f0.equals(FeatureBinsUtil.NULL_LABEL)) {
						attribute.setPredicate(new SimplePredicate()
							.setField(fieldName)
							.setOperator(SimplePredicate.Operator.IS_MISSING));
					} else if (t.f0.equals(FeatureBinsUtil.ELSE_LABEL)) {
						attribute.setPredicate(new True());
					} else {
						String[] values = t.f0.split(Bins.JOIN_DELIMITER);
						if (values.length == 1) {
							attribute.setPredicate(new SimplePredicate()
								.setField(fieldName)
								.setOperator(SimplePredicate.Operator.EQUAL)
								.setValue(values[0]));
						} else {
							CompoundPredicate predicate = new CompoundPredicate().setBooleanOperator(
								CompoundPredicate.BooleanOperator.OR);
							attribute.setPredicate(predicate);
							for (String s : values) {
								predicate.addPredicates(new SimplePredicate()
									.setField(fieldName)
									.setOperator(SimplePredicate.Operator.EQUAL)
									.setValue(s));
							}
						}
					}
					characteristic.addAttributes(attribute);
				}
				characteristics.addCharacteristics(characteristic);
			}
			for (Map.Entry <String, List <Tuple2 <String, Double>>> entry : numericScores.entrySet()) {
				FieldName fieldName = FieldName.create(entry.getKey());
				dataDictionary.addDataFields(new DataField(fieldName, OpType.CONTINUOUS, DataType.DOUBLE));
				miningSchema.addMiningFields(new MiningField(fieldName)
					.setUsageType(MiningField.UsageType.ACTIVE)
					.setInvalidValueTreatment(InvalidValueTreatmentMethod.AS_MISSING));
				Characteristic characteristic = new Characteristic().setName(entry.getKey() + "_score");
				Number[] list = splitArrays.get(entry.getKey());
				SimplePredicate.Operator first = isLeftOpen ? SimplePredicate.Operator.GREATER_THAN : SimplePredicate
					.Operator.GREATER_OR_EQUAL;
				SimplePredicate.Operator second = isLeftOpen ? SimplePredicate.Operator.LESS_OR_EQUAL :
					SimplePredicate.Operator.LESS_THAN;

				for (int i = 0; i < entry.getValue().size(); i++) {
					Tuple2 <String, Double> t = entry.getValue().get(i);
					Attribute attribute = new Attribute().setPartialScore(t.f1);
					if (t.f0.equals(FeatureBinsUtil.NULL_LABEL)) {
						attribute.setPredicate(new SimplePredicate()
							.setField(fieldName)
							.setOperator(SimplePredicate.Operator.IS_MISSING));
					} else {
						if (i == 0) {
							attribute.setPredicate(new SimplePredicate()
								.setField(fieldName)
								.setOperator(second)
								.setValue(list[i].toString()));
						} else if (i == list.length) {
							attribute.setPredicate(new SimplePredicate()
								.setField(fieldName)
								.setOperator(first)
								.setValue(list[i - 1].toString()));
						} else {
							CompoundPredicate predicate = new CompoundPredicate().setBooleanOperator(
								CompoundPredicate.BooleanOperator.AND);
							attribute.setPredicate(predicate);
							predicate.addPredicates(new SimplePredicate()
								.setField(fieldName)
								.setOperator(first)
								.setValue(list[i - 1].toString()))
								.addPredicates(new SimplePredicate()
									.setField(fieldName)
									.setOperator(second)
									.setValue(list[i].toString()));
						}
					}
					characteristic.addAttributes(attribute);
				}
				characteristics.addCharacteristics(characteristic);
			}

			// for completeness add target field
			dataDictionary.addDataFields(new DataField(outputField, OpType.CONTINUOUS, DataType.DOUBLE));
			miningSchema.addMiningFields(new MiningField(outputField).setUsageType(MiningField.UsageType.TARGET));

			dataDictionary.setNumberOfFields(dataDictionary.getDataFields().size());

			pmml.setDataDictionary(dataDictionary);
			pmml.addModels(scorecard);
		}
		return pmml;
	}
}
