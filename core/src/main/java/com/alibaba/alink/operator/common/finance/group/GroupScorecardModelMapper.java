package com.alibaba.alink.operator.common.finance.group;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.mapper.ComboModelMapper;
import com.alibaba.alink.common.mapper.PipelineModelMapper;
import com.alibaba.alink.operator.batch.finance.GroupScorecardTrainBatchOp;
import com.alibaba.alink.params.finance.BinningPredictParams;
import com.alibaba.alink.params.finance.BinningTrainParams;
import com.alibaba.alink.params.finance.GroupScorecardPredictParams;
import com.alibaba.alink.params.finance.GroupScorecardTrainParams;
import com.alibaba.alink.params.finance.ScorePredictParams;
import com.alibaba.alink.params.finance.ScorecardPredictParams;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.pipeline.ModelExporterUtils;
import com.alibaba.alink.pipeline.PipelineStageBase;
import com.alibaba.alink.pipeline.feature.BinningModel;
import com.alibaba.alink.pipeline.finance.SimpleGroupScoreModel;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GroupScorecardModelMapper extends ComboModelMapper {
	private static String SCORE_SUFFIX = "_SCORE";

	public GroupScorecardModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);

		Tuple2 <String[], TypeInformation <?>[]> tuple2 = PipelineModelMapper.getExtendModelSchema(modelSchema);
		String[] selectedCols = tuple2.f0;

		String[] scoreColNames = new String[selectedCols.length];
		for (int i = 0; i < scoreColNames.length; i++) {
			scoreColNames[i] = selectedCols[i] + SCORE_SUFFIX;
		}

		this.params.set(ScorePredictParams.PREDICTION_SCORE_PER_FEATURE_COLS, scoreColNames);
		this.params.set(HasFeatureCols.FEATURE_COLS, selectedCols);

		Preconditions.checkArgument(
			this.params.contains(ScorecardPredictParams.PREDICTION_SCORE_COL)
				&& this.params.contains(ScorecardPredictParams.PREDICTION_DETAIL_COL),
			"predictionScore and predictionDetail must be given!"
		);

		if (this.params.get(ScorePredictParams.RESERVED_COLS) == null) {
			this.params.set(ScorePredictParams.RESERVED_COLS, dataSchema.getFieldNames());
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		List <Tuple3 <PipelineStageBase <?>, TableSchema, List <Row>>> stages =
			ModelExporterUtils.loadStagesFromPipelineModel(modelRows, getModelSchema());

		String[] groupCols = stages.get(0).f0.getParams().get(BinningTrainParams.SELECTED_COLS);
		String[] groupBinPredictCols =
			GroupScorecardTrainBatchOp.getNewColNames(groupCols, GroupScoreCardVariable.GROUP_BINNING_OUTPUT_COL);

		for (int i = 0; i < stages.size(); i++) {
			PipelineStageBase <?> stage = stages.get(i).f0;

			stage.getParams().merge(this.params);

			if (stage instanceof BinningModel) {
				stage.getParams().set(ScorecardPredictParams.RESERVED_COLS, null);
				this.params.set(HasFeatureCols.FEATURE_COLS,
					stage.getParams().get(BinningPredictParams.OUTPUT_COLS));
			}

			if (stage instanceof SimpleGroupScoreModel) {
				stage.getParams().set(GroupScorecardTrainParams.GROUP_COLS, groupBinPredictCols);
			}

			if (stage instanceof SimpleGroupScoreModel) {
				if (i < stages.size() - 1) {
					stage.getParams().remove(GroupScorecardPredictParams.PREDICTION_SCORE_COL);
					stage.getParams().remove(GroupScorecardPredictParams.PREDICTION_SCORE_PER_FEATURE_COLS);
					stage.getParams().remove(GroupScorecardPredictParams.CALCULATE_SCORE_PER_FEATURE);
					stage.getParams().set(GroupScorecardPredictParams.RESERVED_COLS, null);
					stages.get(i + 1).f0.getParams().merge(this.params);
					stages.get(i + 1).f0.getParams().remove(ScorePredictParams.PREDICTION_DETAIL_COL);
					stages.get(i + 1).f0.getParams().set(
						GroupScorecardPredictParams.RESERVED_COLS,
						ArrayUtils.add(
							stages.get(i + 1).f0.getParams().get(GroupScorecardPredictParams.RESERVED_COLS),
							stage.getParams().get(GroupScorecardPredictParams.PREDICTION_DETAIL_COL)
						)
					);
					break;
				}
			}
		}

		this.mapperList = ModelExporterUtils
			.loadMapperListFromStages(stages, getDataSchema());
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		Tuple2 <String[], TypeInformation <?>[]> tuple2 = PipelineModelMapper.getExtendModelSchema(modelSchema);
		String[] selectedCols = tuple2.f0;
		TypeInformation labelType = tuple2.f1[0];

		String[] scoreColNames = new String[selectedCols.length];
		for (int i = 0; i < scoreColNames.length; i++) {
			scoreColNames[i] = selectedCols[i] + SCORE_SUFFIX;
		}

		params.set(ScorePredictParams.PREDICTION_SCORE_PER_FEATURE_COLS, scoreColNames);
		params.set(HasFeatureCols.FEATURE_COLS, selectedCols);

		if (params.get(ScorePredictParams.RESERVED_COLS) == null) {
			params.set(ScorePredictParams.RESERVED_COLS, dataSchema.getFieldNames());
		}

		boolean calculateScore = params.contains(ScorePredictParams.PREDICTION_SCORE_COL);
		boolean calculateDetail = params.contains(ScorePredictParams.PREDICTION_DETAIL_COL);
		boolean calculateScorePerFeature = params.get(ScorePredictParams.CALCULATE_SCORE_PER_FEATURE);
		List <String> helperColNames = new ArrayList <>();
		List <TypeInformation> helperColTypes = new ArrayList <>();
		if (calculateDetail) {
			String predDetailColName = params.get(ScorePredictParams.PREDICTION_DETAIL_COL);
			helperColNames.add(predDetailColName);
			helperColTypes.add(Types.STRING);
		}
		if (calculateScore) {
			String predictionScore = params.get(ScorePredictParams.PREDICTION_SCORE_COL);
			helperColNames.add(predictionScore);
			helperColTypes.add(Types.DOUBLE);
		}
		if (calculateScorePerFeature) {
			String[] predictionScorePerFeature = params.get(ScorePredictParams.PREDICTION_SCORE_PER_FEATURE_COLS);
			helperColNames.addAll(Arrays.asList(predictionScorePerFeature));
			for (String aPredictionScorePerFeature : predictionScorePerFeature) {
				helperColTypes.add(Types.DOUBLE);
			}
		}

		return Tuple4.of(dataSchema.getFieldNames(),
			helperColNames.toArray(new String[0]),
			helperColTypes.toArray(new TypeInformation <?>[0]),
			params.get(ScorecardPredictParams.RESERVED_COLS));
	}
}
