package com.alibaba.alink.operator.common.finance;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.mapper.ComboModelMapper;
import com.alibaba.alink.common.mapper.PipelineModelMapper;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.params.finance.BinningPredictParams;
import com.alibaba.alink.params.finance.ScorePredictParams;
import com.alibaba.alink.params.finance.ScorecardPredictParams;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.pipeline.ModelExporterUtils;
import com.alibaba.alink.pipeline.PipelineStageBase;
import com.alibaba.alink.pipeline.feature.BinningModel;
import com.alibaba.alink.pipeline.finance.ScoreModel;
import org.apache.commons.lang3.ArrayUtils;

import java.util.List;

public class ScorecardModelMapper extends ComboModelMapper {
	private static final long serialVersionUID = 7877677418109112341L;
	private static String SCORE_SUFFIX = "_SCORE";

	public ScorecardModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
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
		for (int i = 0; i < stages.size(); i++) {
			PipelineStageBase <?> stage = stages.get(i).f0;

			stage.getParams().merge(this.params);

			if (stage instanceof BinningModel) {
				stage.getParams().set(ScorecardPredictParams.RESERVED_COLS, null);
				this.params.set(HasFeatureCols.FEATURE_COLS,
					stage.getParams().get(BinningPredictParams.OUTPUT_COLS));
			}

			if (stage instanceof ScoreModel) {
				if (i < stages.size() - 1) {
					stage.getParams().remove(ScorePredictParams.PREDICTION_SCORE_COL);
					stage.getParams().remove(ScorePredictParams.PREDICTION_SCORE_PER_FEATURE_COLS);
					stage.getParams().remove(ScorePredictParams.CALCULATE_SCORE_PER_FEATURE);
					stage.getParams().set(ScorePredictParams.RESERVED_COLS, null);
					stages.get(i + 1).f0.getParams().merge(this.params);
					stages.get(i + 1).f0.getParams().remove(ScorePredictParams.PREDICTION_DETAIL_COL);
					stages.get(i + 1).f0.getParams().set(
						ScorePredictParams.RESERVED_COLS,
						ArrayUtils.add(
							stages.get(i + 1).f0.getParams().get(ScorePredictParams.RESERVED_COLS),
							stage.getParams().get(ScorePredictParams.PREDICTION_DETAIL_COL)
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

		ScorePredictMapper emptyMapper = new ScorePredictMapper(
			new LinearModelDataConverter(labelType).getModelSchema(),
			dataSchema, params
		);
		Tuple4 <String[], String[], TypeInformation <?>[], String[]> tuple4 = emptyMapper.prepareIoSchema(modelSchema,
			dataSchema, params);
		return Tuple4.of(dataSchema.getFieldNames(), tuple4.f1, tuple4.f2,
			params.get(ScorecardPredictParams.RESERVED_COLS));
	}
}
