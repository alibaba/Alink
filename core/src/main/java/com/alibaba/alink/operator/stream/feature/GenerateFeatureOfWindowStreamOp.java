package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.params.feature.GenerateFeatureOfWindowParams;
import com.alibaba.alink.common.fe.GenerateFeatureUtil;
import com.alibaba.alink.common.fe.def.BaseStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceHopWindowStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceSessionWindowStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceSlotWindowStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceTumbleWindowStatFeatures;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.stream.StreamOperator;

import java.sql.Timestamp;

/**
 * Generate Feature Window.
 */
@NameCn("Window特征生成")
public class GenerateFeatureOfWindowStreamOp extends StreamOperator <GenerateFeatureOfWindowStreamOp>
	implements GenerateFeatureOfWindowParams <GenerateFeatureOfWindowStreamOp> {

	@Override
	public GenerateFeatureOfWindowStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);

		//step 1: merge stat features by group cols;
		BaseStatFeatures <?>[] features =
			GenerateFeatureUtil.flattenFeatures(
				BaseStatFeatures.fromJson(getFeatureDefinitions(), BaseStatFeatures[].class));

		String timeCol = getTimeCol();

		Table[] sideOutputTables = new Table[features.length];
		for (int iFeature = 0; iFeature < features.length; iFeature++) {
			BaseStatFeatures <?> feature = features[iFeature];

			//step1: group to MTable
			StreamOperator <?> inGrouped = group2MTable(in, feature, timeCol);

			//step2: prepare
			String[] inGroupCols = inGrouped.getColNames();
			String[] groupCols = feature.groupCols;

			int outFeatureNum = feature.getOutColNames().length;
			int groupColNum = groupCols.length;

			int mTableIdx = TableUtil.findColIndex(inGroupCols, GenerateFeatureUtil.TEMP_MTABLE_COL);
			int startTimeIdx = TableUtil.findColIndex(inGrouped.getColNames(), "startTime");
			int endTimeIdx = TableUtil.findColIndex(inGrouped.getColNames(), "endTime");
			int[] groupColIndices = TableUtil.findColIndices(inGroupCols, groupCols);

			TableSchema outSchema = GenerateFeatureUtil.getWindowOutSchema(feature, in.getSchema());
			int outColNum = outSchema.getFieldNames().length;

			//step3: stat
			DataStream <Row> statDataStream = inGrouped.getDataStream()
				.map(new MapFunction <Row, Row>() {
					@Override
					public Row map(Row value) throws Exception {
						MTable mt = MTableUtil.getMTable(value.getField(mTableIdx));
						mt.orderBy(timeCol);

						//find start row index in mt.
						// SlotWindow and HopWindow has the same window data, but startIdx different.
						int startIdx = 0;
						Timestamp curTs = (Timestamp) value.getField(endTimeIdx);
						Timestamp startTime = (Timestamp) value.getField(startTimeIdx);
						if (feature instanceof InterfaceSlotWindowStatFeatures) {
							startTime = GenerateFeatureUtil.getStartTime(curTs,
								((InterfaceSlotWindowStatFeatures) feature).getWindowTimes()[0],
								true);
							startIdx = GenerateFeatureUtil.findMtIdx(mt, timeCol, startTime);
						}

						//calc statistics.
						Object[] outObj = new Object[outFeatureNum];
						GenerateFeatureUtil.calStatistics(mt, Tuple4.of(startIdx, mt.getNumRow(), startTime, curTs),
							timeCol, feature, outObj);

						return GenerateFeatureUtil.setOutRow(outColNum, value, groupColIndices,
							groupColNum, startTime, (Timestamp) value.getField(endTimeIdx), outFeatureNum, outObj);
					}
				});

			//construct single output for one feature.
			sideOutputTables[iFeature] =
				DataStreamConversionUtil.toTable(in.getMLEnvironmentId(), statDataStream, outSchema);
		}

		//step4: set output.
		this.setOutputTable(sideOutputTables[0]);
		this.setSideOutputTables(sideOutputTables);

		return this;
	}

	private static StreamOperator <?> group2MTable(StreamOperator <?> in, BaseStatFeatures <?> feature,
												   String timeCol) {
		String featureStr = String.join(",", GenerateFeatureUtil.getFeatureNames(feature, timeCol));
		String clauseFormat = "mtable_agg(%s) as %s, %s() as startTime, %s() as endTime";
		if (feature instanceof InterfaceHopWindowStatFeatures) {
			return in.link(
				new HopTimeWindowStreamOp()
					.setTimeCol(timeCol)
					.setHopTime((int) GenerateFeatureUtil.getIntervalBySecond(
						((InterfaceHopWindowStatFeatures) feature).getHopTimes()[0]))
					.setWindowTime((int) GenerateFeatureUtil.getIntervalBySecond(
						((InterfaceHopWindowStatFeatures) feature).getWindowTimes()[0]))
					.setGroupCols(feature.groupCols)
					.setClause(
						String.format(clauseFormat,
							featureStr, GenerateFeatureUtil.TEMP_MTABLE_COL, "HOP_START", "HOP_END"))
					.setMLEnvironmentId(in.getMLEnvironmentId())
			);
		} else if (feature instanceof InterfaceSessionWindowStatFeatures) {
			return in.link(
				new SessionTimeWindowStreamOp()
					.setTimeCol(timeCol)
					.setSessionGapTime((int) GenerateFeatureUtil.getIntervalBySecond(
						((InterfaceSessionWindowStatFeatures) feature).getSessionGapTimes()[0]))
					.setGroupCols(feature.groupCols)
					.setClause(
						String.format(clauseFormat,
							featureStr, GenerateFeatureUtil.TEMP_MTABLE_COL, "SESSION_START", "SESSION_END"))
					.setMLEnvironmentId(in.getMLEnvironmentId())
			);

		} else if (feature instanceof InterfaceTumbleWindowStatFeatures) {
			return in.link(
				new TumbleTimeWindowStreamOp()
					.setTimeCol(timeCol)
					.setWindowTime((int) GenerateFeatureUtil.getIntervalBySecond(
						((InterfaceTumbleWindowStatFeatures) feature).getWindowTimes()[0]))
					.setGroupCols(feature.groupCols)
					.setClause(
						String.format(clauseFormat,
							featureStr, GenerateFeatureUtil.TEMP_MTABLE_COL, "TUMBLE_START", "TUMBLE_END"))
					.setMLEnvironmentId(in.getMLEnvironmentId())
			);
		} else if (feature instanceof InterfaceSlotWindowStatFeatures) {
			return in.link(
				new HopTimeWindowStreamOp()
					.setTimeCol(timeCol)
					.setHopTime((int) GenerateFeatureUtil.getIntervalBySecond(
						((InterfaceSlotWindowStatFeatures) feature).getStepTimes()[0]))
					.setWindowTime((int) GenerateFeatureUtil.getIntervalBySecond(
						((InterfaceSlotWindowStatFeatures) feature).getWindowTimes()[0]))
					.setGroupCols(feature.groupCols)
					.setClause(
						String.format(clauseFormat,
							featureStr, GenerateFeatureUtil.TEMP_MTABLE_COL, "HOP_START", "HOP_END"))
					.setMLEnvironmentId(in.getMLEnvironmentId())
			);
		} else {
			throw new AkUnsupportedOperationException("It is not support yet." + feature.getClass().getName());
		}

	}
}
