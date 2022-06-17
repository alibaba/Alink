package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.feature.GenerateFeatureOfLatestParams;
import com.alibaba.alink.common.fe.GenerateFeatureUtil;
import com.alibaba.alink.common.fe.def.InterfaceNStatFeatures;
import com.alibaba.alink.common.fe.def.BaseStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceTimeIntervalStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceTimeSlotStatFeatures;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;

import java.sql.Timestamp;

/**
 * Latest Feature Window.
 */
@NameCn("Latest特征生成")
public class GenerateFeatureOfLatestStreamOp extends StreamOperator <GenerateFeatureOfLatestStreamOp>
	implements GenerateFeatureOfLatestParams <GenerateFeatureOfLatestStreamOp> {

	@Override
	public GenerateFeatureOfLatestStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);

		BaseStatFeatures <?>[] features =
			GenerateFeatureUtil.flattenFeatures(
				BaseStatFeatures.fromJson(getFeatureDefinitions(), BaseStatFeatures[].class));

		final String timeCol = getTimeCol();

		for (BaseStatFeatures <?> feature : features) {
			//step2: group to MTable.
			StreamOperator <?> inGrouped = null;
			String clause = String.format("MTABLE_AGG(%s) as %s",
				String.join(",", GenerateFeatureUtil.getFeatureNames(feature, timeCol)),
				GenerateFeatureUtil.TEMP_MTABLE_COL);

			if (feature instanceof InterfaceNStatFeatures) {
				inGrouped = in.link(
					new OverCountWindowStreamOp()
						.setTimeCol(timeCol)
						.setPrecedingRows(((InterfaceNStatFeatures) feature).getNumbers()[0])
						.setGroupCols(feature.groupCols)
						.setClause(clause)
						.setMLEnvironmentId(in.getMLEnvironmentId())
				);
			} else if (feature instanceof InterfaceTimeIntervalStatFeatures ||
				feature instanceof InterfaceTimeSlotStatFeatures) {
				String windowTime;
				if (feature instanceof InterfaceTimeIntervalStatFeatures) {
					windowTime = ((InterfaceTimeIntervalStatFeatures) feature).getTimeIntervals()[0];
				} else {
					windowTime = ((InterfaceTimeSlotStatFeatures) feature).getTimeSlots()[0];
				}
				inGrouped = in.link(
					new OverTimeWindowStreamOp()
						.setTimeCol(timeCol)
						.setPrecedingTime((int) GenerateFeatureUtil.getIntervalBySecond(windowTime))
						.setGroupCols(feature.groupCols)
						.setClause(clause)
						.setMLEnvironmentId(in.getMLEnvironmentId())
				);
			}

			int mTableIdx = TableUtil.findColIndex(inGrouped.getColNames(), GenerateFeatureUtil.TEMP_MTABLE_COL);
			int timeIdx = TableUtil.findColIndex(inGrouped.getColNames(), timeCol);

			//step3: stat
			DataStream <Row> statDataSet = inGrouped.getDataStream()
				.map(new MapFunction <Row, Row>() {
					@Override
					public Row map(Row value) {
						MTable mt = MTableUtil.getMTable(value.getField(mTableIdx));
						Timestamp curTs = (Timestamp) value.getField(timeIdx);

						mt.orderBy(timeCol);

						int featureNum = feature.getOutColNames().length;
						Object[] outObj = new Object[featureNum];

						int startIdx = 0;
						if (feature instanceof InterfaceTimeSlotStatFeatures) {
							startIdx = GenerateFeatureUtil.findStartIdx(mt, timeCol, feature,
								curTs, 0);
						}
						Timestamp startTs = (Timestamp) mt.getEntry(startIdx,
							TableUtil.findColIndex(mt.getSchema(), timeCol));
						Timestamp endTs = (Timestamp) mt.getEntry(mt.getNumRow() - 1,
							TableUtil.findColIndex(mt.getSchema(), timeCol));
						GenerateFeatureUtil.calStatistics(mt, Tuple4.of(startIdx, mt.getNumRow(), startTs, endTs),
							timeCol, feature, outObj);

						Row out = new Row(value.getArity() - 1 + featureNum);
						for (int i = 0; i < mTableIdx; i++) {
							out.setField(i, value.getField(i));
						}

						for (int i = mTableIdx + 1; i < value.getArity(); i++) {
							out.setField(i - 1, value.getField(i));
						}
						for (int i = 0; i < featureNum; i++) {
							out.setField(value.getArity() - 1 + i, outObj[i]);
						}

						return out;
					}
				});

			//for iter
			in = new TableSourceStreamOp(DataStreamConversionUtil.toTable(
				inGrouped.getMLEnvironmentId(),
				statDataSet,
				GenerateFeatureUtil.getOutMTableSchema(in.getSchema(), feature)));

			in.getOutputTable().printSchema();
		}

		//step4: flatten MTable
		this.setOutputTable(
			in.getOutputTable());

		return this;
	}

}

