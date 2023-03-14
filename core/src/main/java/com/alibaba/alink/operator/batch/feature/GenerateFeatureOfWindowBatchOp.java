package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.fe.GenerateFeatureUtil;
import com.alibaba.alink.common.fe.define.BaseStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceWindowStatFeatures;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.feature.GenerateFeatureOfWindowParams;

import java.sql.Timestamp;
import java.util.List;

/**
 * Generate Feature Window.
 */
@NameCn("窗口特征生成")
@NameEn("Generate Feature of Window")
public class GenerateFeatureOfWindowBatchOp extends BatchOperator <GenerateFeatureOfWindowBatchOp>
	implements GenerateFeatureOfWindowParams <GenerateFeatureOfWindowBatchOp> {

	@Override
	public GenerateFeatureOfWindowBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		BaseStatFeatures <?>[] features = GenerateFeatureUtil.flattenFeatures(
			BaseStatFeatures.fromJson(getFeatureDefinitions(), BaseStatFeatures[].class)
		);

		final String timeCol = getTimeCol();

		Table[] sideOutputTables = new Table[features.length];

		for (int iFeature = 0; iFeature < features.length; iFeature++) {
			BaseStatFeatures <?> feature = features[iFeature];

			//step1: group to MTable.
			String[] groupCols = feature.groupCols;
			BatchOperator <?> inGrouped = GenerateFeatureUtil.group2MTables(in, groupCols);

			//step2: prepare
			int outFeatureNum = feature.getOutColNames().length;
			int groupColNum = groupCols.length;



			TableSchema outSchema = GenerateFeatureUtil.getWindowOutSchema(feature, in.getSchema());
			int outColNum = outSchema.getFieldNames().length;

			//step3: stat
			DataSet <Row> singleStatDataSet = inGrouped.getDataSet()
				.flatMap(new FlatMapFunction <Row, Row>() {
					@Override
					public void flatMap(Row value, Collector <Row> out) throws Exception {
						MTable mt = MTableUtil.getMTable(value.getField(0));
						mt.orderBy(timeCol);

						List <Tuple4 <Integer, Integer, Timestamp, Timestamp>> indices =
							GenerateFeatureUtil.findMtIndices(mt, timeCol, (InterfaceWindowStatFeatures) feature);

						//set out
						int[] groupColIndices = TableUtil.findColIndices(mt.getColNames(), groupCols);
						Row firstRow = mt.getRow(0);

						Object[] outObj = new Object[feature.getOutColNames().length];
						for (Tuple4 <Integer, Integer, Timestamp, Timestamp> ins : indices) {
							GenerateFeatureUtil.calStatistics(mt, ins, timeCol, feature, outObj);
							out.collect(GenerateFeatureUtil.setOutRow(outColNum, firstRow, groupColIndices,
								groupColNum, ins.f2, ins.f3, outFeatureNum, outObj));
						}
					}
				});

			//construct single output for one feature.
			sideOutputTables[iFeature] =
				DataSetConversionUtil.toTable(in.getMLEnvironmentId(), singleStatDataSet, outSchema);
		}

		//step4: set output.
		this.setOutputTable(sideOutputTables[0]);
		this.setSideOutputTables(sideOutputTables);

		return this;
	}

}
