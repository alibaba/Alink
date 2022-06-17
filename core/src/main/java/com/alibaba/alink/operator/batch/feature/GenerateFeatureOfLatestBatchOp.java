package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.feature.GenerateFeatureOfLatestParams;
import com.alibaba.alink.common.fe.GenerateFeatureUtil;
import com.alibaba.alink.common.fe.def.BaseStatFeatures;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.FlattenMTableBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Latest Feature Window.
 */
@NameCn("Latest特征生成")
public class GenerateFeatureOfLatestBatchOp extends BatchOperator <GenerateFeatureOfLatestBatchOp>
	implements GenerateFeatureOfLatestParams <GenerateFeatureOfLatestBatchOp> {

	@Override
	public GenerateFeatureOfLatestBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		//step 1: merge stat features by group cols;
		Map <String[], Tuple2 <List <BaseStatFeatures <?>>, List <Integer>>> mergedFeatures =
			GenerateFeatureUtil.mergeFeatures(
				GenerateFeatureUtil.flattenFeatures(
					BaseStatFeatures.fromJson(getFeatureDefinitions(), BaseStatFeatures[].class)
				)
			);

		final String timeCol = getTimeCol();

		TableSchema outSchema = in.getSchema();

		for (Map.Entry <String[], Tuple2 <List <BaseStatFeatures <?>>, List <Integer>>> entry :
			mergedFeatures.entrySet()) {
			String[] groupCols = entry.getKey();
			List <BaseStatFeatures <?>> features = entry.getValue().f0;

			//step2: group to MTable.
			BatchOperator <?> inGrouped = GenerateFeatureUtil.group2MTables(in, groupCols);

			outSchema = GenerateFeatureUtil.getOutMTableSchema(outSchema, features);

			//step3: stat
			DataSet <Row> statDataSet = inGrouped.getDataSet()
				.map(new MapFunction <Row, Row>() {
					@Override
					public Row map(Row value) {
						MTable mt = MTableUtil.getMTable(value.getField(0));
						mt.orderBy(timeCol);

						int n = mt.getNumRow();

						TableSchema outSchemaM = GenerateFeatureUtil.getOutMTableSchema(mt.getSchema(), features);

						Row[] outRows = new Row[n];
						for (int i = 0; i < n; i++) {
							outRows[i] = new Row(outSchemaM.getFieldNames().length);
						}
						MTable outMt = new MTable(outRows, outSchemaM);

						for (int i = 0; i < n; i++) {
							for (int j = 0; j < mt.getNumCol(); j++) {
								outMt.setEntry(i, j, mt.getEntry(i, j));
							}
						}

						int startIdx = mt.getNumCol();
						int timeIdx = TableUtil.findColIndex(mt.getColNames(), timeCol);
						for (BaseStatFeatures <?> feature : features) {
							Object[] outObj = new Object[feature.getOutColNames().length];
							for (int i = 0; i < n; i++) {
								Timestamp curTs = (Timestamp) mt.getEntry(i, timeIdx);
								int fromIdx = Math.max(0,
									GenerateFeatureUtil.findStartIdx(mt, timeCol, feature, curTs, i));
								int endIdx = i + 1;
								Timestamp startTime = (Timestamp) mt.getEntry(fromIdx,
									TableUtil.findColIndex(mt.getSchema(), timeCol));

								GenerateFeatureUtil.calStatistics(mt, Tuple4.of(fromIdx, endIdx, startTime, curTs),
									timeCol, feature, outObj);

								for (int j = 0; j < outObj.length; j++) {
									outMt.setEntry(i, startIdx + j, outObj[j]);
								}
							}
							startIdx += outObj.length;
						}
						Row out = new Row(1);
						out.setField(0, outMt);
						return out;
					}
				});

			//step4: flatten MTable
			in = new TableSourceBatchOp(
				DataSetConversionUtil.toTable(
					inGrouped.getMLEnvironmentId(),
					statDataSet,
					inGrouped.getSchema()))
				.link(new FlattenMTableBatchOp()
					.setMLEnvironmentId(in.getMLEnvironmentId())
					.setSelectedCol(GenerateFeatureUtil.TEMP_MTABLE_COL)
					.setReservedCols()
					.setSchemaStr(TableUtil.schema2SchemaStr(outSchema))
				);
		}

		this.setOutputTable(in.getOutputTable());

		return this;
	}

}

