package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;
import com.alibaba.alink.operator.batch.dataproc.format.ColumnsToVectorBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import com.alibaba.alink.operator.batch.sql.SelectBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.AppendIdStreamOp;
import com.alibaba.alink.operator.stream.dataproc.format.ColumnsToVectorStreamOp;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.sink.AkSinkStreamOp;
import com.alibaba.alink.operator.stream.source.RandomTableSourceStreamOp;
import com.alibaba.alink.operator.stream.sql.SelectStreamOp;
import com.alibaba.alink.operator.stream.timeseries.LSTNetPredictStreamOp;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class LSTNetTrainBatchOpTest {

	@Test
	public void testLSTNetTrainBatchOp() throws Exception {
		BatchOperator.setParallelism(1);

		List <Row> data = Arrays.asList(
			Row.of(0, Timestamp.valueOf("2021-11-01 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-02 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-03 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-04 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-06 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-07 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-08 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-09 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-10 00:00:00"), 900.0),
			Row.of(0, Timestamp.valueOf("2021-11-11 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-12 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-13 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-14 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-15 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-16 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-17 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-18 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-19 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-20 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-21 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-22 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-23 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-24 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-25 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-26 00:00:00"), 900.0),
			Row.of(0, Timestamp.valueOf("2021-11-27 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-28 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-29 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-30 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-12-01 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-12-02 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-12-03 00:00:00"), 200.0)
		);

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "id int, ts timestamp, series double");

		LSTNetTrainBatchOp lstNetTrainBatchOp = new LSTNetTrainBatchOp()
			.setTimeCol("ts")
			.setSelectedCol("series")
			.setNumEpochs(10)
			.setWindow(24)
			.setHorizon(1);

		GroupByBatchOp groupData = new GroupByBatchOp()
			.setGroupByPredicate("id")
			.setSelectClause("mtable_agg(ts, series) as mtable_agg_series");

		LSTNetPredictBatchOp lstNetPredictBatchOp = new LSTNetPredictBatchOp()
			.setPredictNum(1)
			.setPredictionCol("pred")
			.setReservedCols()
			.setValueCol("mtable_agg_series");

		lstNetPredictBatchOp
			.linkFrom(
				lstNetTrainBatchOp.linkFrom(memSourceBatchOp),
				groupData.linkFrom(memSourceBatchOp.filter("ts >= TO_TIMESTAMP('2021-11-10 00:00:00')"))
			)
			.print();
	}

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Before
	public void setUp() {
		AlinkGlobalConfiguration.setAutoPluginDownload(true);
	}

	@Test
	public void testStreamMultiVar() throws Exception {
		BatchOperator.setParallelism(1);

		final int numCols = 10;
		final String timeColName = "ts";
		final String vecColName = "vec";

		final String selectClause = "TO_TIMESTAMP(" + timeColName + ") as " + timeColName + ", " + vecColName;

		BatchOperator <?> source = new RandomTableSourceBatchOp()
			.setNumRows(1000L)
			.setNumCols(numCols);

		String[] selectedColNames = source.getColNames();

		AppendIdBatchOp appendIdBatchOp = new AppendIdBatchOp()
			.setIdCol(timeColName)
			.linkFrom(source);

		ColumnsToVectorBatchOp columnsToVectorBatchOp = new ColumnsToVectorBatchOp()
			.setSelectedCols(selectedColNames)
			.setVectorCol(vecColName)
			.linkFrom(appendIdBatchOp);

		BatchOperator <?> timeBatchOp = new SelectBatchOp()
			.setClause(selectClause)
			.linkFrom(columnsToVectorBatchOp);

		LSTNetTrainBatchOp trainOp = new LSTNetTrainBatchOp()
			.setVectorCol(vecColName)
			.setTimeCol(timeColName)
			.setWindow(24 * 7)
			.setHorizon(12)
			.setNumEpochs(1)
			.linkFrom(timeBatchOp);

		StreamOperator <?> sourceStreamOp = new RandomTableSourceStreamOp()
			.setNumCols(numCols)
			.setMaxRows(1000L);

		ColumnsToVectorStreamOp columnsToVectorStreamOp = new ColumnsToVectorStreamOp()
			.setSelectedCols(selectedColNames)
			.setVectorCol(vecColName)
			.linkFrom(sourceStreamOp);

		AppendIdStreamOp appendIdStreamOp = new AppendIdStreamOp()
			.setIdCol(timeColName)
			.linkFrom(columnsToVectorStreamOp);

		StreamOperator <?> timestampStreamOp = new SelectStreamOp()
			.setClause(selectClause)
			.linkFrom(appendIdStreamOp);

		OverCountWindowStreamOp overCountWindowStreamOp = new OverCountWindowStreamOp()
			.setClause("MTABLE_AGG_PRECEDING(" + timeColName + ", " + vecColName + ") as col_agg")
			.setTimeCol(timeColName)
			.setPrecedingRows(24 * 7)
			.linkFrom(timestampStreamOp);

		LSTNetPredictStreamOp predictStreamOp = new LSTNetPredictStreamOp(trainOp)
			.setValueCol("col_agg")
			.setPredictionCol("pred")
			.setReservedCols(timeColName)
			.linkFrom(overCountWindowStreamOp);

		FilePath tmpAkFile = new FilePath(
			new Path(folder.getRoot().getPath(), "lstnet_test_stream_multi_var_result.ak")
		);

		predictStreamOp
			.link(
				new AkSinkStreamOp()
					.setOverwriteSink(true)
					.setFilePath(tmpAkFile)
			);

		StreamOperator.execute();
	}

	@Test
	public void testStreamSingleVar() throws Exception {
		BatchOperator.setParallelism(1);

		final int numCols = 1;
		final String timeColName = "ts";

		BatchOperator <?> source = new RandomTableSourceBatchOp()
			.setNumRows(1000L)
			.setNumCols(numCols);

		String colName = source.getColNames()[0];

		final String selectClause = "TO_TIMESTAMP(" + timeColName + ") as " + timeColName + ", " + colName;

		AppendIdBatchOp appendIdBatchOp = new AppendIdBatchOp()
			.setIdCol(timeColName)
			.linkFrom(source);

		BatchOperator <?> timeBatchOp = new SelectBatchOp()
			.setClause(selectClause)
			.linkFrom(appendIdBatchOp);

		LSTNetTrainBatchOp trainOp = new LSTNetTrainBatchOp()
			.setSelectedCol(colName)
			.setTimeCol(timeColName)
			.setWindow(24 * 7)
			.setHorizon(12)
			.setNumEpochs(1)
			.linkFrom(timeBatchOp);

		StreamOperator <?> sourceStreamOp = new RandomTableSourceStreamOp()
			.setNumCols(numCols)
			.setMaxRows(6000L);

		AppendIdStreamOp appendIdStreamOp = new AppendIdStreamOp()
			.setIdCol(timeColName)
			.linkFrom(sourceStreamOp);

		StreamOperator <?> timestampStreamOp = new SelectStreamOp()
			.setClause(selectClause)
			.linkFrom(appendIdStreamOp);

		OverCountWindowStreamOp overTimeWindowStreamOp = new OverCountWindowStreamOp()
			.setClause("MTABLE_AGG_PRECEDING(" + timeColName + ", " + colName + ") as col_agg")
			.setTimeCol(timeColName)
			.setPrecedingRows(24 * 7)
			.linkFrom(timestampStreamOp);

		LSTNetPredictStreamOp predictStreamOp = new LSTNetPredictStreamOp(trainOp)
			.setValueCol("col_agg")
			.setPredictionCol("pred")
			.setReservedCols(timeColName)
			.setNumThreads(4)
			.linkFrom(overTimeWindowStreamOp);

		FilePath tmpAkFile = new FilePath(
			new Path(folder.getRoot().getPath(), "lstnet_test_stream_single_var_result.ak")
		);

		predictStreamOp
			.link(
				new AkSinkStreamOp()
					.setOverwriteSink(true)
					.setFilePath(tmpAkFile)
			);

		StreamOperator.execute();
	}
}
