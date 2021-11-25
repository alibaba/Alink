package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
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
import com.alibaba.alink.operator.stream.feature.HopTimeWindowStreamOp;
import com.alibaba.alink.operator.stream.feature.TumbleTimeWindowStreamOp;
import com.alibaba.alink.operator.stream.sink.AkSinkStreamOp;
import com.alibaba.alink.operator.stream.source.RandomTableSourceStreamOp;
import com.alibaba.alink.operator.stream.sql.SelectStreamOp;
import com.alibaba.alink.operator.stream.timeseries.DeepARPredictStreamOp;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class DeepARTrainBatchOpTest {

	@Test
	public void testDeepARTrainBatchOp() throws Exception {
		BatchOperator.setParallelism(1);

		List <Row> data = Arrays.asList(
			Row.of(0, Timestamp.valueOf("2021-11-01 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-02 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-03 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-04 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-05 00:00:00"), 100.0)
		);

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "id int, ts timestamp, series double");

		DeepARTrainBatchOp deepARTrainBatchOp = new DeepARTrainBatchOp()
			.setTimeCol("ts")
			.setSelectedCol("series")
			.setNumEpochs(10)
			.setWindow(2)
			.setStride(1);

		GroupByBatchOp groupData = new GroupByBatchOp()
			.setGroupByPredicate("id")
			.setSelectClause("mtable_agg(ts, series) as mtable_agg_series");

		DeepARPredictBatchOp deepARPredictBatchOp = new DeepARPredictBatchOp()
			.setPredictNum(2)
			.setPredictionCol("pred")
			.setValueCol("mtable_agg_series");

		deepARPredictBatchOp
			.linkFrom(
				deepARTrainBatchOp.linkFrom(memSourceBatchOp),
				groupData.linkFrom(memSourceBatchOp)
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
	public void testMultiVar() throws Exception {
		BatchOperator.setParallelism(1);
		final String timeColName = "ts";
		final int numCols = 10;
		final String vecColName = "vec";

		BatchOperator <?> source = new RandomTableSourceBatchOp()
			.setNumRows(1000L)
			.setNumCols(numCols);

		String[] colNames = source.getColNames();

		AppendIdBatchOp appendIdBatchOp = new AppendIdBatchOp()
			.setIdCol(timeColName)
			.linkFrom(source);

		BatchOperator <?> timeBatchOp = new SelectBatchOp()
			.setClause(
				String.format(
					"%s, FLOOR(TO_TIMESTAMP(%s * 3600000) TO HOUR) as %s",
					Joiner.on(",").join(colNames),
					timeColName,
					timeColName
				)
			)
			.linkFrom(appendIdBatchOp);

		StringBuilder selectClause = new StringBuilder();
		StringBuilder groupByPredicate = new StringBuilder();

		selectClause.append(timeColName);

		for (int i = 0; i < numCols; ++i) {
			selectClause.append(", ");
			selectClause.append(String.format("SUM(%s) as %s", colNames[i], colNames[i]));
		}

		groupByPredicate.append(timeColName);

		BatchOperator <?> groupedTimeBatchOp = new GroupByBatchOp()
			.setSelectClause(selectClause.toString())
			.setGroupByPredicate(groupByPredicate.toString())
			.linkFrom(timeBatchOp);

		ColumnsToVectorBatchOp columnsToVectorBatchOp = new ColumnsToVectorBatchOp()
			.setSelectedCols(colNames)
			.setVectorCol(vecColName)
			.linkFrom(groupedTimeBatchOp);

		BatchOperator <?> deepArTrainBatchOp = new DeepARTrainBatchOp()
			.setVectorCol(vecColName)
			.setTimeCol(timeColName)
			.setWindow(24 * 7)
			.setStride(24)
			.setNumEpochs(1)
			.linkFrom(columnsToVectorBatchOp);

		StreamOperator <?> sourceStreamOp = new RandomTableSourceStreamOp()
			.setNumCols(numCols)
			.setMaxRows(1000L);

		AppendIdStreamOp appendIdStreamOp = new AppendIdStreamOp()
			.setIdCol(timeColName)
			.linkFrom(sourceStreamOp);

		StreamOperator <?> timeStreamOp = new SelectStreamOp()
			.setClause(
				String.format(
					"%s, FLOOR(TO_TIMESTAMP(%s * 3600000) TO HOUR) as %s",
					Joiner.on(",").join(colNames),
					timeColName,
					timeColName
				)
			)
			.linkFrom(appendIdStreamOp);

		StringBuilder selectClausePred = new StringBuilder();

		selectClausePred.append(String.format("TUMBLE_START() as %s", timeColName));

		for (int i = 0; i < numCols; ++i) {
			selectClausePred.append(", ");
			selectClausePred.append(String.format("SUM(%s) as %s", colNames[i], colNames[i]));
		}

		TumbleTimeWindowStreamOp timeWindowStreamOp = new TumbleTimeWindowStreamOp()
			.setWindowTime(3600)
			.setTimeCol(timeColName)
			.setClause(selectClausePred.toString())
			.linkFrom(timeStreamOp);

		ColumnsToVectorStreamOp columnsToVectorStreamOp = new ColumnsToVectorStreamOp()
			.setSelectedCols(colNames)
			.setVectorCol(vecColName)
			.linkFrom(timeWindowStreamOp);

		HopTimeWindowStreamOp hopTimeWindowStreamOp = new HopTimeWindowStreamOp()
			.setTimeCol(timeColName)
			.setClause(String.format("MTABLE_AGG(%s, %s) as %s", timeColName, vecColName, "mt"))
			.setHopTime(24 * 3600)
			.setWindowTime((24 * 7 - 24) * 3600)
			.linkFrom(columnsToVectorStreamOp);

		DeepARPredictStreamOp deepARPredictStreamOp = new DeepARPredictStreamOp(deepArTrainBatchOp)
			.setValueCol("mt")
			.setPredictionCol("pred")
			.linkFrom(hopTimeWindowStreamOp);

		FilePath tmpAkFile = new FilePath(
			new Path(folder.getRoot().getPath(), "deepar_test_stream_multi_var_result.ak")
		);

		deepARPredictStreamOp
			.link(
				new AkSinkStreamOp()
					.setOverwriteSink(true)
					.setFilePath(tmpAkFile)
			);

		StreamOperator.execute();
	}

	@Test
	public void testSingleVar() throws Exception {
		BatchOperator.setParallelism(1);
		final String timeColName = "ts";

		BatchOperator <?> source = new RandomTableSourceBatchOp()
			.setNumRows(1000L)
			.setNumCols(1);

		String colName = source.getColNames()[0];

		AppendIdBatchOp appendIdBatchOp = new AppendIdBatchOp()
			.setIdCol(timeColName)
			.linkFrom(source);

		BatchOperator <?> timeBatchOp = new SelectBatchOp()
			.setClause(
				String.format(
					"%s, FLOOR(TO_TIMESTAMP(%s * 3600000) TO HOUR) as %s",
					colName,
					timeColName,
					timeColName
				)
			)
			.linkFrom(appendIdBatchOp);

		StringBuilder groupByPredicate = new StringBuilder();

		String selectClause = timeColName
			+ String.format(", SUM(%s) as %s", colName, colName);

		groupByPredicate.append(timeColName);

		BatchOperator <?> groupedTimeBatchOp = new GroupByBatchOp()
			.setSelectClause(selectClause)
			.setGroupByPredicate(groupByPredicate.toString())
			.linkFrom(timeBatchOp);

		BatchOperator <?> deepArTrainBatchOp = new DeepARTrainBatchOp()
			.setSelectedCol(colName)
			.setTimeCol(timeColName)
			.setWindow(24 * 7)
			.setStride(24)
			.setNumEpochs(1)
			.linkFrom(groupedTimeBatchOp);

		StreamOperator <?> sourceStreamOp = new RandomTableSourceStreamOp()
			.setNumCols(1)
			.setMaxRows(1000L);

		AppendIdStreamOp appendIdStreamOp = new AppendIdStreamOp()
			.setIdCol(timeColName)
			.linkFrom(sourceStreamOp);

		StreamOperator <?> timeStreamOp = new SelectStreamOp()
			.setClause(
				String.format(
					"%s, FLOOR(TO_TIMESTAMP(%s * 3600000) TO HOUR) as %s",
					colName,
					timeColName,
					timeColName
				)
			)
			.linkFrom(appendIdStreamOp);

		String selectClausePred = String.format("TUMBLE_START() as %s", timeColName)
			+ String.format(", SUM(%s) as %s", colName, colName);

		TumbleTimeWindowStreamOp timeWindowStreamOp = new TumbleTimeWindowStreamOp()
			.setWindowTime(3600)
			.setTimeCol(timeColName)
			.setClause(selectClausePred)
			.linkFrom(timeStreamOp);

		HopTimeWindowStreamOp hopTimeWindowStreamOp = new HopTimeWindowStreamOp()
			.setTimeCol(timeColName)
			.setClause(String.format("MTABLE_AGG(%s, %s) as %s", timeColName, colName, "mt"))
			.setHopTime(24 * 3600)
			.setWindowTime((24 * 7 - 24) * 3600)
			.linkFrom(timeWindowStreamOp);

		DeepARPredictStreamOp deepARPredictStreamOp = new DeepARPredictStreamOp(deepArTrainBatchOp)
			.setValueCol("mt")
			.setPredictionCol("pred")
			.setPredictNum(24)
			.linkFrom(hopTimeWindowStreamOp);

		FilePath tmpAkFile = new FilePath(
			new Path(folder.getRoot().getPath(), "deepar_test_stream_single_var_result.ak")
		);

		deepARPredictStreamOp
			.link(
				new AkSinkStreamOp()
					.setOverwriteSink(true)
					.setFilePath(tmpAkFile)
			);

		StreamOperator.execute();
	}
}
