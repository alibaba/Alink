package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkFlinkExecutionErrorException;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.RankingMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.function.Consumer;

/**
 * Evaluation for Ranking system.
 */

public class EvalRankingBatchOpTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private final Row[] rows = new Row[] {
		Row.of("{\"object\":\"[1, 6, 2, 7, 8, 3, 9, 10, 4, 5]\"}", "{\"object\":\"[1, 2, 3, 4, 5]\"}"),
		Row.of("{\"object\":\"[4, 1, 5, 6, 2, 7, 3, 8, 9, 10]\"}", "{\"object\":\"[1, 2, 3]\"}"),
		Row.of("{\"object\":\"[1, 2, 3, 4, 5]\"}", "{\"object\":\"[]\"}")
	};

	@Test
	public void test() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"pred", "label"});

		EvalRankingBatchOp op = new EvalRankingBatchOp()
			.setLabelCol("label")
			.setPredictionCol("pred")
			.linkFrom(data);

		RankingMetrics metrics = op.collectMetrics();
		Assert.assertEquals(metrics.getNdcg(3), 0.33, 0.01);
		Assert.assertEquals(metrics.getNdcg(7), 0.42, 0.01);
		Assert.assertEquals(metrics.getNdcg(10), 0.48, 0.01);
		Assert.assertEquals(metrics.getNdcg(17), 0.48, 0.01);
		Assert.assertEquals(metrics.getPrecisionAtK(1), 0.33, 0.01);
		Assert.assertEquals(metrics.getPrecisionAtK(3), 0.33, 0.01);
		Assert.assertEquals(metrics.getPrecisionAtK(5), 0.26, 0.01);
		Assert.assertEquals(metrics.getPrecisionAtK(7), 0.28, 0.01);
		Assert.assertEquals(metrics.getPrecisionAtK(9), 0.26, 0.01);
		Assert.assertEquals(metrics.getPrecisionAtK(10), 0.26, 0.01);
		Assert.assertEquals(metrics.getPrecisionAtK(15), 0.17, 0.01);
		Assert.assertEquals(metrics.getMap(), 0.35, 0.01);
		Assert.assertEquals(metrics.getRecallAtK(1), 0.06, 0.01);
	}

	@Test
	public void testObjectCol() {
		Row[] rows = new Row[] {
			Row.of("{\"ID\":\"[1, 6, 2, 7, 8, 3, 9, 10, 4, 5]\"}", "{\"ID\":\"[1]\"}"),
			Row.of("{\"ID\":\"[1, 4, 5, 6, 2, 7, 3, 8, 9, 10]\"}", "{\"ID\":\"[1]\"}"),
			Row.of("{\"ID\":\"[1, 2, 3, 4, 5]\"}", "{\"ID\":\"[1]\"}")
		};

		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"pred", "label"});

		EvalRankingBatchOp op = new EvalRankingBatchOp()
			.setLabelCol("label")
			.setPredictionCol("pred")
			.setPredictionRankingInfo("ID")
			.setLabelRankingInfo("ID")
			.linkFrom(data);

		RankingMetrics metrics = op.collectMetrics();
		Assert.assertEquals(metrics.getRecallAtK(1), 1.0, 0.01);
	}

	@Test
	public void testLazyCollect() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"pred", "label"});

		EvalRankingBatchOp op = new EvalRankingBatchOp(new Params())
			.setLabelCol("label")
			.setPredictionCol("pred")
			.linkFrom(data);

		op.lazyCollectMetrics(new Consumer <RankingMetrics>() {
			@Override
			public void accept(RankingMetrics metrics) {
				System.err.println(
					"Map: " + metrics.getMap() + "\t Precision:" + metrics.getPrecision() + "\t Recall: " + metrics
						.getRecall()
						+ "\t F1: " + metrics.getF1());
			}
		});

		BatchOperator.execute();
	}

	@Test
	public void testEmptyInput() throws Exception {
		Row[] rows = new Row[] {
			Row.of(null, null),
			Row.of("1", null),
			Row.of(null, "1")
		};

		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows),
			new TableSchema(new String[] {"pred", "label"},
				new TypeInformation[] {Types.STRING, Types.STRING}));
		try {
			EvalRankingBatchOp op = new EvalRankingBatchOp()
				.setLabelCol("label")
				.setPredictionCol("pred")
				.linkFrom(data);
			op.print();
			Assert.fail("Expected an IllegalStateException to be thrown");
		} catch (JobExecutionException | ProgramInvocationException | AkFlinkExecutionErrorException e) {
			// pass
		}
	}

}