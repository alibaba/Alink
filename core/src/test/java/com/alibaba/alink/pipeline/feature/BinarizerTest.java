package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.BinarizerBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.BinarizerStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test for Binarizer.
 */

public class BinarizerTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1, 1.218, 16.0, "1.560 -0.605"),
			Row.of(2, 2.949, 4.0, "0.346 2.158"),
			Row.of(3, 3.627, 2.0, "1.380 0.231"),
			Row.of(4, 0.273, 15.0, "0.520 1.151"),
			Row.of(5, 4.199, 7.0, "0.795 -0.226")
		};

		List <Row> expectedRows = Arrays.asList(
			Row.of(1, 1.0),
			Row.of(2, 0.0),
			Row.of(3, 0.0),
			Row.of(4, 1.0),
			Row.of(5, 0.0)
		);

		BatchOperator <?> data = new MemSourceBatchOp(rows, new String[] {"id", "label", "censor", "features"});
		StreamOperator <?> dataStream = new MemSourceStreamOp(rows, new String[] {"id", "label", "censor", "features"
		});

		Binarizer op = new Binarizer()
			.setSelectedCol("censor")
			.setThreshold(8.0);

		//batch
		List <Row> list = op
			.transform(data)
			.select("id, censor")
			.collect();

		AlinkTestBase.assertListRowEqual(expectedRows, list, 0);

		CollectSinkStreamOp resS = op
			.transform(dataStream)
			.select("id, censor")
			.link(new CollectSinkStreamOp());

		StreamOperator.execute();

		AlinkTestBase.assertListRowEqual(expectedRows, resS.getAndRemoveValues(), 0);
	}

	@Test
	public void testInitializer() {
		Binarizer op = new Binarizer(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator<?> b = new BinarizerBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new BinarizerBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator<?> s = new BinarizerStreamOp();
		Assert.assertEquals(s.getParams().size(), 0);
		s = new BinarizerStreamOp(new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
