package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.dataproc.SortUtils.RowComparator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.feature.Binarizer;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CollectSinkStreamOpTest extends AlinkTestBase {

	@Test
	public void testSink() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1, 1.218, 16.0, "1.560 -0.605"),
			Row.of(2, 2.949, 4.0, "0.346 2.158"),
			Row.of(3, 3.627, 2.0, "1.380 0.231"),
			Row.of(4, 0.273, 15.0, "0.520 1.151"),
			Row.of(5, 4.199, 7.0, "0.795 -0.226")
		};

		StreamOperator <?> dataStream = new MemSourceStreamOp(rows, new String[] {"id", "label", "censor", "features"
		});

		Binarizer op = new Binarizer()
			.setSelectedCol("censor")
			.setThreshold(8.0)
			.setNumThreads(2);

		StreamOperator <?> resS = op.transform(dataStream);

		CollectSinkStreamOp collectSinkStreamOp = resS.link(new CollectSinkStreamOp());
		CollectSinkStreamOp collectSinkStreamOp2 = resS.link(new CollectSinkStreamOp());

		StreamOperator.execute();

		List <Row> sResult = collectSinkStreamOp.getAndRemoveValues();
		sResult.sort(new RowComparator(0));

		Assert.assertEquals(sResult.get(0).getField(0), 1);
		Assert.assertEquals(sResult.get(1).getField(0), 2);
		Assert.assertEquals(sResult.get(2).getField(0), 3);
		Assert.assertEquals(sResult.get(3).getField(0), 4);
		Assert.assertEquals(sResult.get(4).getField(0), 5);

		List <Row> sResult2 = collectSinkStreamOp2.getAndRemoveValues();
		sResult2.sort(new RowComparator(0));

		Assert.assertEquals(sResult2.get(0).getField(0), 1);
		Assert.assertEquals(sResult2.get(1).getField(0), 2);
		Assert.assertEquals(sResult2.get(2).getField(0), 3);
		Assert.assertEquals(sResult2.get(3).getField(0), 4);
		Assert.assertEquals(sResult2.get(4).getField(0), 5);
	}

	@Test
	public void testSink2() throws Exception {
		Row[] rows = new Row[] {
			Row.of(6, 1.218, 16.0, "1.560 -0.605"),
			Row.of(7, 2.949, 4.0, "0.346 2.158"),
			Row.of(8, 3.627, 2.0, "1.380 0.231"),
			Row.of(9, 0.273, 15.0, "0.520 1.151"),
			Row.of(10, 4.199, 7.0, "0.795 -0.226")
		};

		StreamOperator <?> dataStream = new MemSourceStreamOp(rows, new String[] {"id", "label", "censor", "features"
		});

		Binarizer op = new Binarizer()
			.setSelectedCol("censor")
			.setThreshold(8.0)
			.setNumThreads(2);

		StreamOperator <?> resS = op.transform(dataStream);

		CollectSinkStreamOp collectSinkStreamOp = resS.link(new CollectSinkStreamOp());
		CollectSinkStreamOp collectSinkStreamOp2 = resS.link(new CollectSinkStreamOp());

		StreamOperator.execute();

		List <Row> sResult = collectSinkStreamOp.getAndRemoveValues();
		sResult.sort(new RowComparator(0));

		Assert.assertEquals(sResult.get(0).getField(0), 6);
		Assert.assertEquals(sResult.get(1).getField(0), 7);
		Assert.assertEquals(sResult.get(2).getField(0), 8);
		Assert.assertEquals(sResult.get(3).getField(0), 9);
		Assert.assertEquals(sResult.get(4).getField(0), 10);

		List <Row> sResult2 = collectSinkStreamOp2.getAndRemoveValues();
		sResult2.sort(new RowComparator(0));

		Assert.assertEquals(sResult2.get(0).getField(0), 6);
		Assert.assertEquals(sResult2.get(1).getField(0), 7);
		Assert.assertEquals(sResult2.get(2).getField(0), 8);
		Assert.assertEquals(sResult2.get(3).getField(0), 9);
		Assert.assertEquals(sResult2.get(4).getField(0), 10);
	}

}