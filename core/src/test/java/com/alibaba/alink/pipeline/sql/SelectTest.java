package com.alibaba.alink.pipeline.sql;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.testutil.AlinkTestBase;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SelectTest extends AlinkTestBase {

	@Test
	public void testTransform() throws Exception {
		Select select = new Select()
			.setClause("*, f_string as fas, f_double as fd");

		select.transform(data()).print();
		select.transform(datas()).print();

		StreamOperator.execute();
	}

	@Test
	public void testLocalPredictor() throws Exception {
		Select select = new Select()
			.setClause("*, f_string as fas, f_double as fd");

		BatchOperator <?> data = data();
		LocalPredictor lP = select.collectLocalPredictor(data.getSchema());
		Object[] out = lP.predict("a", 1L, 1, 2.0, true);
		Assert.assertEquals(new Object[] {"a", 1L, 1, 2.0, true, "a", 2.0}, out);
	}

	@Test
	public void testLocalPredictor2() throws Exception {
		Select select = new Select()
			.setClause("*, f_string as fas, f_double + 1 as fd");

		BatchOperator <?> data = data();
		LocalPredictor lP = select.collectLocalPredictor(data.getSchema());
		Object[] out = lP.predict("a", 1L, 1, 2.0, true);
		Assert.assertEquals(new Object[] {"a", 1L, 1, 2.0, true, "a", 3.0}, out);
	}

	@Test
	public void testPipelineLocalPredictor() throws Exception {
		BatchOperator <?> data = data();
		Pipeline pipeline = new Pipeline()
			.add(
				new Select()
					.setClause("*, f_string as fas, f_double + 1 as fd")
			)
			.add(
				new Select()
					.setClause("*, fas as fas2, fd as fd1")
			);
		PipelineModel model = pipeline.fit(data);
		LocalPredictor lP = model.collectLocalPredictor(data.getSchema());
		Object[] out = lP.predict("a", 1L, 1, 2.0, true);
		Assert.assertEquals(new Object[] {"a", 1L, 1, 2.0, true, "a", 3.0, "a", 3.0}, out);
	}

	private BatchOperator <?> data() {
		List <Row> testArray = Arrays.asList(
			Row.of("a", 1L, 1, 2.0, true),
			Row.of(null, 2L, 2, -3.0, true),
			Row.of("c", null, null, 2.0, false),
			Row.of("a", 0L, 0, null, null)
		);

		String[] colNames = new String[] {"f_string", "f_long", "f_lint", "f_double", "f_boolean"};

		return new MemSourceBatchOp(testArray, colNames);
	}

	private StreamOperator <?> datas() {
		List <Row> testArray = Arrays.asList(
			Row.of("a", 1L, 1, 2.0, true),
			Row.of(null, 2L, 2, -3.0, true),
			Row.of("c", null, null, 2.0, false),
			Row.of("a", 0L, 0, null, null)
		);

		String[] colNames = new String[] {"f_string", "f_long", "f_lint", "f_double", "f_boolean"};

		return new MemSourceStreamOp(testArray, colNames);
	}

}