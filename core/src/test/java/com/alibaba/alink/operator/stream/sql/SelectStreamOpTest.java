package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SelectStreamOpTest extends AlinkTestBase {

	@Test
	public void testSimpleSelect() throws Exception {
		StreamOperator <?> select = data().link(
			new SelectStreamOp()
				.setClause("f_double, f_long")
		);

		Assert.assertArrayEquals(new String[] {"f_double", "f_long"}, select.getColNames());
		Assert.assertArrayEquals(new TypeInformation <?>[] {Types.DOUBLE, Types.LONG}, select.getColTypes());

		CollectSinkStreamOp op = select.link(new CollectSinkStreamOp());

		StreamOperator.execute();

		Assert.assertEquals(4, op.getAndRemoveValues().size());

	}

	@Test
	public void testSimpleSelect2() throws Exception {
		StreamOperator <?> select = data().link(
			new SelectStreamOp()
				.setClause("*")
		);

		Assert.assertArrayEquals(new String[] {"f_string", "f_long", "f_lint", "f_double", "f_boolean"},
			select.getColNames());
		Assert.assertArrayEquals(
			new TypeInformation <?>[] {Types.STRING, Types.LONG, Types.INT, Types.DOUBLE, Types.BOOLEAN},
			select.getColTypes());
	}

	@Test
	public void testSimpleSelect3() throws Exception {
		StreamOperator <?> select = data().link(
			new SelectStreamOp()
				.setClause("f_double, `f_l.*`")
		);

		Assert.assertArrayEquals(new String[] {"f_double", "f_long", "f_lint"},
			select.getColNames());
		Assert.assertArrayEquals(
			new TypeInformation <?>[] {Types.DOUBLE, Types.LONG, Types.INT},
			select.getColTypes());

		StreamOperator.execute();
	}

	@Test
	public void testSelect4() throws Exception {
		data().select("f_double, `f_l.*`").print();
		StreamOperator.execute();
	}


	@Test
	public void testSelect2() throws Exception {
		data().link(
			new SelectStreamOp()
				.setClause("f_double, `f_l.*`, f_double + 1 as f_double_1")
		).print();

		StreamOperator.execute();
	}

	@Test
	public void testSelect3() throws Exception {
		data().select("f_double, `f_l.*`, f_double + 1 as f_double_1").print();
		StreamOperator.execute();
	}

	private StreamOperator <?> data() {
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
