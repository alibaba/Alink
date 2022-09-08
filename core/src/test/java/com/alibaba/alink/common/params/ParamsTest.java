package com.alibaba.alink.common.params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class ParamsTest extends AlinkTestBase {

	@Test
	public void testOpSetString() {
		TestBatchOp testBatchOp = new TestBatchOp()
			.setEnumType("aaa")
			.setAppendType("DENSE");

		Assert.assertEquals(CalcType.aAA, testBatchOp.getEnumType());

	}

	@Test
	public void testOpSetEnum() {
		TestBatchOp testBatchOp = new TestBatchOp()
			.setEnumType(CalcType.aAA)
			.setAppendType("DENSE");

		Assert.assertEquals(CalcType.aAA, testBatchOp.getEnumType());
	}

	@Test
	public void testDefaultValue() {
		TestBatchOp testBatchOp = new TestBatchOp()
			.setAppendType("DENSE");

		Assert.assertEquals(CalcType.aAA, testBatchOp.getEnumType());
	}

	@Test
	public void testFromJson() {
		String json = "{\"enumType\":\"\\\"aAA\\\"\",\"appendType\":\"\\\"DENSE\\\"\"}";
		Params params = Params.fromJson(json);
		Assert.assertEquals(CalcType.aAA, params.get(HasEnumType.ENUM_TYPE));

		TestBatchOp op = new TestBatchOp(params);
		Assert.assertEquals(CalcType.aAA, op.getEnumType());
		Assert.assertEquals(CalcType.aAA, op.get(HasEnumType.ENUM_TYPE));
	}

	@Test
	public void testParamsSetEnum() {
		Params params = new Params()
			.set(HasEnumType.ENUM_TYPE, CalcType.aAA)
			.set(HasAppendType.APPEND_TYPE, "Dense");
		TestBatchOp testBatchOp = new TestBatchOp(params);
		Assert.assertEquals(CalcType.aAA, testBatchOp.getEnumType());
	}

	@Test
	public void testParamsSetString() {
		Params params = new Params()
			.set("enumType", "aaa")
			.set("appendType", "DENSE");
		TestBatchOp testBatchOp = new TestBatchOp(params);
		Assert.assertEquals(CalcType.aAA, testBatchOp.getEnumType());
	}

	@Test
	public void testException() {
		Params params = new Params()
			.set("enumType", "cbbb")
			.set("appendType", "DENSE");
		try {
			TestBatchOp testBatchOp = new TestBatchOp(params);
			testBatchOp.linkFrom(new TestBatchOp());
		} catch (Exception ex) {
			Assert.assertEquals(
				"In TestBatchOp,cbbb is not member of enumType.It maybe aAA,BBB.",
				ex.getMessage());
		}
	}

	@Test
	public void testColorFromJson() {
		String json = "{\"enumTypeColor\":\"\\\"green\\\"\",\"appendType\":\"\\\"DENSE\\\"\"}";
		Params params = Params.fromJson(json);

		TestBatchOpColor testBatchOp = new TestBatchOpColor(params);
		Assert.assertEquals(Color.GREEN, testBatchOp.getEnumTypeColor());

		System.out.println(testBatchOp.getParams().toJson());
	}

	@Test
	public void testColor() {
		Params params = new Params()
			.set("enumTypeColor", "green")
			.set("appendType", "DENSE");

		Assert.assertEquals(Color.GREEN, params.get(HasEnumTypeColor.ENUM_TYPE_COLOR));

		TestBatchOpColor testBatchOp = new TestBatchOpColor(params);
		Assert.assertEquals(Color.GREEN, testBatchOp.getEnumTypeColor());
		Assert.assertEquals(Color.GREEN, testBatchOp.get(HasEnumTypeColor.ENUM_TYPE_COLOR));
	}

	@Test
	public void testColorAlias() {
		Params params = new Params()
			.set("enumType2", "green")
			.set("appendType", "DENSE");

		Assert.assertEquals(Color.GREEN, params.get(HasEnumTypeColor.ENUM_TYPE_COLOR));

		TestBatchOpColor testBatchOp = new TestBatchOpColor(params);
		Assert.assertEquals(Color.GREEN, testBatchOp.getEnumTypeColor());
		Assert.assertEquals(Color.GREEN, testBatchOp.get(HasEnumTypeColor.ENUM_TYPE_COLOR));
	}

	@Test
	public void testColor2() {
		Params params = new Params()
			.set("enumTypeColor", "green")
			.set("appendType", "DENSE");

		Assert.assertEquals(Color.GREEN, params.get(HasEnumTypeColor.ENUM_TYPE_COLOR));

		TestBatchOpColor testBatchOp = new TestBatchOpColor(params);
		Assert.assertEquals(Color.GREEN, testBatchOp.getEnumTypeColor());
		Assert.assertEquals(Color.GREEN, testBatchOp.get(HasEnumTypeColor.ENUM_TYPE_COLOR));
	}

	@Test
	public void testContain() {
		Params params = new Params()
			.set("enumTypeColor", "green")
			.set("appendType", "DENSE");

		Assert.assertTrue(params.contains("enumTypeColor"));
		Assert.assertFalse(params.contains("enumTypeColor2"));
	}

	@Test
	public void testContain2() {
		TestBatchOp testBatchOp = new TestBatchOp()
			.setEnumType("aaa")
			.setAppendType("DENSE");

		Assert.assertTrue(testBatchOp.getParams().contains("enumType"));
		Assert.assertFalse(testBatchOp.getParams().contains("enumType2"));
	}

	@Test
	public void testContain3() {
		Params params = new Params()
			.set(HasEnumType.ENUM_TYPE, CalcType.aAA)
			.set(HasAppendType.APPEND_TYPE, "Dense");
		TestBatchOp testBatchOp = new TestBatchOp(params);

		Assert.assertTrue(testBatchOp.getParams().contains("enumType"));
		Assert.assertFalse(testBatchOp.getParams().contains("enumType2"));
	}

	@Test
	public void testContain4() {
		Params params = new Params()
			.set(HasEnumType.ENUM_TYPE, CalcType.aAA)
			.set(HasAppendType.APPEND_TYPE, "Dense");

		CalcType type = params.get(HasEnumType.ENUM_TYPE);
		System.out.println(type);
	}

	public static class TestBatchOpColor
		extends BatchOperator <TestBatchOp>
		implements HasEnumTypeColor <TestBatchOp>,
		HasAppendType <TestBatchOp> {

		private static final long serialVersionUID = -7535880286721320823L;

		public TestBatchOpColor() {
			this(new Params());
		}

		public TestBatchOpColor(Params params) {
			super(params);
		}

		@Override
		public TestBatchOp linkFrom(BatchOperator <?>... inputs) {
			Color calcType = getEnumTypeColor();
			String appendType = getAppendType();

			System.out.println("color: " + calcType);
			System.out.println("appendType: " + appendType);

			return null;
		}
	}

	public static class TestBatchOp
		extends BatchOperator <TestBatchOp>
		implements HasEnumType <TestBatchOp>,
		HasAppendType <TestBatchOp> {

		private static final long serialVersionUID = -5426600464347574111L;

		public TestBatchOp() {
			this(new Params());
		}

		public TestBatchOp(Params params) {
			super(params);
		}

		@Override
		public TestBatchOp linkFrom(BatchOperator <?>... inputs) {
			CalcType calcType = getEnumType();
			String appendType = getAppendType();

			System.out.println("calcType: " + calcType);
			System.out.println("appendType: " + appendType);

			return null;
		}
	}

	public static class Test2BatchOp
		extends BatchOperator <Test2BatchOp>
		implements HasEnumType <Test2BatchOp>,
		HasAppendType <Test2BatchOp> {

		private static final long serialVersionUID = 7415712026648258276L;

		public Test2BatchOp() {
			this(new Params());
		}

		public Test2BatchOp(Params params) {
			super(params);
		}

		@Override
		public Test2BatchOp linkFrom(BatchOperator <?>... inputs) {
			CalcType calcType = getEnumType();
			String appendType = getAppendType();

			System.out.println("calcType: " + calcType);
			System.out.println("appendType: " + appendType);

			return null;
		}
	}

	public interface HasEnumType<T> extends WithParams <T> {

		ParamInfo <CalcType> ENUM_TYPE = ParamInfoFactory
			.createParamInfo("enumType", CalcType.class)
			.setDescription("test")
			.setHasDefaultValue(CalcType.aAA)
			.setAlias(new String[] {"aaaType"})
			.build();

		default CalcType getEnumType() {
			return get(ENUM_TYPE);
		}

		default T setEnumType(String value) {
			return set(ENUM_TYPE, ParamUtil.searchEnum(ENUM_TYPE, value));
		}

		default T setEnumType(CalcType value) {
			return set(ENUM_TYPE, value);
		}
	}

	public interface HasEnumTypeColor<T> extends WithParams <T> {

		ParamInfo <Color> ENUM_TYPE_COLOR = ParamInfoFactory
			.createParamInfo("enumTypeColor", Color.class)
			.setDescription("test")
			//            .setHasDefaultValue(Color.GREEN)
			.setAlias(new String[] {"enumType2"})
			.build();

		default Color getEnumTypeColor() {
			return get(ENUM_TYPE_COLOR);
		}

		default T setEnumTypeColor(String value) {
			return set(ENUM_TYPE_COLOR, ParamUtil.searchEnum(ENUM_TYPE_COLOR, value));
		}

		default T setEnumTypeColor(Color value) {
			return set(ENUM_TYPE_COLOR, value);
		}
	}

	public enum CalcType {
		aAA,
		BBB
	}

	public enum Color {
		RED("红色", 1),
		GREEN("绿色", 2),
		WHITE("白色", 3),
		YELLOW("黄色", 4);

		private String name;
		private int index;

		private Color(String name, int index) {
			this.name = name;
			this.index = index;
		}

		@Override
		public String toString() {
			return this.index + "-" + this.name;
		}
	}

	public static class AppendType {
		public static final String DENSE = "DENSE";
		public static final String UNIQUE = "UNIQUE";
	}

	public interface HasAppendType<T> extends WithParams <T> {
		ParamInfo <String> APPEND_TYPE = ParamInfoFactory
			.createParamInfo("appendType", String.class)
			.setDescription("append type. DENSE or UNIQUE")
			.setHasDefaultValue(AppendType.DENSE)
			.setAlias(new String[] {"AppendType"})
			.build();

		default String getAppendType() {
			return get(APPEND_TYPE);
		}

		default T setAppendType(String value) {
			return set(APPEND_TYPE, value);
		}
	}
}
