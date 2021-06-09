package com.alibaba.alink.common.params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.validators.ArrayWithMaxLengthValidator;
import com.alibaba.alink.params.validators.MaxValidator;
import com.alibaba.alink.params.validators.MinValidator;
import com.alibaba.alink.params.validators.RangeValidator;
import com.alibaba.alink.params.validators.Validator;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class ValidatorTest extends AlinkTestBase {

	public interface HasNum<T> extends WithParams <T> {

		ParamInfo <Double> NUM = ParamInfoFactory
			.createParamInfo("num", Double.class)
			.setDescription("test")
			.setValidator(new RangeValidator <>(-1.0, 3.0).setLeftInclusive(true).setRightInclusive(true)
				.setNullValid(false))
			.build();

		default double getEnumType() {
			return get(NUM);
		}

		default T setEnumType(double value) {
			return set(NUM, value);
		}
	}

	@Test
	public void testSet() {
		try {
			Params params = new Params()
				.set(HasNum.NUM, 4.0);
		} catch (Exception ex) {
			Assert.assertEquals("4.0 of num is not validate. value in [-1.0, 3.0]", ex.getMessage());
		}
	}

	@Test
	public void testGet() {
		try {
			Params params = new Params()
				.set("num", 4.0);
			double result = params.get(HasNum.NUM);
		} catch (Exception ex) {
			Assert.assertEquals("4.0 of num is not validate. value in [-1.0, 3.0]", ex.getMessage());
		}
	}

	public interface HasNumDefaultNull<T> extends WithParams <T> {

		ParamInfo <Integer> NUM = ParamInfoFactory
			.createParamInfo("num", Integer.class)
			.setDescription("test")
			.setHasDefaultValue(null)
			.setValidator(new RangeValidator <>(-1, 3).setNullValid(true))
			.build();

		default Integer getEnumType() {
			return get(NUM);
		}

		default T setEnumType(Integer value) {
			return set(NUM, value);
		}
	}

	@Test
	public void testDefaultValueNull() {
		Params params = new Params();
		Assert.assertNull(params.get(HasNumDefaultNull.NUM));
	}

	public interface HasNumMin<T> extends WithParams <T> {

		ParamInfo <Double> NUM = ParamInfoFactory
			.createParamInfo("num", Double.class)
			.setDescription("test")
			.setValidator(new MinValidator <>(3.0).setLeftInclusive(true).setNullValid(true))
			.build();

		default double getEnumType() {
			return get(NUM);
		}

		default T setEnumType(double value) {
			return set(NUM, value);
		}
	}

	@Test
	public void testMinValidator() {
		Params params = new Params()
			.set("num", 4.0);
		Assert.assertEquals(4.0, params.get(HasNumMin.NUM), 10e-8);
		Assert.assertFalse(new MinValidator <>(3.0).setNullValid(false).validate(null));

		try {
			params.set("num", 2.0);
			params.get(HasNumMin.NUM);
		} catch (Exception ex) {
			Assert.assertEquals("2.0 of num is not validate. value in [3.0, +inf)", ex.getMessage());
		}

	}

	public interface HasNumMax<T> extends WithParams <T> {

		ParamInfo <Double> NUM = ParamInfoFactory
			.createParamInfo("num", Double.class)
			.setDescription("test")
			.setValidator(new MaxValidator <>(3.0).setRightInclusive(true).setNullValid(true))
			.build();

		default double getEnumType() {
			return get(NUM);
		}

		default T setEnumType(double value) {
			return set(NUM, value);
		}
	}

	@Test
	public void testMaxValidator() {
		Params params = new Params()
			.set("num", 2.0);
		Assert.assertEquals(2.0, params.get(HasNumMax.NUM), 10e-8);
		Assert.assertFalse(new MaxValidator <>(3.0).setNullValid(false).validate(null));
		try {
			params.set("num", 4.0);
			params.get(HasNumMax.NUM);
		} catch (Exception ex) {
			Assert.assertEquals("4.0 of num is not validate. value in (-inf, 3.0]", ex.getMessage());
		}
	}

	public interface HasNumRange<T> extends WithParams <T> {

		ParamInfo <Double> NUM = ParamInfoFactory
			.createParamInfo("num", Double.class)
			.setDescription("test")
			.setValidator(new RangeValidator <>(3.0, 5.0))
			.build();

		default double getEnumType() {
			return get(NUM);
		}

		default T setEnumType(double value) {
			return set(NUM, value);
		}
	}

	@Test
	public void testRangeValidator() {
		Params params = new Params()
			.set("num", 4.0);
		Assert.assertEquals(4.0, params.get(HasNumRange.NUM), 10e-8);
		Assert.assertFalse(new RangeValidator <>(3.0, 5.0).setNullValid(false).validate(null));

		try {
			params.set("num", 2.0);
			params.get(HasNumRange.NUM);
		} catch (Exception ex) {
			Assert.assertEquals("2.0 of num is not validate. value in [3.0, 5.0]", ex.getMessage());
		}

		try {
			params.set("num", 6.0);
			params.get(HasNumRange.NUM);
		} catch (Exception ex) {
			Assert.assertEquals("6.0 of num is not validate. value in [3.0, 5.0]", ex.getMessage());
		}

		RangeValidator validator = new RangeValidator <>(3.0, 5.0).setLeftInclusive(false);
		validator.validate(4.0);

		validator.setLeftInclusive(false).setRightInclusive(false);
		validator.validate(4.0);
	}

	public interface HasArray<T> extends WithParams <T> {

		ParamInfo <String[]> NUM = ParamInfoFactory
			.createParamInfo("num", String[].class)
			.setDescription("test")
			.setValidator(new ArrayWithMaxLengthValidator(3).setNullValid(false))
			.build();

		default String[] getEnumType() {
			return get(NUM);
		}

		default T setEnumType(String[] value) {
			return set(NUM, value);
		}
	}

	@Test
	public void testArrayValidator() {
		Params params = new Params()
			.set("num", new String[] {"aa", "bb"});
		Assert.assertArrayEquals(new String[] {"aa", "bb"}, params.get(HasArray.NUM));
		Assert.assertFalse(new ArrayWithMaxLengthValidator(3).validate(null));

		try {
			params.set("num", new String[] {"aa", "bb", "cc", "dd"});
			params.get(HasArray.NUM);
		} catch (Exception ex) {
			Assert.assertTrue(ex.getMessage().contains("of num is not validate. lengthOfArray <= 3"));
		}

		ArrayWithMaxLengthValidator validator = new ArrayWithMaxLengthValidator(3);
		validator.setParamName("test");
		validator.validateThrows(new String[] {});
		validator.validate(new String[] {});

		Validator validator1 = new Validator();
		validator1.validate(1);
	}

}
