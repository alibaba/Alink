package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.exceptions.AkColumnNotFoundException;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit test for TableUtil.
 */

public class TableUtilTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();
	private String[] colNames = new String[] {"f0", "f1", "f2"};
	private TableSchema tableSchema = new TableSchema(colNames,
		new TypeInformation[] {Types.INT, Types.LONG, Types.STRING});

	@Test
	public void testFindIndexFromName() {
		String[] colNames = new String[] {"f0", "f1", "F2"};
		Assert.assertEquals(TableUtil.findColIndex(colNames, "f0"), 0);
		Assert.assertEquals(TableUtil.findColIndex(colNames, "F1"), 1);
		Assert.assertEquals(TableUtil.findColIndex(colNames, "f3"), -1);
		Assert.assertEquals(TableUtil.findColIndex(tableSchema, "f0"), 0);

		Assert.assertArrayEquals(TableUtil.findColIndices(colNames, new String[] {"f1", "F2"}), new int[] {1, 2});
		Assert.assertArrayEquals(TableUtil.findColIndices(tableSchema, new String[] {"f1", "F2"}), new int[] {1, 2});
		Assert.assertArrayEquals(TableUtil.findColIndices(tableSchema, new String[] {"f3", "F2"}), new int[] {-1, 2});
		Assert.assertArrayEquals(TableUtil.findColIndices(colNames, null), new int[] {0, 1, 2});
	}

	@Test
	public void testFindTypeFromTable() {
		Assert.assertArrayEquals(TableUtil.findColTypes(tableSchema, new String[] {"f0", "f1"}),
			new TypeInformation[] {TypeInformation.of(Integer.class), Types.LONG});
		Assert.assertArrayEquals(TableUtil.findColTypes(tableSchema, new String[] {"f1", "f3"}),
			new TypeInformation[] {Types.LONG, null});
		Assert.assertArrayEquals(TableUtil.findColTypes(tableSchema, null),
			new TypeInformation[] {Types.INT, Types.LONG, Types.STRING});

		Assert.assertEquals(TableUtil.findColType(tableSchema, "f0"), TypeInformation.of(Integer.class));
		Assert.assertNull(TableUtil.findColType(tableSchema, "f3"));
	}

	@Test
	public void isNumberIsStringTest() {
		Assert.assertTrue(TableUtil.isSupportedNumericType(Types.INT));
		Assert.assertTrue(TableUtil.isSupportedNumericType(Types.DOUBLE));
		Assert.assertTrue(TableUtil.isSupportedNumericType(Types.LONG));
		Assert.assertTrue(TableUtil.isSupportedNumericType(Types.BYTE));
		Assert.assertTrue(TableUtil.isSupportedNumericType(Types.FLOAT));
		Assert.assertTrue(TableUtil.isSupportedNumericType(Types.SHORT));
		Assert.assertFalse(TableUtil.isSupportedNumericType(Types.STRING));
		Assert.assertTrue(TableUtil.isString(Types.STRING));
	}

	@Test
	public void assertColExistOrTypeTest() {
		String[] colNames = new String[] {"f0", "f1", "f2"};
		TableUtil.assertSelectedColExist(colNames, null);
		TableUtil.assertSelectedColExist(colNames, "f0");
		TableUtil.assertSelectedColExist(colNames, "f0", "f1");

		TableUtil.assertNumericalCols(tableSchema, null);
		TableUtil.assertNumericalCols(tableSchema, "f1");
		TableUtil.assertNumericalCols(tableSchema, "f0", "f1");

		TableUtil.assertStringCols(tableSchema, null);
		TableUtil.assertStringCols(tableSchema, "f2");
	}

	@Test
	public void assertColExistOrTypeExceptionTest() {
		thrown.expect(AkColumnNotFoundException.class);
		thrown.expectMessage("Can not find column: f3");
		TableUtil.assertSelectedColExist(colNames, "f3");

		thrown.expect(AkColumnNotFoundException.class);
		thrown.expectMessage("Can not find column: f3");
		TableUtil.assertSelectedColExist(colNames, "f0", "f3");

		thrown.expect(AkIllegalOperatorParameterException.class);
		thrown.expectMessage("col type must be number f2");
		TableUtil.assertNumericalCols(tableSchema, "f2");

		thrown.expect(AkIllegalOperatorParameterException.class);
		thrown.expectMessage("col type must be number f2");
		TableUtil.assertNumericalCols(tableSchema, "f2", "f0");

		thrown.expect(AkIllegalOperatorParameterException.class);
		thrown.expectMessage("col type must be string f2");
		TableUtil.assertStringCols(tableSchema, "f2");

		thrown.expect(AkIllegalOperatorParameterException.class);
		thrown.expectMessage("col type must be string f0");
		TableUtil.assertStringCols(tableSchema, "f0", "f3");
	}

	@Test
	public void getNumericColsTest() {
		TableSchema tableSchema = new TableSchema(new String[] {"f0", "f1", "F2", "f3"},
			new TypeInformation[] {Types.INT, Types.LONG, Types.STRING, Types.BOOLEAN});

		Assert.assertArrayEquals(TableUtil.getNumericCols(tableSchema), new String[] {"f0", "f1"});
		Assert.assertArrayEquals(TableUtil.getNumericCols(tableSchema, new String[] {"f0"}), new String[] {"f1"});
		Assert.assertArrayEquals(TableUtil.getNumericCols(tableSchema, new String[] {"f2"}), new String[] {"f0",
			"f1"});
	}

	@Test
	public void getCategoricalColsTest() {
		TableSchema tableSchema = new TableSchema(new String[] {"f0", "f1", "f2", "f3"},
			new TypeInformation[] {Types.INT, Types.LONG, Types.STRING, Types.BOOLEAN});

		Assert.assertArrayEquals(TableUtil.getCategoricalCols(tableSchema, tableSchema.getFieldNames(), null),
			new String[] {"f2", "f3"});
		Assert.assertArrayEquals(
			TableUtil.getCategoricalCols(tableSchema, new String[] {"f2", "f1", "f0", "f3"}, new String[] {"f0"}),
			new String[] {"f2", "f0", "f3"});

		thrown.expect(AkIllegalArgumentException.class);
		Assert.assertArrayEquals(
			TableUtil.getCategoricalCols(tableSchema, new String[] {"f3", "f0"}, new String[] {"f2"}),
			new String[] {"f3", "f2"});
	}

	@Test
	public void getOptionalFeatureColsTest() {
		TableSchema tableSchema = new TableSchema(new String[] {"f0", "f1", "f2", "f3"},
			new TypeInformation[] {Types.INT, Types.LONG, Types.STRING, Types.BOOLEAN});

		Assert.assertArrayEquals(
			TableUtil.getOptionalFeatureCols(
				tableSchema,
				new Params()
					.set(HasLabelCol.LABEL_COL, "f0")
					.set(HasWeightColDefaultAsNull.WEIGHT_COL, "f3")
			),
			new String[] {"f1", "f2"}
		);
	}

	@Test
	public void findColIndexWithAssertAndHintTest() {
		thrown.expect(AkColumnNotFoundException.class);
		thrown.expectMessage("Can not find column: features, do you mean: feature ?");

		String[] colNames = new String[] {"id", "text", "vector", "feature"};
		TableUtil.findColIndexWithAssertAndHint(colNames, "features");
	}

	@Test
	public void testStringTypeToString() {
		Assert.assertEquals("f0 INT,f1 BIGINT,f2 STRING", TableUtil.schema2SchemaStr(tableSchema));
	}
}
