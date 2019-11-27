package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;

/**
 * Unit test for RowTypeDataSet.
 */
public class DataSetConversionUtilTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testForceType() {
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();

		DataSet<Row> input = env.fromElements(Row.of("s1")).map(new GenericTypeMap());
		Table table2 = DataSetConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,
				input,
				new String[] {"word"},
				new TypeInformation[] {TypeInformation.of(Integer.class)}
		);
		Assert.assertEquals(
				new TableSchema(new String[] {"word"}, new TypeInformation[] {TypeInformation.of(Integer.class)}),
				table2.getSchema()
		);

	}

	@Test
	public void testForceTypeWithTableSchema() {
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();

		DataSet<Row> input = env.fromElements(Row.of("s1")).map(new GenericTypeMap());
		Table table2 = DataSetConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,
			input,
			new TableSchema(
				new String[]{"word"},
				new TypeInformation[]{TypeInformation.of(Integer.class)}
			)
		);
		Assert.assertEquals(
			new TableSchema(new String[] {"word"}, new TypeInformation[] {TypeInformation.of(Integer.class)}),
			table2.getSchema()
		);

	}

	@Test
	public void testExceptionWithoutTypeSchema() {
		thrown.expect(ValidationException.class);
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();
		DataSet<Row>  input = env.fromElements(Row.of("s1")).map(new GenericTypeMap());
		DataSetConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, input, new String[] {"f0"});
	}

	@Test
	public void testBasicConvert() throws Exception {
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();

		DataSet <Row> input = env.fromElements(Row.of("a"));

		Table table1 = DataSetConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, input, new String[] {"word"});
		Assert.assertEquals(
			new TableSchema(new String[] {"word"}, new TypeInformation[] {TypeInformation.of(String.class)}),
			table1.getSchema()
		);
		List <Row> list = DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, table1).collect();
		Assert.assertEquals(Collections.singletonList(Row.of("a")), list);
	}

	private static class GenericTypeMap implements MapFunction <Row, Row> {

		@Override
		public Row map(Row value) throws Exception {
			return value;
		}
	}
}
