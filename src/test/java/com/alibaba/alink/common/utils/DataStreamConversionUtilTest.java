package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit Test for RowTypeDataStream.
 */
public class DataStreamConversionUtilTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment env = MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment();

		DataStream <Row> input = env.fromElements(Row.of("a"));

		Table table1 = DataStreamConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, input, new String[] {"word"});
		Assert.assertEquals(
			new TableSchema(new String[] {"word"}, new TypeInformation[] {TypeInformation.of(String.class)}),
			table1.getSchema()
		);

		input = input.map(new GenericTypeMap());

		Table table2 = DataStreamConversionUtil.toTable(
			MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,
			input,
			new String[] {"word"},
			new TypeInformation[] {TypeInformation.of(Integer.class)}
		);

		Assert.assertEquals(
			new TableSchema(new String[] {"word"}, new TypeInformation[] {TypeInformation.of(Integer.class)}),
			table2.getSchema()
		);

		Table tableFromDataStreamWithTableSchema = DataStreamConversionUtil.toTable(
			MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,
			input,
			new TableSchema(
				new String[]{"word"},
				new TypeInformation[]{TypeInformation.of(Integer.class)}
			)
		);

		Assert.assertEquals(
			new TableSchema(new String[] {"word"}, new TypeInformation[] {TypeInformation.of(Integer.class)}),
			tableFromDataStreamWithTableSchema.getSchema()
		);

		thrown.expect(ValidationException.class);
		DataStreamConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, input, new String[] {"f0"});

		DataStream <Row> output = DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,table1);

		output.print();

		env.execute();
	}

	private static class GenericTypeMap implements MapFunction <Row, Row> {

		@Override
		public Row map(Row value) throws Exception {
			return value;
		}
	}
}
