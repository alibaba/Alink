package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.feature.BucketizerParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit test for BucketizerMapper.
 */

public class BucketizerMapperTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static double[][] cutsArray = new double[][] {{0.5, 0.0, 0.5}, {-0.3, 0.0, 0.3, 0.4}};
	private static double[] cuts = new double[] {-999.9, -0.5, 0.0, 0.5, 999.9};

	@Test
	public void testOneFeature() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"feature"}, new TypeInformation <?>[] {Types.LONG});

		Params params = new Params()
			.set(BucketizerParams.SELECTED_COLS, new String[] {"feature"})
			.set(BucketizerParams.CUTS_ARRAY, new double[][] {cuts});

		BucketizerMapper mapper = new BucketizerMapper(schema, params);
		mapper.open();
		assertEquals(mapper.map(Row.of(-999.9)).getField(0), 0L);
		assertEquals(mapper.map(Row.of(-0.5)).getField(0), 1L);
		assertEquals(mapper.map(Row.of(-0.3)).getField(0), 2L);
		assertEquals(mapper.map(Row.of(0.0)).getField(0), 2L);
		assertEquals(mapper.map(Row.of(0.5)).getField(0), 3L);
		assertEquals(mapper.map(Row.of(999.9)).getField(0), 4L);
		assertEquals(mapper.getOutputSchema(), schema);
	}

	@Test
	public void testMultiFeatures() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"featureA", "featureB"},
			new TypeInformation <?>[] {Types.LONG, Types.LONG});

		Params params = new Params()
			.set(BucketizerParams.SELECTED_COLS, new String[] {"featureA", "featureB"})
			.set(BucketizerParams.CUTS_ARRAY, cutsArray);

		BucketizerMapper mapper = new BucketizerMapper(schema, params);
		mapper.open();
		assertEquals(mapper.map(Row.of(-999.9, -999.9)).getField(1), 0L);
		assertEquals(mapper.map(Row.of(-0.5, -0.2)).getField(1), 1L);
		assertEquals(mapper.map(Row.of(-0.3, -0.6)).getField(1), 0L);
		assertEquals(mapper.map(Row.of(0.0, 0.0)).getField(1), 1L);
		assertEquals(mapper.map(Row.of(0.5, 0.4)).getField(1), 3L);
		assertEquals(mapper.map(Row.of(0.5, null)).getField(1), 5L);
		assertEquals(mapper.getOutputSchema(), schema);
	}

	@Test
	public void testSkipAndError() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"featureA", "featureB"},
			new TypeInformation <?>[] {Types.LONG, Types.LONG});

		Params params = new Params()
			.set(BucketizerParams.SELECTED_COLS, new String[] {"featureA", "featureB"})
			.set(BucketizerParams.CUTS_ARRAY, cutsArray)
			.set(BucketizerParams.HANDLE_INVALID, HasHandleInvalid.HandleInvalid.SKIP);

		BucketizerMapper mapper = new BucketizerMapper(schema, params);
		mapper.open();
		assertNull(mapper.map(Row.of(0.5, null)).getField(1));
		assertEquals(mapper.getOutputSchema(), schema);

		thrown.expect(RuntimeException.class);
		params.set(BucketizerParams.HANDLE_INVALID, HasHandleInvalid.HandleInvalid.ERROR);
		mapper = new BucketizerMapper(schema, params);
		mapper.open();
		mapper.map(Row.of(0.5, null));
	}

	@Test
	public void testCutsArrayStr() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"featureA", "featureB"},
			new TypeInformation <?>[] {Types.LONG, Types.LONG});

		Params params = new Params()
			.set(BucketizerParams.SELECTED_COLS, new String[] {"featureA", "featureB"})
			.set(BucketizerParams.CUTS_ARRAY_STR, new String[] {"-0.5 : 0.0:0.5", "-0.3: 0.0: 0.3: 0.4"});

		BucketizerMapper mapper = new BucketizerMapper(schema, params);
		mapper.open();
		assertEquals(mapper.map(Row.of(-999.9, -999.9)).getField(1), 0L);
		assertEquals(mapper.map(Row.of(-0.5, -0.2)).getField(1), 1L);
	}
}
