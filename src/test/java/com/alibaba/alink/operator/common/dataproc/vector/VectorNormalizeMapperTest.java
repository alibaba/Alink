package com.alibaba.alink.operator.common.dataproc.vector;

import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.dataproc.vector.VectorNormalizeParams;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for VectorNormalizeMapper.
 */
public class VectorNormalizeMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorNormalizeParams.SELECTED_COL, "vec")
			.set(VectorNormalizeParams.P, 2.0);

		VectorNormalizeMapper mapper = new VectorNormalizeMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}))).getField(0),
				new DenseVector(new double[]{0.6, 0.8}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {VectorTypes.VECTOR}));
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorNormalizeParams.SELECTED_COL, "vec")
			.set(VectorNormalizeParams.OUTPUT_COL, "res")
			.set(VectorNormalizeParams.P, 2.0);

		VectorNormalizeMapper mapper = new VectorNormalizeMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}))).getField(1),
				new DenseVector(new double[]{0.6, 0.8}));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[] {"vec", "res"},
					new TypeInformation <?>[] {Types.STRING, VectorTypes.VECTOR})
		);
	}

	@Test
	public void test3() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorNormalizeParams.SELECTED_COL, "vec")
			.set(VectorNormalizeParams.OUTPUT_COL, "res")
			.set(VectorNormalizeParams.RESERVED_COLS, new String[] {})
			.set(VectorNormalizeParams.P, 2.0);

		VectorNormalizeMapper mapper = new VectorNormalizeMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}))).getField(0),
				new DenseVector(new double[]{0.6, 0.8}));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[] {"res"}, new TypeInformation <?>[] {VectorTypes.VECTOR})
		);
	}

	@Test
	public void test4() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorNormalizeParams.SELECTED_COL, "vec")
			.set(VectorNormalizeParams.OUTPUT_COL, "res")
			.set(VectorNormalizeParams.P, 1.0);

		VectorNormalizeMapper mapper = new VectorNormalizeMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{2.0, 3.0}))).getField(1),
				new DenseVector(new double[]{0.4, 0.6}));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[] {"vec", "res"},
					new TypeInformation <?>[] {Types.STRING, VectorTypes.VECTOR})
		);
	}

}