package com.alibaba.alink.operator.common.dataproc.vector;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.dataproc.vector.VectorSliceParams;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for VectorSliceMapper.
 */
public class VectorSliceMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorSliceParams.SELECTED_COL, "vec")
			.set(VectorSliceParams.INDICES, new int[] {0, 1});

		VectorSliceMapper mapper = new VectorSliceMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0, 3.0}))).getField(0),
				new DenseVector(new double[]{3.0, 4.0}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {VectorTypes.VECTOR}));
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorSliceParams.SELECTED_COL, "vec")
			.set(VectorSliceParams.OUTPUT_COL, "res")
			.set(VectorSliceParams.INDICES, new int[] {0, 1});

		VectorSliceMapper mapper = new VectorSliceMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0, 3.0}))).getField(1),
				new DenseVector(new double[]{3.0, 4.0}));
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
			.set(VectorSliceParams.SELECTED_COL, "vec")
			.set(VectorSliceParams.OUTPUT_COL, "res")
			.set(VectorSliceParams.RESERVED_COLS, new String[] {})
			.set(VectorSliceParams.INDICES, new int[] {0, 2, 4});

		VectorSliceMapper mapper = new VectorSliceMapper(schema, params);

		assertEquals(mapper.map(Row.of(new SparseVector(5, new int[]{0, 2, 4}, new double[]{3.0, 4.0, 3.0})))
				.getField(0), new SparseVector(3, new int[]{0, 1, 2}, new double[]{3.0, 4.0, 3.0}));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[] {"res"}, new TypeInformation <?>[] {VectorTypes.VECTOR})
		);
	}
}