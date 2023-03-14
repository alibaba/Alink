package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.params.dataproc.vector.VectorSliceParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for VectorSliceMapper.
 */

public class VectorSliceMapperTest extends AlinkTestBase {
	@Test
	public void testDense() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorSliceParams.SELECTED_COL, "vec")
			.set(VectorSliceParams.INDICES, new int[] {0, 1});

		VectorSliceMapper mapper = new VectorSliceMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[] {3.0, 4.0, 3.0}))).getField(0),
			new DenseVector(new double[] {3.0, 4.0}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {AlinkTypes.VECTOR}));
	}

	@Test
	public void testDenseOutputCol() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorSliceParams.SELECTED_COL, "vec")
			.set(VectorSliceParams.OUTPUT_COL, "res")
			.set(VectorSliceParams.INDICES, new int[] {0, 1});

		VectorSliceMapper mapper = new VectorSliceMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[] {3.0, 4.0, 3.0}))).getField(1),
			new DenseVector(new double[] {3.0, 4.0}));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[] {"vec", "res"},
				new TypeInformation <?>[] {Types.STRING, AlinkTypes.VECTOR})
		);
	}

	@Test
	public void testSparse() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorSliceParams.SELECTED_COL, "vec")
			.set(VectorSliceParams.OUTPUT_COL, "res")
			.set(VectorSliceParams.RESERVED_COLS, new String[] {})
			.set(VectorSliceParams.INDICES, new int[] {0, 2, 4});

		VectorSliceMapper mapper = new VectorSliceMapper(schema, params);

		assertEquals(mapper.map(Row.of(new SparseVector(5, new int[] {0, 2, 4}, new double[] {3.0, 4.0, 3.0})))
			.getField(0), new SparseVector(3, new int[] {0, 1, 2}, new double[] {3.0, 4.0, 3.0}));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[] {"res"}, new TypeInformation <?>[] {AlinkTypes.VECTOR})
		);
	}
}