package com.alibaba.alink.operator.common.dataproc.vector;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.dataproc.vector.VectorInteractionParams;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for VectorInteractionMapper.
 */
public class VectorInteractionMapperTest {
	@Test
	public void testReserveTwoCol() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"c0", "c1"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING});

		TableSchema outSchema = new TableSchema(new String[] {"c0", "c1", "out"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, VectorTypes.VECTOR});

		Params params = new Params()
			.set(VectorInteractionParams.SELECTED_COLS, new String[] {"c0", "c1"})
			.set(VectorInteractionParams.OUTPUT_COL, "out");

		VectorInteractionMapper mapper = new VectorInteractionMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}), new DenseVector(new double[]{3.0, 4.0})))
				.getField(2), new DenseVector(new double[]{9.0, 12.0, 12.0, 16.0}));
		assertEquals(mapper.getOutputSchema(), outSchema);
	}

	@Test
	public void testReserveOneCol() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"c0", "c1"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING});

		TableSchema outSchema = new TableSchema(new String[] {"c0", "out"},
			new TypeInformation <?>[] {Types.STRING, VectorTypes.VECTOR});

		Params params = new Params()
			.set(VectorInteractionParams.SELECTED_COLS, new String[] {"c0", "c1"})
			.set(VectorInteractionParams.OUTPUT_COL, "out")
			.set(VectorInteractionParams.RESERVED_COLS, new String[] {"c0"});

		VectorInteractionMapper mapper = new VectorInteractionMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}), new DenseVector(new double[]{3.0, 4.0})))
				.getField(1), new DenseVector(new double[]{9.0, 12.0, 12.0, 16.0}));
		assertEquals(mapper.getOutputSchema(), outSchema);
	}

	@Test
	public void testSparse() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"c0", "c1"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING});

		TableSchema outSchema = new TableSchema(new String[] {"c0", "out"},
			new TypeInformation <?>[] {Types.STRING, VectorTypes.VECTOR});

		Params params = new Params()
			.set(VectorInteractionParams.SELECTED_COLS, new String[] {"c0", "c1"})
			.set(VectorInteractionParams.OUTPUT_COL, "out")
			.set(VectorInteractionParams.RESERVED_COLS, new String[] {"c0"});

		VectorInteractionMapper mapper = new VectorInteractionMapper(schema, params);

		assertEquals(mapper.map(Row.of(new SparseVector(10, new int[]{0, 9}, new double[]{1.0, 4.0}),
				new SparseVector(10, new int[]{0, 9}, new double[]{1.0, 4.0}))).getField(1),
			new SparseVector(100, new int[]{0, 9, 90, 99}, new double[]{1.0, 4.0, 4.0, 16.0}));
		assertEquals(mapper.getOutputSchema(), outSchema);
	}
}