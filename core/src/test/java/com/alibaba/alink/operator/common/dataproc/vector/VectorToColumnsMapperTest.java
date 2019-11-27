package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.params.dataproc.vector.VectorToColumnsParams;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for VectorToColumnsMapper.
 */
public class VectorToColumnsMapperTest {
	@Test
	public void testDenseReversed() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorToColumnsParams.SELECTED_COL, "vec")
			.set(VectorToColumnsParams.OUTPUT_COLS, new String[] {"f0", "f1"});

		VectorToColumnsMapper mapper = new VectorToColumnsMapper(schema, params);
		Row row = mapper.map(Row.of(new DenseVector(new double[] {3.0, 4.0})));
		assertEquals(row.getField(1), 3.0);
		assertEquals(row.getField(2), 4.0);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"vec", "f0", "f1"},
			new TypeInformation <?>[] {Types.STRING, Types.DOUBLE, Types.DOUBLE}));

	}

	@Test
	public void testDense() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorToColumnsParams.SELECTED_COL, "vec")
			.set(VectorToColumnsParams.RESERVED_COLS, new String[] {})
			.set(VectorToColumnsParams.OUTPUT_COLS, new String[] {"f0", "f1"});

		VectorToColumnsMapper mapper = new VectorToColumnsMapper(schema, params);

		Row row = mapper.map(Row.of(new DenseVector(new double[] {3.0, 4.0})));
		assertEquals(row.getField(0), 3.0);
		assertEquals(row.getField(1), 4.0);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"f0", "f1"},
			new TypeInformation <?>[] {Types.DOUBLE, Types.DOUBLE}));
	}

	@Test
	public void testSparse() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});
		//here don't set RESERVED_COLS, but reserve the inout data "vec"?
		Params params = new Params()
			.set(VectorToColumnsParams.SELECTED_COL, "vec")
			.set(VectorToColumnsParams.OUTPUT_COLS, new String[] {"f0", "f1", "f2"});

		VectorToColumnsMapper mapper = new VectorToColumnsMapper(schema, params);

		RowCollector collector = new RowCollector();

		Row row = mapper.map(Row.of(new SparseVector(3, new int[] {1, 2}, new double[] {3.0, 4.0})));
		assertEquals(row.getField(0), new SparseVector(3, new int[] {1, 2}, new double[] {3.0, 4.0}));
		assertEquals(row.getField(1), 0.0);
		assertEquals(row.getField(2), 3.0);
		assertEquals(row.getField(3), 4.0);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"vec", "f0", "f1", "f2"},
			new TypeInformation <?>[] {Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE}));
	}

	@Test
	public void testNull() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorToColumnsParams.SELECTED_COL, "vec")
			.set(VectorToColumnsParams.RESERVED_COLS, new String[] {})
			.set(VectorToColumnsParams.OUTPUT_COLS, new String[] {"f0", "f1"});

		VectorToColumnsMapper mapper = new VectorToColumnsMapper(schema, params);

		Row row = mapper.map(Row.of((Object) null));
		assertEquals(row.getField(0), null);
		assertEquals(row.getField(1), null);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"f0", "f1"},
			new TypeInformation <?>[] {Types.DOUBLE, Types.DOUBLE}));
	}

}