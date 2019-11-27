package com.alibaba.alink.operator.common.dataproc.vector;

import com.alibaba.alink.common.linalg.SparseVector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.dataproc.vector.VectorPolynomialExpandParams;
import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for PolynomialExpansionMapper.
 */
public class PolynomialExpansionMapperTest {
	@Test
	public void testDense() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(VectorPolynomialExpandParams.SELECTED_COL, "vec")
			.set(VectorPolynomialExpandParams.DEGREE, 2);

		PolynomialExpansionMapper mapper = new PolynomialExpansionMapper(schema, params);

		assertEquals(new DenseVector(new double[]{3.0, 9.0, 4.0, 12.0, 16.0}), mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}))).getField(0));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{VectorTypes.VECTOR}));
	}

	@Test
	public void testSparse() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(VectorPolynomialExpandParams.SELECTED_COL, "vec")
			.set(VectorPolynomialExpandParams.OUTPUT_COL, "res")
			.set(VectorPolynomialExpandParams.DEGREE, 2);

		PolynomialExpansionMapper mapper = new PolynomialExpansionMapper(schema, params);
		assertEquals(new SparseVector(9, new int[]{0, 1, 5, 6, 8}, new double[]{2.0, 4.0, 3.0, 6.0, 9.0}),
			mapper.map(Row.of(new SparseVector(3, new int[]{0,2}, new double[]{2.0, 3.0}))).getField(1));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[]{"vec", "res"}, new TypeInformation<?>[]{Types.STRING, VectorTypes.VECTOR})
		);

	}

	@Test
	public void testGetPolySize() {
		int res1 = PolynomialExpansionMapper.getPolySize(4, 4);
		int res2 = PolynomialExpansionMapper.getPolySize(65, 2);
		assertEquals(res1, 70);
		assertEquals(res2, 2211);
	}

}
