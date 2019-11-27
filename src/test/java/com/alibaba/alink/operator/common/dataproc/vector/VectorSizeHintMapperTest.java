package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.dataproc.vector.VectorSizeHintParams;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for VectorSizeHintMapper.
 */
public class VectorSizeHintMapperTest {
	@Test
	public void testError() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorSizeHintParams.SELECTED_COL, "vec")
			.set(VectorSizeHintParams.SIZE, 3);

		VectorSizeHintMapper mapper = new VectorSizeHintMapper(schema, params);
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {VectorTypes.VECTOR}));
	}

	@Test
	public void testOptimistic() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorSizeHintParams.SELECTED_COL, "vec")
			.set(VectorSizeHintParams.OUTPUT_COL, "res")
			.set(VectorSizeHintParams.HANDLE_INVALID, "optimistic")
			.set(VectorSizeHintParams.SIZE, 2);

		VectorSizeHintMapper mapper = new VectorSizeHintMapper(schema, params);
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[] {"vec", "res"},
					new TypeInformation <?>[] {Types.STRING, VectorTypes.VECTOR}));
	}
}