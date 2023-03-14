package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.dataproc.vector.VectorSizeHintParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for VectorSizeHintMapper.
 */

public class VectorSizeHintMapperTest extends AlinkTestBase {
	@Test
	public void testError() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorSizeHintParams.SELECTED_COL, "vec")
			.set(VectorSizeHintParams.SIZE, 3);

		VectorSizeHintMapper mapper = new VectorSizeHintMapper(schema, params);
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {AlinkTypes.VECTOR}));
	}

	@Test
	public void testSkip() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorSizeHintParams.SELECTED_COL, "vec")
			.set(VectorSizeHintParams.OUTPUT_COL, "res")
			.set(VectorSizeHintParams.HANDLE_INVALID, ParamUtil.searchEnum(VectorSizeHintParams.HANDLE_INVALID,
				"skip"))
			.set(VectorSizeHintParams.SIZE, 2);

		VectorSizeHintMapper mapper = new VectorSizeHintMapper(schema, params);
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[] {"vec", "res"},
				new TypeInformation <?>[] {Types.STRING, AlinkTypes.VECTOR}));
	}
}