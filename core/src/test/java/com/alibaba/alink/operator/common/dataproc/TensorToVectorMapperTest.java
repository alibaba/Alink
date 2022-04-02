package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.params.dataproc.TensorToVectorParams;
import com.alibaba.alink.params.dataproc.TensorToVectorParams.ConvertMethod;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class TensorToVectorMapperTest extends AlinkTestBase {
	private static final double EPS = 0.00001;

	@Test
	public void test() throws Exception {
		final Mapper mapper = new TensorToVectorMapper(
			new TableSchema(
				new String[] {"tensor"},
				new TypeInformation <?>[] {AlinkTypes.FLOAT_TENSOR}
			),
			new Params()
				.set(TensorToVectorParams.SELECTED_COL, "tensor")
				.set(TensorToVectorParams.CONVERT_METHOD, ConvertMethod.SUM)
		);

		final Tensor <?> tensor = TensorUtil.getTensor("FLOAT#3,2#0.0 0.1 1.0 1.1 2.0 2.1 ");

		Assert.assertArrayEquals(
			new double[] {3.0, 3.3},
			((DenseVector) (mapper.map(Row.of(tensor)).getField(0))).getData(),
			EPS
		);
	}
}