package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.linalg.tensor.DataType;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.VectorToTensorBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.dataproc.VectorToTensorParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.Consumer;

public class VectorToTensorMapperTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		final Mapper mapper = new VectorToTensorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {AlinkTypes.DENSE_VECTOR}
			),
			new Params()
				.set(VectorToTensorParams.SELECTED_COL, "vec")
		);

		final DoubleTensor tensor = DoubleTensor.of(TensorUtil.getTensor("FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "));
		final Tensor <?> result = (Tensor <?>) mapper.map(Row.of(tensor.toVector())).getField(0);

		Assert.assertEquals(tensor, result);
	}

	@Test
	public void testReshape() throws Exception {
		final Mapper mapper = new VectorToTensorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {AlinkTypes.DENSE_VECTOR}
			),
			new Params()
				.set(VectorToTensorParams.SELECTED_COL, "vec")
				.set(VectorToTensorParams.TENSOR_SHAPE, new Long[] {2L, 3L})
		);

		final DoubleTensor tensor = DoubleTensor.of(TensorUtil.getTensor("FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "));
		final DoubleTensor expect = tensor.reshape(new Shape(2L, 3L));

		final Tensor <?> result = (Tensor <?>) mapper.map(Row.of(tensor.toVector())).getField(0);

		Assert.assertEquals(expect, result);
	}

	@Test
	public void testFloatType() throws Exception {
		final Mapper mapper = new VectorToTensorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {AlinkTypes.DENSE_VECTOR}
			),
			new Params()
				.set(VectorToTensorParams.SELECTED_COL, "vec")
				.set(VectorToTensorParams.TENSOR_SHAPE, new Long[] {2L, 3L})
				.set(VectorToTensorParams.TENSOR_DATA_TYPE, DataType.FLOAT)
		);

		Assert.assertEquals(AlinkTypes.FLOAT_TENSOR, mapper.getOutputSchema().getFieldTypes()[0]);

		final DoubleTensor tensor = DoubleTensor.of(TensorUtil.getTensor("FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "));
		final FloatTensor expect = FloatTensor.of(tensor.reshape(new Shape(2L, 3L)));

		final Tensor <?> result = (Tensor <?>) mapper.map(Row.of(tensor.toVector())).getField(0);

		Assert.assertEquals(expect, result);
	}

	@Test
	public void testOp() throws Exception {
		final DoubleTensor tensor = DoubleTensor.of(TensorUtil.getTensor("FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "));
		final FloatTensor expect = FloatTensor.of(tensor.reshape(new Shape(2L, 3L)));

		Row[] rows = new Row[] {
			Row.of(tensor.toVector())
		};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			rows, new String[] {"vec"}
		);

		memSourceBatchOp
			.link(
				new VectorToTensorBatchOp()
					.setSelectedCol("vec")
					.setTensorShape(2, 3)
					.setTensorDataType("float")
			)
			.lazyCollect(new Consumer <List <Row>>() {
				@Override
				public void accept(List <Row> rows) {
					Assert.assertEquals(expect, TensorUtil.getTensor(rows.get(0).getField(0)));
				}
			});

		BatchOperator.execute();
	}
}