package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.linalg.tensor.DataType;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ToTensorBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.dataproc.ToTensorParams;
import com.alibaba.alink.params.shared.HasHandleInvalid.HandleInvalidMethod;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.Consumer;

public class ToTensorMapperTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		final Mapper mapper = new ToTensorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {AlinkTypes.DENSE_VECTOR}
			),
			new Params()
				.set(ToTensorParams.SELECTED_COL, "vec")
		);

		final DoubleTensor tensor = DoubleTensor.of(TensorUtil.getTensor("FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "));
		final Tensor <?> result = (Tensor <?>) mapper.map(Row.of(tensor.toVector())).getField(0);

		Assert.assertEquals(tensor, result);
	}

	@Test
	public void testReshape() throws Exception {
		final Mapper mapper = new ToTensorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {AlinkTypes.DENSE_VECTOR}
			),
			new Params()
				.set(ToTensorParams.SELECTED_COL, "vec")
				.set(ToTensorParams.TENSOR_SHAPE, new Long[] {2L, 3L})
		);

		final DoubleTensor tensor = DoubleTensor.of(TensorUtil.getTensor("FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "));
		final DoubleTensor expect = tensor.reshape(new Shape(2L, 3L));

		final Tensor <?> result = (Tensor <?>) mapper.map(Row.of(tensor.toVector())).getField(0);

		Assert.assertEquals(expect, result);
	}

	@Test
	public void testFloatType() throws Exception {
		final Mapper mapper = new ToTensorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {AlinkTypes.DENSE_VECTOR}
			),
			new Params()
				.set(ToTensorParams.SELECTED_COL, "vec")
				.set(ToTensorParams.TENSOR_SHAPE, new Long[] {2L, 3L})
				.set(ToTensorParams.TENSOR_DATA_TYPE, DataType.FLOAT)
		);

		Assert.assertEquals(AlinkTypes.FLOAT_TENSOR, mapper.getOutputSchema().getFieldTypes()[0]);

		final DoubleTensor tensor = DoubleTensor.of(TensorUtil.getTensor("FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "));
		final FloatTensor expect = FloatTensor.of(tensor.reshape(new Shape(2L, 3L)));

		final Tensor <?> result = (Tensor <?>) mapper.map(Row.of(tensor.toVector())).getField(0);

		Assert.assertEquals(expect, result);
	}

	@Test
	public void testDefaultType() throws Exception {
		final Mapper mapper = new ToTensorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {AlinkTypes.DENSE_VECTOR}
			),
			new Params()
				.set(ToTensorParams.SELECTED_COL, "vec")
				.set(ToTensorParams.TENSOR_SHAPE, new Long[] {2L, 3L})
		);

		Assert.assertEquals(AlinkTypes.TENSOR, mapper.getOutputSchema().getFieldTypes()[0]);

		final DoubleTensor tensor = DoubleTensor.of(TensorUtil.getTensor("FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "));

		final DoubleTensor expect = tensor.reshape(new Shape(2L, 3L));

		final Tensor <?> result = (Tensor <?>) mapper.map(Row.of(tensor.toVector())).getField(0);

		Assert.assertEquals(expect, result);
	}

	@Test
	public void testStringType() throws Exception {
		final Mapper mapper = new ToTensorMapper(
			new TableSchema(
				new String[] {"str"},
				new TypeInformation <?>[] {Types.STRING}
			),
			new Params()
				.set(ToTensorParams.SELECTED_COL, "str")
				.set(ToTensorParams.TENSOR_DATA_TYPE, DataType.STRING)
		);

		Assert.assertEquals(AlinkTypes.STRING_TENSOR, mapper.getOutputSchema().getFieldTypes()[0]);

		final DoubleTensor tensor = DoubleTensor.of(TensorUtil.getTensor("FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "));
		final StringTensor expect = new StringTensor(tensor.toString());

		final Tensor <?> result = (Tensor <?>) mapper.map(Row.of(tensor.toString())).getField(0);

		Assert.assertEquals(expect, result);
	}

	@Test(expected = AkIllegalOperatorParameterException.class)
	public void testHandleInvalidError() throws Exception {
		final Mapper mapper = new ToTensorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {AlinkTypes.DENSE_VECTOR}
			),
			new Params()
				.set(ToTensorParams.SELECTED_COL, "vec")
				.set(ToTensorParams.TENSOR_SHAPE, new Long[] {2L, 3L})
				.set(ToTensorParams.TENSOR_DATA_TYPE, DataType.INT)
		);

		Assert.assertEquals(AlinkTypes.INT_TENSOR, mapper.getOutputSchema().getFieldTypes()[0]);

		final DoubleTensor tensor = DoubleTensor.of(TensorUtil.getTensor("FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "));

		mapper.map(Row.of(tensor.toVector())).getField(0);
	}

	@Test
	public void testHandleInvalidSkip() throws Exception {
		final Mapper mapper = new ToTensorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {AlinkTypes.DENSE_VECTOR}
			),
			new Params()
				.set(ToTensorParams.SELECTED_COL, "vec")
				.set(ToTensorParams.TENSOR_SHAPE, new Long[] {2L, 3L})
				.set(ToTensorParams.TENSOR_DATA_TYPE, DataType.INT)
				.set(ToTensorParams.HANDLE_INVALID, HandleInvalidMethod.SKIP)
		);

		Assert.assertEquals(AlinkTypes.INT_TENSOR, mapper.getOutputSchema().getFieldTypes()[0]);

		final DoubleTensor tensor = DoubleTensor.of(TensorUtil.getTensor("FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "));

		final Tensor <?> result = (Tensor <?>) mapper.map(Row.of(tensor.toVector())).getField(0);

		Assert.assertNull(result);
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
				new ToTensorBatchOp()
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