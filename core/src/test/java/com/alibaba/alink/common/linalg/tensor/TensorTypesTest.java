package com.alibaba.alink.common.linalg.tensor;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.DataSetUtil;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;

public class TensorTypesTest {
	@Test
	public void test() throws Exception {
		Row[] tensors = new Row[] {
			Row.of(new FloatTensor(new float[] {0.0f, 1.0f}))
		};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			Arrays.asList(tensors),
			new TableSchema(new String[] {"col0"}, new TypeInformation <?>[] {TensorTypes.TENSOR})
		);

		DataSet <Tensor <?>> mapedTensor = memSourceBatchOp
			.getDataSet()
			.map(new MapFunction <Row, Tensor <?>>() {
				@Override
				public Tensor <?> map(Row value) throws Exception {
					return (Tensor <?>) value.getField(0);
				}
			});

		new TableSourceBatchOp(DataSetConversionUtil.toTable(
			MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,
			mapedTensor.map(new MapFunction <Tensor <?>, Row>() {
				@Override
				public Row map(Tensor <?> value) throws Exception {
					return Row.of(value);
				}
			}),
			new String[] {"col0"},
			new TypeInformation<?>[] {TensorTypes.TENSOR}
		)).print();
	}
}