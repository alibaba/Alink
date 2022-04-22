package com.alibaba.alink.operator.common.onnx;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.params.dl.HasModelPath;
import com.alibaba.alink.params.onnx.HasInputNames;
import com.alibaba.alink.params.onnx.HasOutputNames;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputSchemaStr;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class OnnxModelPredictMapperTest {
	@Test
	public void test() throws Exception {
		File file = File.createTempFile("cnn_mnist_pytorch", ".onnx");
		InputStream modelInputStream = getClass().getResourceAsStream("/cnn_mnist_pytorch.onnx");
		assert modelInputStream != null;
		Files.copy(modelInputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);

		Params params = new Params();
		params.set(HasModelPath.MODEL_PATH, "file://" + file.getAbsolutePath());
		params.set(HasInputNames.INPUT_NAMES, new String[] {"0"});
		params.set(HasOutputNames.OUTPUT_NAMES, new String[] {"21"});
		params.set(HasOutputSchemaStr.OUTPUT_SCHEMA_STR, "output FLOAT_TENSOR");
		params.set(HasReservedColsDefaultAsNull.RESERVED_COLS, new String[] {});

		TableSchema tableSchema = TableSchema.builder()
			.field("tensor", AlinkTypes.FLOAT_TENSOR).build();
		OnnxModelPredictMapper mapper = new OnnxModelPredictMapper(tableSchema, params);
		mapper.open();
		Row out = mapper.map(Row.of(new FloatTensor(new Shape(1, 1, 28, 28))));
		Assert.assertTrue(out.getField(0) instanceof FloatTensor);
		FloatTensor tensor = (FloatTensor) out.getField(0);
		Assert.assertArrayEquals(tensor.shape(), new long[] {1, 10});
		mapper.close();
		file.delete();
	}
}
