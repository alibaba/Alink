package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.linalg.tensor.TensorTypes;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.dl.utils.FileDownloadUtils;
import com.alibaba.alink.common.dl.utils.ZipFileUtil;
import com.alibaba.alink.operator.common.tensorflow.BaseTFSavedModelPredictRowMapper;
import com.alibaba.alink.params.dl.HasModelPath;
import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputSchemaStr;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.testutil.categories.DLTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class BaseTFSavedModelPredictRowMapperTest {

	@Category(DLTest.class)
	@Test
	public void testString() throws Exception {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().setParallelism(2);
		String url = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/mnist_dense.csv";
		String schema = "label bigint, image string";

		BatchOperator <?> data = new CsvSourceBatchOp().setFilePath(url).setSchemaStr(schema).setFieldDelimiter(";");
		List <Row> rows = data.collect();

		String modelPath = "http://alink-dataset.oss-cn-zhangjiakou.aliyuncs.com/tf/1551968314.zip";
		String workDir = PythonFileUtils.createTempWorkDir("temp_");
		String fn = FileDownloadUtils.downloadHttpOrOssFile(modelPath, workDir);
		String localModelPath = workDir + File.separator + fn;
		System.out.println("localModelPath: " + localModelPath);
		if (localModelPath.endsWith(".zip")) {
			File target = new File(localModelPath).getParentFile();
			ZipFileUtil.unZip(new File(localModelPath), target);
			localModelPath = localModelPath.substring(0, localModelPath.length() - ".zip".length());
			Preconditions.checkArgument(new File(localModelPath).exists(), "problematic zip file.");
		}

		Params params = new Params();
		params.set(HasModelPath.MODEL_PATH, localModelPath);
		params.set(HasSelectedCols.SELECTED_COLS, new String[] {"image"});
		params.set(HasOutputSchemaStr.OUTPUT_SCHEMA_STR, "classes bigint, probabilities string");
		BaseTFSavedModelPredictRowMapper baseTFSavedModelPredictRowMapper = new BaseTFSavedModelPredictRowMapper(
			data.getSchema(), params);
		baseTFSavedModelPredictRowMapper.open();
		Assert.assertEquals(TableSchema.builder()
			.field("label", Types.LONG)
			.field("image", Types.STRING)
			.field("classes", Types.LONG)
			.field("probabilities", Types.STRING)
			.build(), baseTFSavedModelPredictRowMapper.getOutputSchema()
		);
		for (Row row : rows) {
			Row output = baseTFSavedModelPredictRowMapper.map(row);
			Assert.assertEquals(row.getField(0), output.getField(0));
			Assert.assertEquals(row.getField(1), output.getField(1));
		}
		baseTFSavedModelPredictRowMapper.close();
	}

	@Category(DLTest.class)
	@Test
	public void testTensor() throws Exception {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().setParallelism(2);

		List <Row> rows = new ArrayList <>();
		for (int i = 0; i < 1000; i += 1) {
			Row row = Row.of(0,
				new FloatTensor(new Shape(28, 28)));
			rows.add(row);
		}
		BatchOperator <?> data = new MemSourceBatchOp(rows, "label LONG, image TENSOR_TYPES_FLOAT_TENSOR");

		String modelPath = "http://alink-dataset.oss-cn-zhangjiakou.aliyuncs.com/tf/1551968314.zip";
		String workDir = PythonFileUtils.createTempWorkDir("temp_");
		String fn = FileDownloadUtils.downloadHttpOrOssFile(modelPath, workDir);
		String localModelPath = workDir + File.separator + fn;
		System.out.println("localModelPath: " + localModelPath);
		if (localModelPath.endsWith(".zip")) {
			File target = new File(localModelPath).getParentFile();
			ZipFileUtil.unZip(new File(localModelPath), target);
			localModelPath = localModelPath.substring(0, localModelPath.length() - ".zip".length());
			Preconditions.checkArgument(new File(localModelPath).exists(), "problematic zip file.");
		}

		Params params = new Params();
		params.set(HasModelPath.MODEL_PATH, localModelPath);
		params.set(HasSelectedCols.SELECTED_COLS, new String[] {"image"});
		params.set(HasOutputSchemaStr.OUTPUT_SCHEMA_STR, "classes LONG, probabilities TENSOR_TYPES_FLOAT_TENSOR");
		BaseTFSavedModelPredictRowMapper baseTFSavedModelPredictRowMapper = new BaseTFSavedModelPredictRowMapper(
			data.getSchema(), params);
		baseTFSavedModelPredictRowMapper.open();
		Assert.assertEquals(TableSchema.builder()
			.field("label", Types.LONG)
			.field("image", TensorTypes.FLOAT_TENSOR)
			.field("classes", Types.LONG)
			.field("probabilities", TensorTypes.FLOAT_TENSOR)
			.build(), baseTFSavedModelPredictRowMapper.getOutputSchema()
		);
		for (Row row : rows) {
			Row output = baseTFSavedModelPredictRowMapper.map(row);
			Assert.assertEquals(row.getField(0), output.getField(0));
			Assert.assertEquals(row.getField(1), output.getField(1));
			Assert.assertArrayEquals(((FloatTensor) output.getField(3)).shape(), new long[] {10});
		}
		baseTFSavedModelPredictRowMapper.close();
	}
}
