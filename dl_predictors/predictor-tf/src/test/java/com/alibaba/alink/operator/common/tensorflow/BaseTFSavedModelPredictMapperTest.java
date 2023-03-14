package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.dl.plugin.TFPredictorClassLoaderFactory;
import com.alibaba.alink.common.dl.utils.FileDownloadUtils;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.dl.utils.ZipFileUtil;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.dl.HasModelPath;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputSchemaStr;
import com.alibaba.alink.testutil.categories.DLTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class BaseTFSavedModelPredictMapperTest {

	@Category(DLTest.class)
	@Test
	public void testString() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();

		RegisterKey registerKey = TFPredictorClassLoaderFactory.getRegisterKey();
		pluginDownloader.downloadPlugin(registerKey.getName(), registerKey.getVersion());

		MLEnvironmentFactory.getDefault().getExecutionEnvironment().setParallelism(2);
		String url = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/mnist_dense.csv";
		String schema = "label bigint, image string";

		BatchOperator <?> data = new CsvSourceBatchOp().setFilePath(url).setSchemaStr(schema).setFieldDelimiter(";");
		List <Row> rows = data.collect();

		String modelPath = "http://alink-dataset.oss-cn-zhangjiakou.aliyuncs.com/tf/1551968314.zip";
		String workDir = PythonFileUtils.createTempDir("temp_").toString();
		String fn = FileDownloadUtils.downloadHttpOrOssFile(modelPath, workDir);
		String localModelPath = workDir + File.separator + fn;
		System.out.println("localModelPath" + localModelPath);
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
		BaseTFSavedModelPredictMapper baseTFSavedModelPredictMapper = new BaseTFSavedModelPredictMapper(
			data.getSchema(), params);
		baseTFSavedModelPredictMapper.open();
		Assert.assertEquals(TableSchema.builder()
			.field("label", Types.LONG)
			.field("image", Types.STRING)
			.field("classes", Types.LONG)
			.field("probabilities", Types.STRING)
			.build(), baseTFSavedModelPredictMapper.getOutputSchema()
		);
		for (Row row : rows) {
			Row output = baseTFSavedModelPredictMapper.map(row);
			Assert.assertEquals(row.getField(0), output.getField(0));
			Assert.assertEquals(row.getField(1), output.getField(1));
		}
		baseTFSavedModelPredictMapper.close();
	}

	@Category(DLTest.class)
	@Test
	public void testTensor() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();

		RegisterKey registerKey = TFPredictorClassLoaderFactory.getRegisterKey();
		pluginDownloader.downloadPlugin(registerKey.getName(), registerKey.getVersion());

		MLEnvironmentFactory.getDefault().getExecutionEnvironment().setParallelism(2);

		int batchSize = 3;
		List <Row> rows = new ArrayList <>();
		for (int i = 0; i < 1000; i += 1) {
			Row row = Row.of(new LongTensor((new Shape(batchSize))),
				new FloatTensor(new Shape(batchSize, 28, 28)));
			rows.add(row);
		}
		BatchOperator <?> data = new MemSourceBatchOp(rows, "label LONG_TENSOR, image FLOAT_TENSOR");

		String modelPath = "http://alink-dataset.oss-cn-zhangjiakou.aliyuncs.com/tf/1551968314.zip";
		String workDir = PythonFileUtils.createTempDir("temp_").toString();
		String fn = FileDownloadUtils.downloadHttpOrOssFile(modelPath, workDir);
		String localModelPath = workDir + File.separator + fn;
		System.out.println("localModelPath:" + localModelPath);
		if (localModelPath.endsWith(".zip")) {
			File target = new File(localModelPath).getParentFile();
			ZipFileUtil.unZip(new File(localModelPath), target);
			localModelPath = localModelPath.substring(0, localModelPath.length() - ".zip".length());
			Preconditions.checkArgument(new File(localModelPath).exists(), "problematic zip file.");
		}

		Params params = new Params();
		params.set(HasModelPath.MODEL_PATH, localModelPath);
		params.set(HasSelectedCols.SELECTED_COLS, new String[] {"image"});
		params.set(HasOutputSchemaStr.OUTPUT_SCHEMA_STR, "classes LONG_TENSOR, probabilities FLOAT_TENSOR");
		BaseTFSavedModelPredictMapper baseTFSavedModelPredictMapper = new BaseTFSavedModelPredictMapper(
			data.getSchema(), params);
		baseTFSavedModelPredictMapper.open();
		Assert.assertEquals(TableSchema.builder()
			.field("label", AlinkTypes.LONG_TENSOR)
			.field("image", AlinkTypes.FLOAT_TENSOR)
			.field("classes", AlinkTypes.LONG_TENSOR)
			.field("probabilities", AlinkTypes.FLOAT_TENSOR)
			.build(), baseTFSavedModelPredictMapper.getOutputSchema()
		);
		for (Row row : rows) {
			Row output = baseTFSavedModelPredictMapper.map(row);
			Assert.assertEquals(row.getField(0), output.getField(0));
			Assert.assertEquals(row.getField(1), output.getField(1));
			Assert.assertArrayEquals(((LongTensor) output.getField(2)).shape(), new long[] {batchSize});
			Assert.assertArrayEquals(((FloatTensor) output.getField(3)).shape(), new long[] {batchSize, 10});
		}
		baseTFSavedModelPredictMapper.close();
	}
}
