package com.alibaba.alink.operator.common.regression.tensorflow;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.plugin.TFPredictorClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.testutil.categories.DLTest;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class TFTableModelRegressionModelMapperTest {

	@Category(DLTest.class)
	@Test
	public void test() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();

		RegisterKey registerKey = TFPredictorClassLoaderFactory.getRegisterKey();
		pluginDownloader.downloadPlugin(registerKey.getName(), registerKey.getVersion());

		List <Row> baseData = Arrays.asList(
			Row.of(1.2, 3.4, 10L, 3L, "yes", 0.),
			Row.of(1.2, 3.4, 2L, 5L, "no", 0.2),
			Row.of(1.2, 3.4, 6L, 8L, "no", 0.4),
			Row.of(1.2, 3.4, 3L, 2L, "yes", 1.0)
		);
		String dataSchemaStr = "f double, d double, i long, l long, s string, label double";

		Random random = new Random();
		List <Row> data = new ArrayList <>();
		for (int i = 0; i < 1000; i += 1) {
			data.add(baseData.get(random.nextInt(baseData.size())));
		}

		InputStream resourceAsStream = getClass().getClassLoader().
			getResourceAsStream("tf_table_model_regression_model.ak");
		String modelPath = Files.createTempFile("tf_table_model_regression_model", ".ak").toString();
		assert resourceAsStream != null;
		FileUtils.copyInputStreamToFile(resourceAsStream, new File(modelPath));

		BatchOperator <?> modelOp = new AkSourceBatchOp().setFilePath(modelPath);
		List <Row> modelRows = modelOp.collect();
		Params params = new Params();
		params.set(HasPredictionCol.PREDICTION_COL, "pred");
		params.set(HasReservedColsDefaultAsNull.RESERVED_COLS, new String[] {"s", "label"});

		TFTableModelRegressionModelMapper mapper = new TFTableModelRegressionModelMapper(modelOp.getSchema(),
			CsvUtil.schemaStr2Schema(dataSchemaStr), params);
		mapper.loadModel(modelRows);
		mapper.open();
		Assert.assertEquals(TableSchema.builder()
			.field("s", Types.STRING)
			.field("label", Types.DOUBLE)
			.field("pred", Types.DOUBLE)
			.build(), mapper.getOutputSchema()
		);
		for (Row row : data) {
			Row output = mapper.map(row);
			Assert.assertEquals(3, output.getArity());
			Assert.assertEquals(row.getField(4), output.getField(0));
			Assert.assertEquals(row.getField(5), output.getField(1));
		}
		mapper.close();
	}
}
