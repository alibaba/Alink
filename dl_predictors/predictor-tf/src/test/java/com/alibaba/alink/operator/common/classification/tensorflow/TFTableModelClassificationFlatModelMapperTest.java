package com.alibaba.alink.operator.common.classification.tensorflow;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.common.classification.tensorflow.TFTableModelClassificationFlatModelMapper;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
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

public class TFTableModelClassificationFlatModelMapperTest {

	@Category(DLTest.class)
	@Test
	public void test() throws Exception {
		List <Row> baseData = Arrays.asList(
			Row.of((float) 1.2, 3.4, 10, 3L, "bad"),
			Row.of((float) 1.2, 3.4, 2, 5L, "good"),
			Row.of((float) 1.2, 3.4, 6, 8L, "bad"),
			Row.of((float) 1.2, 3.4, 3, 2L, "good")
		);
		String dataSchemaStr = "f float, d double, i int, l long, label string";

		Random random = new Random();
		List <Row> data = new ArrayList <>();
		for (int i = 0; i < 1000; i += 1) {
			data.add(baseData.get(random.nextInt(baseData.size())));
		}

		InputStream resourceAsStream = getClass().getClassLoader().
			getResourceAsStream("tf_table_model_binary_class_model.ak");
		String modelPath = Files.createTempFile("tf_table_model_binary_class_model", ".ak").toString();
		assert resourceAsStream != null;
		FileUtils.copyInputStreamToFile(resourceAsStream, new File(modelPath));

		BatchOperator <?> modelOp = new AkSourceBatchOp().setFilePath(modelPath);
		List <Row> modelRows = modelOp.collect();
		Params params = new Params();
		params.set(HasPredictionCol.PREDICTION_COL, "pred");
		params.set(HasPredictionDetailCol.PREDICTION_DETAIL_COL, "pred_detail");
		params.set(HasReservedColsDefaultAsNull.RESERVED_COLS, new String[] {"l", "label"});

		TFTableModelClassificationFlatModelMapper mapper = new TFTableModelClassificationFlatModelMapper(modelOp.getSchema(),
			CsvUtil.schemaStr2Schema(dataSchemaStr), params);
		mapper.loadModel(modelRows);

		List <Row> list = new ArrayList <>();
		ListCollector <Row> collector = new ListCollector <>(list);
		mapper.open();
		for (Row row : data) {
			mapper.flatMap(row, collector);
		}
		mapper.close();
		Assert.assertEquals(TableSchema.builder()
			.field("l", Types.LONG)
			.field("label", Types.STRING)
			.field("pred", Types.STRING)
			.field("pred_detail", Types.STRING)
			.build(), mapper.getOutputSchema()
		);
		Assert.assertEquals(data.size(), list.size());
		for (int i = 0; i < data.size(); i += 1) {
			Assert.assertEquals(4, list.get(i).getArity());
			Assert.assertEquals(data.get(i).getField(3), list.get(i).getField(0));
			Assert.assertEquals(data.get(i).getField(4), list.get(i).getField(1));
		}
	}
}
