package com.alibaba.alink.operator.common.regression;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.feature.pca.PcaModelDataConverter;
import com.alibaba.alink.params.regression.GlmPredictParams;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GlmModelMapperTest {

	@Test
	public void testGamma() {
		List <Row> modelRows = Arrays.asList(
			Row.of(0L,
				"{\"epsilon\":\"1.0E-5\",\"linkPower\":\"1.0\",\"fitIntercept\":\"false\",\"link\":\"\\\"Log\\\"\","
					+ "\"featureCols\":\"[\\\"lot1\\\",\\\"lot2\\\"]\",\"regParam\":\"0.0\","
					+ "\"labelCol\":\"\\\"u\\\"\",\"offsetCol\":\"\\\"offset\\\"\",\"family\":\"\\\"Gamma\\\"\","
					+ "\"variancePower\":\"0.0\",\"weightCol\":\"\\\"weights\\\"\"}\n"),
			Row.of(1048576L, "[-0.12332670967971325,0.20479691748486428]"),
			Row.of(2097152L, "0.0"),
			Row.of(3145728L, "[1.7276368579696404E-4,5.736803808313293E-4]")
		);

		String[] colNames = new String[] {"u", "lot1", "lot2", "offset", "weights"};

		Object[][] dataRows = new Object[][] {
			{Math.log(10), 58.0, 35.0, 10.0, 2.0}
		};

		MemSourceBatchOp data = new MemSourceBatchOp(dataRows, colNames);

		TableSchema modelSchema = new PcaModelDataConverter().getModelSchema();
		TableSchema dataSchema = data.getSchema();
		Params params = new Params()
			.set(GlmPredictParams.PREDICTION_COL, "pred");

		GlmModelMapper modelMapper = new GlmModelMapper(modelSchema, dataSchema, params);
		modelMapper.loadModel(modelRows);

		assertArrayEquals(new String[] {"u", "lot1", "lot2", "offset", "weights", "pred"},
			modelMapper.getOutputSchema().getFieldNames());
		assertArrayEquals(
			new TypeInformation[] {Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE},
			modelMapper.getOutputSchema().getFieldTypes());

		Row in = Row.of(Math.log(10), 58.0, 35.0, 10.0, 2.0);
		Row out = modelMapper.map(in);
		assertEquals(in.getField(0), out.getField(0));
		assertEquals(in.getField(1), out.getField(1));
		assertEquals(in.getField(2), out.getField(2));
		assertEquals(in.getField(3), out.getField(3));
		assertEquals(in.getField(4), out.getField(4));
		assertEquals(22358.077643132798, out.getField(5));

		params.set(GlmPredictParams.LINK_PRED_RESULT_COL, "link_pred");

		modelMapper = new GlmModelMapper(modelSchema, dataSchema, params);
		modelMapper.loadModel(modelRows);

		assertArrayEquals(new String[] {"u", "lot1", "lot2", "offset", "weights", "pred", "link_pred"},
			modelMapper.getOutputSchema().getFieldNames());
		assertArrayEquals(
			new TypeInformation[] {Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE},
			modelMapper.getOutputSchema().getFieldTypes());

		out = modelMapper.map(in);
		assertEquals(in.getField(0), out.getField(0));
		assertEquals(in.getField(1), out.getField(1));
		assertEquals(in.getField(2), out.getField(2));
		assertEquals(in.getField(3), out.getField(3));
		assertEquals(in.getField(4), out.getField(4));
		assertEquals(22358.077643132798, out.getField(5));
		assertEquals(10.014942950546882, out.getField(6));

		assertNull(modelMapper.map(null));
	}

	@Test
	public void testGammaNoOffset() {
		List <Row> modelRows = Arrays.asList(
			Row.of(0L,
				"{\"epsilon\":\"1.0E-5\",\"linkPower\":\"1.0\",\"fitIntercept\":\"false\",\"link\":\"\\\"Log\\\"\","
					+ "\"featureCols\":\"[\\\"lot1\\\",\\\"lot2\\\"]\",\"regParam\":\"0.0\","
					+ "\"labelCol\":\"\\\"u\\\"\",\"family\":\"\\\"Gamma\\\"\",\"variancePower\":\"0.0\","
					+ "\"weightCol\":\"\\\"weights\\\"\"}\n"),
			Row.of(1048576L, "[-0.12332670967971325,0.20479691748486428]"),
			Row.of(2097152L, "0.0"),
			Row.of(3145728L, "[1.7276368579696404E-4,5.736803808313293E-4]")
		);

		String[] colNames = new String[] {"u", "lot1", "lot2", "offset", "weights"};

		Object[][] dataRows = new Object[][] {
			{Math.log(10), 58.0, 35.0, 10.0, 2.0}
		};

		MemSourceBatchOp data = new MemSourceBatchOp(dataRows, colNames);

		TableSchema modelSchema = new PcaModelDataConverter().getModelSchema();
		TableSchema dataSchema = data.getSchema();
		Params params = new Params()
			.set(GlmPredictParams.PREDICTION_COL, "pred");

		GlmModelMapper modelMapper = new GlmModelMapper(modelSchema, dataSchema, params);
		modelMapper.loadModel(modelRows);

		Row in = Row.of(Math.log(10), 58.0, 35.0, 10.0, 2.0);
		Row out = modelMapper.map(in);
		assertEquals(in.getField(0), out.getField(0));
		assertEquals(in.getField(1), out.getField(1));
		assertEquals(in.getField(2), out.getField(2));
		assertEquals(in.getField(3), out.getField(3));
		assertEquals(in.getField(4), out.getField(4));
		assertEquals(1.0150551546224118, out.getField(5));
	}

}