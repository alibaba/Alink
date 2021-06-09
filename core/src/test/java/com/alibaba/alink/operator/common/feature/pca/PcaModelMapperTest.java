package com.alibaba.alink.operator.common.feature.pca;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.feature.PcaPredictParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class PcaModelMapperTest extends AlinkTestBase {

	private TableSchema modelSchema;
	private TableSchema dataSchema;
	private List <Row> modelRows;

	@Test
	public void testDenseDefault() throws Exception {
		genDense();
		Params params = new Params()
			.set(PcaPredictParams.PREDICTION_COL, "pred");

		PcaModelMapper mapper = new PcaModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(modelRows);

		assertArrayEquals(mapper.getOutputSchema().getFieldNames(), new String[] {"id", "vec", "pred"});

		Row in = Row.of(1L, "0.1 0.2 0.3 0.4");
		Row out = mapper.map(in);

		assertEquals(in.getField(0), out.getField(0));
		assertEquals(in.getField(1), out.getField(1));
		assertEquals("0.38335283618300353 -1.3916523315682483 -0.31709372958050464", out.getField(2).toString());

	}

	@Test
	public void testSpaseDefault() throws Exception {
		geneSparse();
		Params params = new Params()
			.set(PcaPredictParams.PREDICTION_COL, "pred");

		PcaModelMapper mapper = new PcaModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(modelRows);

		Row in = Row.of(1, "0:0.1 1:0.2 2:0.3 3:0.4");
		Row out = mapper.map(in);

		assertEquals(in.getField(0), out.getField(0));
		assertEquals(in.getField(1), out.getField(1));
		assertEquals("0.38335283618300353 -1.3916523315682483 -0.31709372958050464", out.getField(2).toString());

	}

	@Test
	public void testSparseSameValue() throws Exception {
		geneColSame();
		Params params = new Params()
			.set(PcaPredictParams.PREDICTION_COL, "pred");

		PcaModelMapper mapper = new PcaModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(modelRows);

		Row in = Row.of(1, "0:0.1 1:0.2 2:0.3 3:0.4 4:0");
		Row out = mapper.map(in);

		assertEquals(in.getField(0), out.getField(0));
		assertEquals(in.getField(1), out.getField(1));
		assertEquals("0.38335283618300353 -1.3916523315682483 -0.31709372958050464", out.getField(2).toString());
	}

	@Test
	public void testTableDefault() throws Exception {
		geneTable();
		Params params = new Params()
			.set(PcaPredictParams.PREDICTION_COL, "pred");

		PcaModelMapper mapper = new PcaModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(modelRows);

		Row in = Row.of(1, 0.1, 0.2, 0.3, 0.4);
		Row out = mapper.map(in);

		assertEquals(in.getField(0), out.getField(0));
		assertEquals(in.getField(1), out.getField(1));
		assertEquals(in.getField(2), out.getField(2));
		assertEquals(in.getField(3), out.getField(3));
		assertEquals(in.getField(4), out.getField(4));
		assertEquals("0.38335283618300353 -1.3916523315682483 -0.31709372958050464", out.getField(5).toString());
	}

	private void genDense() {
		modelRows = Arrays.asList(Row.of(0L, "{\"vectorCol\":null,\"featureCols\":null}"),
			Row.of(1048576L,
				"{\"featureColNames\":null,\"vectorColName\":\"vec\",\"pcaType\":\"CORR\",\"nameX\":null,"
					+ "\"means\":[0.24000000000000005,0.18000000000000002,0.34,0.4999999999999999],"
					+ "\"stddevs\":[0.11401754250991379,0.08366600265340751,0.11401754250991372,0.14142135623730995],"
					+ "\"p\":3,\"lambda\":[2.578254995762849,1.1193647466973609,0.298935627147233],"
					+ "\"coef\":[[-0.03379834830303102,0.6157858399626491,0.5600763889470556,-0.5531545077982355],"
					+ "[0.9312433602630378,-0.0823424465307846,0.312683805538429,0.16803084008400654],"
					+ "[-0.29590042989993104,0.20683332154502362,0.5195734233774061,0.7744071089572797]],\"cov\":null,"
					+ "\"idxNonEqual\":[0,1,2,3],\"nx\":4}"));

		String[] colNames = new String[] {"id", "vec"};

		Object[][] dataRows = new Object[][] {
			{1, "0.1 0.2 0.3 0.4"},
		};

		MemSourceBatchOp data = new MemSourceBatchOp(dataRows, colNames);

		modelSchema = new PcaModelDataConverter().getModelSchema();
		dataSchema = data.getSchema();
	}

	private void geneTable() {
		modelRows = Arrays.asList(Row.of(0L, "{\"vectorCol\":null,\"featureCols\":null}"),
			Row.of(1048576L,
				"{\"featureColNames\":[\"f0\",\"f1\",\"f2\",\"f3\"],\"vectorColName\":null,\"pcaType\":\"CORR\","
					+ "\"nameX\":null,\"means\":[0.24000000000000005,0.18000000000000002,0.34,0.4999999999999999],"
					+ "\"stddevs\":[0.11401754250991379,0.08366600265340751,0.11401754250991372,0.14142135623730995],"
					+ "\"p\":3,\"lambda\":[2.578254995762849,1.1193647466973609,0.298935627147233],"
					+ "\"coef\":[[-0.03379834830303102,0.6157858399626491,0.5600763889470556,-0.5531545077982355],"
					+ "[0.9312433602630378,-0.0823424465307846,0.312683805538429,0.16803084008400654],"
					+ "[-0.29590042989993104,0.20683332154502362,0.5195734233774061,0.7744071089572797]],\"cov\":null,"
					+ "\"idxNonEqual\":[0,1,2,3],\"nx\":4}\n"));

		String[] colNames = new String[] {"id", "f0", "f1", "f2", "f3"};

		Object[][] dataRows = new Object[][] {
			{1, 0.1, 0.2, 0.3, 0.4}
		};

		MemSourceBatchOp data = new MemSourceBatchOp(dataRows, colNames);

		modelSchema = new PcaModelDataConverter().getModelSchema();
		dataSchema = data.getSchema();
	}

	private void geneSparse() {
		modelRows = Arrays.asList(Row.of(0L, "{\"vectorCol\":null,\"featureCols\":null}"),
			Row.of(1048576L,
				"{\"featureColNames\":null,\"vectorColName\":\"vec\",\"pcaType\":\"CORR\",\"nameX\":null,"
					+ "\"means\":[0.24000000000000005,0.18000000000000002,0.34,0.4999999999999999],"
					+ "\"stddevs\":[0.11401754250991379,0.08366600265340751,0.11401754250991372,0.14142135623730995],"
					+ "\"p\":3,\"lambda\":[2.578254995762849,1.1193647466973609,0.298935627147233],"
					+ "\"coef\":[[-0.03379834830303102,0.6157858399626491,0.5600763889470556,-0.5531545077982355],"
					+ "[0.9312433602630378,-0.0823424465307846,0.312683805538429,0.16803084008400654],"
					+ "[-0.29590042989993104,0.20683332154502362,0.5195734233774061,0.7744071089572797]],\"cov\":null,"
					+ "\"idxNonEqual\":[0,1,2,3],\"nx\":4}"));

		String[] colNames = new String[] {"id", "vec"};

		Object[][] dataRows = new Object[][] {
			{1, "0:0.1 1:0.2 2:0.3 3:0.4"}
		};

		MemSourceBatchOp data = new MemSourceBatchOp(dataRows, colNames);

		modelSchema = new PcaModelDataConverter().getModelSchema();
		dataSchema = data.getSchema();
	}

	private void geneColSame() {
		modelRows = Arrays.asList(Row.of(0L, "{\"vectorCol\":null,\"featureCols\":null}"),
			Row.of(1048576L,
				"{\"featureColNames\":null,\"vectorColName\":\"vec\",\"pcaType\":\"CORR\",\"nameX\":null,"
					+ "\"means\":[0.24000000000000005,0.18000000000000002,0.34,0.4999999999999999],"
					+ "\"stddevs\":[0.11401754250991379,0.08366600265340751,0.11401754250991372,0.14142135623730995],"
					+ "\"p\":3,\"lambda\":[2.578254995762849,1.1193647466973609,0.298935627147233],"
					+ "\"coef\":[[-0.03379834830303102,0.6157858399626491,0.5600763889470556,-0.5531545077982355],"
					+ "[0.9312433602630378,-0.0823424465307846,0.312683805538429,0.16803084008400654],"
					+ "[-0.29590042989993104,0.20683332154502362,0.5195734233774061,0.7744071089572797]],\"cov\":null,"
					+ "\"idxNonEqual\":[0,1,2,3],\"nx\":5}"));

		String[] colNames = new String[] {"id", "vec"};

		Object[][] dataRows = new Object[][] {
			{1, "0:0.1 1:0.2 2:0.3 3:0.4 4:0"}
		};

		MemSourceBatchOp data = new MemSourceBatchOp(dataRows, colNames);

		modelSchema = new PcaModelDataConverter().getModelSchema();
		dataSchema = data.getSchema();
	}

}