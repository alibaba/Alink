package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.common.feature.binning.BinTypes;
import com.alibaba.alink.params.feature.OneHotPredictParams;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for OneHotModelMapper.
 */
public class OneHotModelMapperTest {
	private Row[] rows = new Row[] {
		Row.of(-1L, "{\"selectedCols\":\"[\\\"docid\\\",\\\"word\\\",\\\"cnt\\\"]\"}", null),
		Row.of(1L, "人", 0L),
		Row.of(2L, "5", 0L),
		Row.of(2L, "2", 1L),
		Row.of(1L, "地", 1L),
		Row.of(1L, "色", 2L),
		Row.of(0L, "doc1", 0L),
		Row.of(1L, "一",3L),
		Row.of(1L, "合", 4L),
		Row.of(0L, "doc2", 1L),
		Row.of(2L, "4", 2L),
		Row.of(2L, "3", 3L),
		Row.of(0L, "doc0", 2L),
		Row.of(1L, "天", 5L),
		Row.of(1L, "清", 6L),
		Row.of(2L, "1", 4L)
	};

	private Row[] newRows = new Row[] {
		Row.of(-1L, "{\"selectedCols\":\"[\\\"docid\\\",\\\"word\\\",\\\"cnt\\\"]\","
			+ "\"enableElse\":\"false\"}", null),
		Row.of(1L, "人", 0L),
		Row.of(2L, "5", 0L),
		Row.of(2L, "2", 1L),
		Row.of(1L, "地", 1L),
		Row.of(1L, "色", 2L),
		Row.of(0L, "doc1", 0L),
		Row.of(1L, "一",3L),
		Row.of(1L, "合", 4L),
		Row.of(0L, "doc2", 1L),
		Row.of(2L, "4", 2L),
		Row.of(2L, "3", 3L),
		Row.of(0L, "doc0", 2L),
		Row.of(1L, "天", 5L),
		Row.of(1L, "清", 6L),
		Row.of(2L, "1", 4L)
	};

	private List <Row> model = Arrays.asList(rows);
	private List<Row> newModel = Arrays.asList(newRows);
	private TableSchema modelSchema = new OneHotModelDataConverter().getModelSchema();
	private TableSchema dataSchema = new TableSchema(
		new String[] {"docid", "word", "cnt"},
		new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.LONG}
	);

	private Row defaultRow = Row.of("doc0", "天", 4L);
	private Row nullElseRow = Row.of(null, "梅", 4L);

	@Test
	public void testOneHot() throws Exception {
		Params params = new Params()
			.set(HasOutputCols.OUTPUT_COLS, new String[]{"pred"})
			.set(OneHotPredictParams.RESERVED_COLS, new String[] {})
			.set(OneHotPredictParams.DROP_LAST, false);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(defaultRow).getField(0).toString(), "$21$2:1.0 10:1.0 16:1.0");
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"pred"},
			new TypeInformation <?>[] {VectorTypes.SPARSE_VECTOR}));

		mapper.loadModel(newModel);

		assertEquals(mapper.map(defaultRow).getField(0).toString(), "$18$2:1.0 9:1.0 14:1.0");
	}

	@Test
	public void testReserved() throws Exception {
		Params params = new Params()
			.set(HasOutputCols.OUTPUT_COLS, new String[]{"pred"})
			.set(OneHotPredictParams.DROP_LAST, false);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("doc0", "天", 4L)).getField(3).toString(), "$21$2:1.0 10:1.0 16:1.0");
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"docid", "word", "cnt", "pred"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.LONG, VectorTypes.SPARSE_VECTOR}));
	}

	@Test
	public void testIndex() throws Exception {
		Params params = new Params()
			.set(OneHotPredictParams.ENCODE, BinTypes.Encode.INDEX.name())
			.set(OneHotPredictParams.SELECTED_COLS, new String[]{"cnt", "word", "docid"});

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);
		assertEquals(mapper.map(defaultRow), Row.of(2L, 5L, 2L));
		assertEquals(mapper.map(nullElseRow), Row.of(3L, 8L, 2L));

		mapper.loadModel(newModel);
		assertEquals(mapper.map(defaultRow), Row.of(2L, 5L, 2L));
		assertEquals(mapper.map(nullElseRow), Row.of(3L, 7L, 2L));
	}

	@Test
	public void testVector() throws Exception {
		Params params = new Params()
			.set(OneHotPredictParams.ENCODE, BinTypes.Encode.VECTOR.name())
			.set(OneHotPredictParams.SELECTED_COLS, new String[]{"cnt", "word", "docid"})
			.set(OneHotPredictParams.DROP_LAST, false);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);
		assertEquals(mapper.map(defaultRow), Row.of(new SparseVector(5, new int[]{2}, new double[]{1.0}),
			new SparseVector(9, new int[]{5}, new double[]{1.0}),
			new SparseVector(7, new int[]{2}, new double[]{1.0})));
		assertEquals(mapper.map(nullElseRow), Row.of(new SparseVector(5, new int[]{3}, new double[]{1.0}),
			new SparseVector(9, new int[]{8}, new double[]{1.0}),
			new SparseVector(7, new int[]{2}, new double[]{1.0})));

		mapper.loadModel(newModel);
		assertEquals(mapper.map(defaultRow), Row.of(new SparseVector(4, new int[]{2}, new double[]{1.0}),
			new SparseVector(8, new int[]{5}, new double[]{1.0}),
			new SparseVector(6, new int[]{2}, new double[]{1.0})));
		assertEquals(mapper.map(nullElseRow), Row.of(new SparseVector(4, new int[]{3}, new double[]{1.0}),
			new SparseVector(8, new int[]{7}, new double[]{1.0}),
			new SparseVector(6, new int[]{2}, new double[]{1.0})));
	}

	@Test
	public void testDropLast() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"docid", "word", "cnt"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.LONG}
		);
		Params params = new Params()
			.set(OneHotPredictParams.ENCODE, BinTypes.Encode.VECTOR.name())
			.set(OneHotPredictParams.SELECTED_COLS, new String[]{"cnt", "word", "docid"})
			.set(OneHotPredictParams.DROP_LAST, true);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("doc0", "天", 4L)), Row.of(new SparseVector(4),
			new SparseVector(8, new int[]{5}, new double[]{1.0}),
			new SparseVector(6, new int[]{2}, new double[]{1.0})));
		assertEquals(mapper.map(nullElseRow), Row.of(
			new SparseVector(4, new int[]{2}, new double[]{1.0}),
			new SparseVector(8, new int[]{7}, new double[]{1.0}),
			new SparseVector(6, new int[]{2}, new double[]{1.0})));
	}

	@Test
	public void testDropLastAssembleVector() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"docid", "word", "cnt"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.LONG}
		);
		Params params = new Params()
			.set(HasOutputCols.OUTPUT_COLS, new String[]{"pred"})
			.set(OneHotPredictParams.RESERVED_COLS, new String[] {})
			.set(OneHotPredictParams.DROP_LAST, true);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("doc0", "天", 4L)).getField(0).toString(), "$18$9:1.0 14:1.0");
	}

	@Test
	public void testHandleInvalidVector() throws Exception{
		Params params = new Params()
			.set(OneHotPredictParams.ENCODE, BinTypes.Encode.VECTOR.name())
			.set(OneHotPredictParams.HANDLE_INVALID, "skip")
			.set(OneHotPredictParams.SELECTED_COLS, new String[]{"cnt", "word", "docid"})
			.set(OneHotPredictParams.DROP_LAST, false);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);
		assertEquals(mapper.map(nullElseRow), Row.of(null,
			new SparseVector(8, new int[]{7}, new double[]{1.0}),
			new SparseVector(6, new int[]{2}, new double[]{1.0})));

		mapper.loadModel(newModel);
		assertEquals(mapper.map(nullElseRow), Row.of(null, null,
			new SparseVector(5, new int[]{2}, new double[]{1.0})));
	}

	@Test
	public void testHandleInvalidError() throws Exception{
		Params params = new Params()
			.set(OneHotPredictParams.ENCODE, BinTypes.Encode.VECTOR.name())
			.set(OneHotPredictParams.HANDLE_INVALID, "error")
			.set(OneHotPredictParams.SELECTED_COLS, new String[]{"cnt", "word"})
			.set(OneHotPredictParams.DROP_LAST, false);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);
		assertEquals(mapper.map(nullElseRow), Row.of(null,
			new SparseVector(8, new int[]{7}, new double[]{1.0}),
			new SparseVector(6, new int[]{2}, new double[]{1.0})));

		mapper.loadModel(newModel);
		try {
			assertEquals(mapper.map(nullElseRow), Row.of(null, null,
				new SparseVector(5, new int[] {2}, new double[] {1.0})));
		}catch (Exception e){
			assertEquals(e.getMessage(), "Unseen token: 梅");
		}
	}
}
