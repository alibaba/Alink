package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.feature.HasEncodeWithoutWoe;
import com.alibaba.alink.params.feature.OneHotPredictParams;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for OneHotModelMapper.
 */

public class OneHotModelMapperTest extends AlinkTestBase {
	private Row[] rows = new Row[] {
		Row.of(-1L, "{\"selectedCols\":\"[\\\"docid\\\",\\\"word\\\",\\\"cnt\\\"]\"}", null),
		Row.of(1L, "人", 0L),
		Row.of(2L, "5", 0L),
		Row.of(2L, "2", 1L),
		Row.of(1L, "地", 1L),
		Row.of(1L, "色", 2L),
		Row.of(0L, "doc1", 0L),
		Row.of(1L, "一", 3L),
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
		Row.of(1L, "一", 3L),
		Row.of(1L, "合", 4L),
		Row.of(0L, "doc2", 1L),
		Row.of(2L, "4", 2L),
		Row.of(2L, "3", 3L),
		Row.of(0L, "doc0", 2L),
		Row.of(1L, "天", 5L),
		Row.of(1L, "清", 6L),
		Row.of(2L, "1", 4L)
	};

	private Row[] nullRows = new Row[] {
		Row.of(-1L, "{\"selectedCols\":\"[\\\"docid\\\",\\\"word\\\",\\\"cnt\\\"]\","
			+ "\"enableElse\":\"true\"}", null)
	};

	private List <Row> model = Arrays.asList(rows);
	private List <Row> newModel = Arrays.asList(newRows);
	private List <Row> nullModel = Arrays.asList(nullRows);
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
			.set(HasOutputCols.OUTPUT_COLS, new String[] {"pred"})
			.set(OneHotPredictParams.RESERVED_COLS, new String[] {})
			.set(OneHotPredictParams.DROP_LAST, false);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(defaultRow).getField(0),
			new SparseVector(21, new int[] {2, 10, 16}, new double[] {1.0, 1.0, 1.0}));
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"pred"},
			new TypeInformation <?>[] {AlinkTypes.SPARSE_VECTOR}));

		mapper.loadModel(newModel);

		assertEquals(mapper.map(defaultRow).getField(0),
			new SparseVector(18, new int[] {2, 9, 14}, new double[] {1.0, 1.0, 1.0}));
	}

	@Test
	public void testReserved() throws Exception {
		Params params = new Params()
			.set(HasOutputCols.OUTPUT_COLS, new String[] {"pred"})
			.set(OneHotPredictParams.DROP_LAST, false);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("doc0", "天", 4L)).getField(3),
			new SparseVector(21, new int[] {2, 10, 16}, new double[] {1.0, 1.0, 1.0}));
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"docid", "word", "cnt", "pred"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.LONG, AlinkTypes.SPARSE_VECTOR}));
	}

	@Test
	public void testIndex() throws Exception {
		Params params = new Params()
			.set(OneHotPredictParams.ENCODE, HasEncodeWithoutWoe.Encode.INDEX)
			.set(OneHotPredictParams.SELECTED_COLS, new String[] {"cnt", "word", "docid"});

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
			.set(OneHotPredictParams.ENCODE, HasEncodeWithoutWoe.Encode.VECTOR)
			.set(OneHotPredictParams.SELECTED_COLS, new String[] {"cnt", "word", "docid"})
			.set(OneHotPredictParams.DROP_LAST, false);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);
		assertEquals(mapper.map(defaultRow), Row.of(new SparseVector(5, new int[] {2}, new double[] {1.0}),
			new SparseVector(9, new int[] {5}, new double[] {1.0}),
			new SparseVector(7, new int[] {2}, new double[] {1.0})));
		assertEquals(mapper.map(nullElseRow), Row.of(new SparseVector(5, new int[] {3}, new double[] {1.0}),
			new SparseVector(9, new int[] {8}, new double[] {1.0}),
			new SparseVector(7, new int[] {2}, new double[] {1.0})));

		mapper.loadModel(newModel);
		assertEquals(mapper.map(defaultRow), Row.of(new SparseVector(4, new int[] {2}, new double[] {1.0}),
			new SparseVector(8, new int[] {5}, new double[] {1.0}),
			new SparseVector(6, new int[] {2}, new double[] {1.0})));
		assertEquals(mapper.map(nullElseRow), Row.of(new SparseVector(4, new int[] {3}, new double[] {1.0}),
			new SparseVector(8, new int[] {7}, new double[] {1.0}),
			new SparseVector(6, new int[] {2}, new double[] {1.0})));
	}

	@Test
	public void testDropLast() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"docid", "word", "cnt"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.LONG}
		);
		Params params = new Params()
			.set(OneHotPredictParams.ENCODE, HasEncodeWithoutWoe.Encode.VECTOR)
			.set(OneHotPredictParams.SELECTED_COLS, new String[] {"cnt", "word", "docid"})
			.set(OneHotPredictParams.DROP_LAST, true);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("doc0", "天", 4L)), Row.of(new SparseVector(4),
			new SparseVector(8, new int[] {5}, new double[] {1.0}),
			new SparseVector(6, new int[] {2}, new double[] {1.0})));
		assertEquals(mapper.map(nullElseRow), Row.of(
			new SparseVector(4, new int[] {2}, new double[] {1.0}),
			new SparseVector(8, new int[] {7}, new double[] {1.0}),
			new SparseVector(6, new int[] {2}, new double[] {1.0})));
	}

	@Test
	public void testDropLastAssembleVector() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"docid", "word", "cnt"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.LONG}
		);
		Params params = new Params()
			.set(HasOutputCols.OUTPUT_COLS, new String[] {"pred"})
			.set(OneHotPredictParams.RESERVED_COLS, new String[] {})
			.set(OneHotPredictParams.DROP_LAST, true);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("doc0", "天", 4L)).getField(0),
			new SparseVector(18, new int[] {9, 14}, new double[] {1.0, 1.0}));
	}

	@Test
	public void testHandleInvalidVector() throws Exception {
		Params params = new Params()
			.set(OneHotPredictParams.ENCODE, HasEncodeWithoutWoe.Encode.VECTOR)
			.set(OneHotPredictParams.HANDLE_INVALID, HasHandleInvalid.HandleInvalid.SKIP)
			.set(OneHotPredictParams.SELECTED_COLS, new String[] {"cnt", "word", "docid"})
			.set(OneHotPredictParams.DROP_LAST, false);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);
		assertEquals(mapper.map(nullElseRow), Row.of(null,
			new SparseVector(8, new int[] {7}, new double[] {1.0}),
			new SparseVector(6, new int[] {2}, new double[] {1.0})));

		mapper.loadModel(newModel);
		assertEquals(mapper.map(nullElseRow), Row.of(null, null,
			new SparseVector(5, new int[] {2}, new double[] {1.0})));
	}

	@Test
	public void testHandleInvalidError() throws Exception {
		Params params = new Params()
			.set(OneHotPredictParams.ENCODE, HasEncodeWithoutWoe.Encode.VECTOR)
			.set(OneHotPredictParams.HANDLE_INVALID, HasHandleInvalid.HandleInvalid.ERROR)
			.set(OneHotPredictParams.SELECTED_COLS, new String[] {"cnt", "word"})
			.set(OneHotPredictParams.DROP_LAST, false);

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);
		assertEquals(mapper.map(nullElseRow), Row.of(null,
			new SparseVector(8, new int[] {7}, new double[] {1.0}),
			new SparseVector(6, new int[] {2}, new double[] {1.0})));

		mapper.loadModel(newModel);
		try {
			assertEquals(mapper.map(nullElseRow), Row.of(null, null,
				new SparseVector(5, new int[] {2}, new double[] {1.0})));
		} catch (Exception e) {
			assertEquals(e.getMessage(), "Unseen token: 梅");
		}
	}

	@Test
	public void testNullModel() throws Exception {
		Params params = new Params()
			.set(OneHotPredictParams.ENCODE, HasEncodeWithoutWoe.Encode.VECTOR)
			.set(OneHotPredictParams.SELECTED_COLS, new String[] {"cnt", "word", "docid"});

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(nullModel);

		Assert.assertEquals(mapper.map(defaultRow), Row.of(
			new SparseVector(2, new int[] {1}, new double[] {1.0}),
			new SparseVector(2, new int[] {1}, new double[] {1.0}),
			new SparseVector(2, new int[] {1}, new double[] {1.0})
		));
	}
}
