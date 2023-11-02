package com.alibaba.alink.pipeline.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.feature.binning.BinDivideType;
import com.alibaba.alink.params.feature.HasEncode.Encode;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class BinningTest extends AlinkTestBase {
	private Row[] rows =
		new Row[] {
			Row.of("a", true, 1, 1.1, 1),
			Row.of("b", true, -2, 0.9, 1),
			Row.of("c", null, 100, 0.01, 1),
			Row.of("e", false, -99, -100.9, 2),
			Row.of("a", null, 1, 1.1, 2),
			Row.of("b", true, 2, -0.9, 1),
			Row.of("c", true, 99, -0.01, 2),
			Row.of("d", false, -99, 100.9, 2),
			Row.of(null, false, -1, -1.1, 1)
		};

	private String[] colnames = new String[] {"col1", "col2", "col3", "col4", "label"};

	@Test
	public void test() throws Exception {
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, colnames);

		Binning op = new Binning()
			.setNumBuckets(3)
			.setBinningMethod(BinDivideType.QUANTILE.name())
			.setLabelCol("label")
			.setPositiveLabelValueString("1")
			.setSelectedCols("col1", "col2", "col3", "col4")
			.setLeftOpen(false)
			.setEncode(Encode.INDEX.name())
			.setDiscreteThresholds(2);

		PipelineModel model = new Pipeline().add(op).fit(data);

		Table res = model.transform(data);

		new TableSourceBatchOp(res).print();

	}

	@Test
	public void test3() throws Exception {
		String[] binaryNames = new String[] {"docid", "word", "cnt"};
		TableSchema schema = new TableSchema(
			new String[] {"id", "docid", "word", "cnt"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.STRING, Types.LONG}
		);

		Row[] array = new Row[] {
			Row.of("0", "doc0", "天", 4L),
			Row.of("1", "doc0", "地", 5L),
			Row.of("2", "doc0", "人", 1L),
			Row.of("3", "doc1", null, 3L),
			Row.of("4", null, "人", 2L),
			Row.of("5", "doc1", "合", 4L),
			Row.of("6", "doc1", "一", 4L),
			Row.of("7", "doc2", "清", 3L),
			Row.of("8", "doc2", "一", 2L),
			Row.of("9", "doc2", "色", 2L)
		};

		BatchOperator batchSource = new MemSourceBatchOp(Arrays.asList(array), schema);

		Binning oneHot = new Binning()
			.setSelectedCols(binaryNames)
			.setOutputCols("results")
			.setEncode(Encode.ASSEMBLED_VECTOR)
			.setDropLast(false);

		VectorAssembler va = new VectorAssembler()
			.setSelectedCols(new String[] {"cnt", "results"})
			.enableLazyPrintTransformData(10, "xxxxxx")
			.setOutputCol("outN");

		VectorAssembler va2 = new VectorAssembler()
			.setSelectedCols(new String[] {"cnt", "results"})
			.setOutputCol("outN");

		VectorAssembler va3 = new VectorAssembler()
			.setSelectedCols(new String[] {"cnt", "results"})
			.setOutputCol("outN");

		VectorAssembler va4 = new VectorAssembler()
			.setSelectedCols(new String[] {"cnt", "results"})
			.enableLazyPrintTransformStat("xxxxxx4")
			.setOutputCol("outN");

		Pipeline pl = new Pipeline().add(oneHot).add(va).add(va2).add(va3).add(va4);

		PipelineModel model = pl.fit(batchSource);

		Row[] parray = new Row[] {
			Row.of("0", "doc0", "天", 4L),
			Row.of("1", "doc2", null, 3L)
		};

		// batch predict
		MemSourceBatchOp predData = new MemSourceBatchOp(Arrays.asList(parray), schema);
		BatchOperator result = model.transform(predData).select(new String[] {"docid", "outN"});
		result.print();
	}
}