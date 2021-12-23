package com.alibaba.alink.server.common;

import com.alibaba.alink.server.domain.Edge;
import com.alibaba.alink.server.domain.Node;
import com.alibaba.alink.server.domain.NodeParam;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

class ExportScriptUtilsTest {

	Node makeNode(Long id, String name, String className) {
		return new Node().setId(id).setName(name).setClassName(className);
	}

	Edge makeEdge(Long srcNodeId, Short srcNodePort, Long dstNodeId, Short dstNodePort) {
		return new Edge()
			.setSrcNodeId(srcNodeId).setSrcNodePort(srcNodePort)
			.setDstNodeId(dstNodeId).setDstNodePort(dstNodePort);
	}

	NodeParam makeNodeParam(Long nodeId, String key, String value) {
		return new NodeParam().setNodeId(nodeId)
			.setKey(key)
			.setValue(value);
	}

	@Test
	public void testGenerateOpName() {
		String name = ExportScriptUtils.generateOpName("CSVSource", Collections.emptySet());
		Assertions.assertEquals(name, "CSVSource");
	}

	@Test
	public void testGenerateOpNameConflict() {
		String name = ExportScriptUtils.generateOpName("CSVSource", new HashSet <>(Arrays.asList("CSVSource")));
		Assertions.assertEquals(name, "CSVSource1");
	}

	@Test
	public void testExportBatchSourceSink() throws Exception {
		List <Node> nodes = Arrays.asList(
			makeNode(1L, "source", "com.alibaba.alink.operator.batch.source.CsvSourceBatchOp"),
			makeNode(2L, "sink", "com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp"),
			makeNode(8L, "binarizer", "com.alibaba.alink.operator.batch.feature.BinarizerBatchOp")
		);
		List <Edge> edges = Arrays.asList(
			makeEdge(1L, (short) 0, 8L, (short) 0),
			makeEdge(8L, (short) 0, 2L, (short) 0)
		);
		List <NodeParam> nodeParams = Arrays.asList(
			makeNodeParam(1L, "filePath",
				"\"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv\""),
			makeNodeParam(1L, "schemaStr",
				"\"sepal_length double, sepal_width double, petal_length double, petal_width double, category string\""),
			makeNodeParam(2L, "filePath", "\"/tmp/test_export.csv\""),
			makeNodeParam(2L, "overwriteSink", "true"),
			makeNodeParam(8L, "selectedCol", "\"sepal_width\""),
			makeNodeParam(8L, "threshold", "3.5")
		);
		List <String> lines = ExportScriptUtils.generateDAGScript(nodes, edges, nodeParams);
		List <String> expected = Arrays.asList(
			"source = CsvSourceBatchOp() \\",
			"    .setFilePath(\"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv\") \\",
			"    .setSchemaStr(\"sepal_length double, sepal_width double, petal_length double, petal_width double, category string\")",
			"binarizer = BinarizerBatchOp() \\",
			"    .setSelectedCol(\"sepal_width\") \\",
			"    .setThreshold(3.5) \\",
			"    .linkFrom(source)",
			"sink = CsvSinkBatchOp() \\",
			"    .setFilePath(\"/tmp/test_export.csv\") \\",
			"    .setOverwriteSink(True) \\",
			"    .linkFrom(binarizer)",
			"BatchOperator.execute()"
		);
		Assertions.assertLinesMatch(expected, lines);
	}

	@Test
	public void testExportBatchTrainAndStreamPredictAndWithBatchSink() throws Exception {
		List <Node> nodes = Arrays.asList(
			makeNode(1L, "source", "com.alibaba.alink.operator.batch.source.CsvSourceBatchOp"),
			makeNode(2L, "sink", "com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp"),
			makeNode(8L, "binarizer", "com.alibaba.alink.operator.stream.feature.BinarizerStreamOp"),
			makeNode(13L, "discretizer_train",
				"com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerTrainBatchOp"),
			makeNode(14L, "discretizer_predict",
				"com.alibaba.alink.operator.stream.feature.EqualWidthDiscretizerPredictStreamOp"),
			makeNode(16L, "source", "com.alibaba.alink.operator.stream.source.CsvSourceStreamOp"),
			makeNode(17L, "sink", "com.alibaba.alink.operator.batch.sink.AkSinkBatchOp")
		);
		List <Edge> edges = Arrays.asList(
			makeEdge(16L, (short) 0, 8L, (short) 0),
			makeEdge(1L, (short) 0, 13L, (short) 0),
			makeEdge(13L, (short) 0, 14L, (short) 0),
			makeEdge(8L, (short) 0, 14L, (short) 1),
			makeEdge(14L, (short) 0, 2L, (short) 0),
			makeEdge(13L, (short) 0, 17L, (short) 0)
		);
		List <NodeParam> nodeParams = Arrays.asList(
			makeNodeParam(1L, "filePath",
				"\"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv\""),
			makeNodeParam(1L, "schemaStr",
				"\"sepal_length double, sepal_width double, petal_length double, petal_width double, category string\""),
			makeNodeParam(2L, "filePath", "\"/tmp/test_write.csv\""),
			makeNodeParam(2L, "overwriteSink", "true"),
			makeNodeParam(8L, "selectedCol", "\"sepal_width\""),
			makeNodeParam(8L, "threshold", "3.5"),
			makeNodeParam(13L, "selectedCols", "[\"sepal_length\"]"),
			makeNodeParam(13L, "numBuckets", "10"),
			makeNodeParam(14L, "selectedCols", "[\"sepal_length\"]"),
			makeNodeParam(16L, "filePath",
				"\"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv\""),
			makeNodeParam(16L, "schemaStr",
				"\"sepal_length double, sepal_width double, petal_length double, petal_width double, category string\""),
			makeNodeParam(17L, "filePath", "\"/tmp/test_model_write.csv\""),
			makeNodeParam(17L, "overwriteSink", "true")
		);
		List <String> lines = ExportScriptUtils.generateDAGScript(nodes, edges, nodeParams);
		List <String> expected = Arrays.asList(
			"source = CsvSourceStreamOp() \\",
			"    .setFilePath(\"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv\") \\",
			"    .setSchemaStr(\"sepal_length double, sepal_width double, petal_length double, petal_width double, category string\")",
			"source1 = CsvSourceBatchOp() \\",
			"    .setFilePath(\"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv\") \\",
			"    .setSchemaStr(\"sepal_length double, sepal_width double, petal_length double, petal_width double, category string\")",
			"binarizer = BinarizerStreamOp() \\",
			"    .setSelectedCol(\"sepal_width\") \\",
			"    .setThreshold(3.5) \\",
			"    .linkFrom(source)",
			"discretizer_train = EqualWidthDiscretizerTrainBatchOp() \\",
			"    .setSelectedCols([\"sepal_length\"]) \\",
			"    .setNumBuckets(10) \\",
			"    .linkFrom(source1)",
			"discretizer_predict = EqualWidthDiscretizerPredictStreamOp(discretizer_train) \\",
			"    .setSelectedCols([\"sepal_length\"]) \\",
			"    .linkFrom(binarizer)",
			"sink = AkSinkBatchOp() \\",
			"    .setFilePath(\"/tmp/test_model_write.csv\") \\",
			"    .setOverwriteSink(True) \\",
			"    .linkFrom(discretizer_train)",
			"sink1 = CsvSinkStreamOp() \\",
			"    .setFilePath(\"/tmp/test_write.csv\") \\",
			"    .setOverwriteSink(True) \\",
			"    .linkFrom(discretizer_predict)",
			"try:",
			"    BatchOperator.execute()",
			"except Exception as ex:",
			"    if 'No new data sinks have been defined since the last execution.' not in ex.java_exception.getMessage():",
			"        raise ex",
			"StreamOperator.execute()"
		);
		Assertions.assertLinesMatch(expected, lines);
	}

	@Test
	public void testGenerateExecuteScriptOnlyStream() {
		List <String> lines = ExportScriptUtils.generateExecuteScript(true, false);
		Assertions.assertLinesMatch(Arrays.asList(
			"StreamOperator.execute()"
		), lines);
	}

	@Test
	public void testGenerateExecuteScriptOnlyBatch() {
		List <String> lines = ExportScriptUtils.generateExecuteScript(false, false);
		Assertions.assertLinesMatch(Arrays.asList(
			"BatchOperator.execute()"
		), lines);
	}

	@Test
	public void testGenerateExecuteScriptStreamOpAndBatchSink() {
		List <String> lines = ExportScriptUtils.generateExecuteScript(true, true);
		for (String line : lines) {
			System.out.println(line);
		}
		Assertions.assertLinesMatch(Arrays.asList(
			"try:",
			"    BatchOperator.execute()",
			"except Exception as ex:",
			"    if 'No new data sinks have been defined since the last execution.' not in ex.java_exception.getMessage():",
			"        raise ex",
			"StreamOperator.execute()"
		), lines);
	}
}
