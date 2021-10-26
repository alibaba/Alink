package com.alibaba.alink.server.service.impl;

import com.alibaba.alink.server.domain.Edge;
import com.alibaba.alink.server.domain.Node;
import com.alibaba.alink.server.domain.NodeParam;
import com.alibaba.alink.server.service.ExecutionService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.util.Arrays;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest()
class EmbedExecutionServiceImplTest {

	@Autowired
	@Qualifier("localEnv")
	ExecutionService executionService;

	Node makeNode(Long id, String className) {
		return new Node().setId(id).setClassName(className);
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
	public void testBatchSourceSink() throws Exception {
		File file = File.createTempFile("test_", ".csv");
		List <Node> nodes = Arrays.asList(
			makeNode(1L, "com.alibaba.alink.operator.batch.source.CsvSourceBatchOp"),
			makeNode(2L, "com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp"),
			makeNode(8L, "com.alibaba.alink.operator.batch.feature.BinarizerBatchOp")
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
			makeNodeParam(2L, "filePath",
				String.format("\"%s\"", file.getAbsolutePath())),
			makeNodeParam(2L, "overwriteSink", "true"),
			makeNodeParam(8L, "selectedCol", "\"sepal_width\""),
			makeNodeParam(8L, "threshold", "3.5")
		);
		executionService.run(nodes, edges, nodeParams);
		Assertions.assertTrue(file.exists() && file.length() > 0);
	}

	@Test
	public void testStreamSourceSink() throws Exception {
		File file = File.createTempFile("test_", ".csv");
		List <Node> nodes = Arrays.asList(
			makeNode(1L, "com.alibaba.alink.operator.stream.source.CsvSourceStreamOp"),
			makeNode(2L, "com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp"),
			makeNode(8L, "com.alibaba.alink.operator.stream.feature.BinarizerStreamOp")
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
			makeNodeParam(2L, "filePath",
				String.format("\"%s\"", file.getAbsolutePath())),
			makeNodeParam(2L, "overwriteSink", "true"),
			makeNodeParam(8L, "selectedCol", "\"sepal_width\""),
			makeNodeParam(8L, "threshold", "3.5")
		);
		executionService.run(nodes, edges, nodeParams);
		Assertions.assertTrue(file.exists() && file.length() > 0);
	}

	@Test
	public void testBatchSourceSinkAndStreamSourceSink() throws Exception {
		File batchSinkFile = File.createTempFile("test_", ".csv");
		File streamSinkFile = File.createTempFile("test_", ".csv");
		List <Node> nodes = Arrays.asList(
			makeNode(1L, "com.alibaba.alink.operator.batch.source.CsvSourceBatchOp"),
			makeNode(2L, "com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp"),
			makeNode(8L, "com.alibaba.alink.operator.batch.feature.BinarizerBatchOp"),

			makeNode(11L, "com.alibaba.alink.operator.stream.source.CsvSourceStreamOp"),
			makeNode(12L, "com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp"),
			makeNode(18L, "com.alibaba.alink.operator.stream.feature.BinarizerStreamOp")
		);
		List <Edge> edges = Arrays.asList(
			makeEdge(1L, (short) 0, 8L, (short) 0),
			makeEdge(8L, (short) 0, 2L, (short) 0),
			makeEdge(11L, (short) 0, 18L, (short) 0),
			makeEdge(18L, (short) 0, 12L, (short) 0)
		);
		List <NodeParam> nodeParams = Arrays.asList(
			makeNodeParam(1L, "filePath",
				"\"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv\""),
			makeNodeParam(1L, "schemaStr",
				"\"sepal_length double, sepal_width double, petal_length double, petal_width double, category string\""),
			makeNodeParam(2L, "filePath",
				String.format("\"%s\"", batchSinkFile.getAbsolutePath())),
			makeNodeParam(2L, "overwriteSink", "true"),
			makeNodeParam(8L, "selectedCol", "\"sepal_width\""),
			makeNodeParam(8L, "threshold", "3.5"),

			makeNodeParam(11L, "filePath",
				"\"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv\""),
			makeNodeParam(11L, "schemaStr",
				"\"sepal_length double, sepal_width double, petal_length double, petal_width double, category string\""),
			makeNodeParam(12L, "filePath",
				String.format("\"%s\"", streamSinkFile.getAbsolutePath())),
			makeNodeParam(12L, "overwriteSink", "true"),
			makeNodeParam(18L, "selectedCol", "sepal_width"),
			makeNodeParam(18L, "threshold", "3.5")
		);
		executionService.run(nodes, edges, nodeParams);
		Assertions.assertTrue(batchSinkFile.exists() && batchSinkFile.length() > 0);
		Assertions.assertTrue(streamSinkFile.exists() && streamSinkFile.length() > 0);
	}

	@Test
	public void testPortOrder() throws Exception {
		List <Node> nodes = Arrays.asList(
			makeNode(1L, "com.alibaba.alink.operator.batch.source.CsvSourceBatchOp"),
			makeNode(2L, "com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp"),
			makeNode(8L, "com.alibaba.alink.operator.batch.feature.BinarizerBatchOp"),
			makeNode(13L, "com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerTrainBatchOp"),
			makeNode(14L, "com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerPredictBatchOp")
		);
		List <Edge> edges = Arrays.asList(
			makeEdge(1L, (short) 0, 8L, (short) 0),
			makeEdge(8L, (short) 0, 13L, (short) 0),
			makeEdge(8L, (short) 0, 14L, (short) 1),
			makeEdge(13L, (short) 0, 14L, (short) 0),
			makeEdge(14L, (short) 0, 2L, (short) 0)
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
			makeNodeParam(14L, "selectedCols", "[\"sepal_length\"]")
		);
		executionService.run(nodes, edges, nodeParams);
	}

	@Test
	public void testBatchTrainAndBatchPredict() throws Exception {
		List <Node> nodes = Arrays.asList(
			makeNode(1L, "com.alibaba.alink.operator.batch.source.CsvSourceBatchOp"),
			makeNode(2L, "com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp"),
			makeNode(8L, "com.alibaba.alink.operator.batch.feature.BinarizerBatchOp"),
			makeNode(13L, "com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerTrainBatchOp"),
			makeNode(14L, "com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerPredictBatchOp")
		);
		List <Edge> edges = Arrays.asList(
			makeEdge(1L, (short) 0, 8L, (short) 0),
			makeEdge(8L, (short) 0, 13L, (short) 0),
			makeEdge(13L, (short) 0, 14L, (short) 0),
			makeEdge(8L, (short) 0, 14L, (short) 1),
			makeEdge(14L, (short) 0, 2L, (short) 0)
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
			makeNodeParam(14L, "selectedCols", "[\"sepal_length\"]")
		);
		executionService.run(nodes, edges, nodeParams);
	}

	@Test
	public void testBatchTrainAndStreamPredict() throws Exception {
		List <Node> nodes = Arrays.asList(
			makeNode(1L, "com.alibaba.alink.operator.batch.source.CsvSourceBatchOp"),
			makeNode(2L, "com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp"),
			makeNode(8L, "com.alibaba.alink.operator.stream.feature.BinarizerStreamOp"),
			makeNode(13L, "com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerTrainBatchOp"),
			makeNode(14L, "com.alibaba.alink.operator.stream.feature.EqualWidthDiscretizerPredictStreamOp"),
			makeNode(16L, "com.alibaba.alink.operator.stream.source.CsvSourceStreamOp")
		);
		List <Edge> edges = Arrays.asList(
			makeEdge(16L, (short) 0, 8L, (short) 0),
			makeEdge(1L, (short) 0, 13L, (short) 0),
			makeEdge(13L, (short) 0, 14L, (short) 0),
			makeEdge(8L, (short) 0, 14L, (short) 1),
			makeEdge(14L, (short) 0, 2L, (short) 0)
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
				"\"sepal_length double, sepal_width double, petal_length double, petal_width double, category string\"")
		);
		executionService.run(nodes, edges, nodeParams);
	}

	@Test
	public void testBatchTrainAndStreamPredictAndWithBatchSink() throws Exception {
		List <Node> nodes = Arrays.asList(
			makeNode(1L, "com.alibaba.alink.operator.batch.source.CsvSourceBatchOp"),
			makeNode(2L, "com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp"),
			makeNode(8L, "com.alibaba.alink.operator.stream.feature.BinarizerStreamOp"),
			makeNode(13L, "com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerTrainBatchOp"),
			makeNode(14L, "com.alibaba.alink.operator.stream.feature.EqualWidthDiscretizerPredictStreamOp"),
			makeNode(16L, "com.alibaba.alink.operator.stream.source.CsvSourceStreamOp"),
			makeNode(17L, "com.alibaba.alink.operator.batch.sink.AkSinkBatchOp")
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
		executionService.run(nodes, edges, nodeParams);
	}
}
