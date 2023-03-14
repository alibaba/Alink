package com.alibaba.alink.common.viz;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.utils.DataSetUtil;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.TreeModelDataConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

public class VizDataWriterForModelInfo {
	private final static Logger LOG = LoggerFactory.getLogger(VizDataWriterForModelInfo.class);

	static public void writeModelInfo(VizDataWriterInterface writer, String opName, TableSchema schema,
									  DataSet <Row> model, Params params) {
		VizOpMeta vizOpMeta = new VizOpMeta();

		vizOpMeta.opName = opName;

		vizOpMeta.dataInfos = new VizOpDataInfo[1];
		vizOpMeta.dataInfos[0] = new VizOpDataInfo(0, VizOpDataInfo.WriteVizDataType.OnlyOnce);

		vizOpMeta.cascades = new HashMap <>();
		vizOpMeta.cascades.put(gson.toJson(new String[] {"model"}), new VizOpChartData(0));

		vizOpMeta.setSchema(schema);
		vizOpMeta.params = params;
		vizOpMeta.isOutput = false;

		writer.writeBatchMeta(vizOpMeta);

		DataSet <String> result = model.mapPartition(new VizDataWriterMapperForTable(
			writer, 0, schema.getFieldNames(), schema.getFieldTypes()))
			.setParallelism(1);
		DataSetUtil.linkDummySink(result);
	}

	static public void writeTreeModelInfo(VizDataWriterInterface writer, String opName, TableSchema schema,
										  DataSet <Row> model, Params params) {
		TypeInformation <?>[] types = schema.getFieldTypes();
		TypeInformation <?> labelType = types[types.length - 1];
		DataSet <Row> processedModel = model
			.reduceGroup(new PruneTreeMapper(labelType));
		writeModelInfo(writer, opName, schema, processedModel, params);
	}

	private static void pruneTree(Node node, int depth, int maxDepthAllowed) {
		if (depth + 1 >= maxDepthAllowed) {
			if (node.getNextNodes() != null) {
				node.setNextNodes(new Node[] {});
			}
			return;
		}
		if (node.getNextNodes() == null) {
			return;
		}
		for (Node child : node.getNextNodes()) {
			pruneTree(child, depth + 1, maxDepthAllowed);
		}
	}

	public static class PruneTreeMapper extends RichGroupReduceFunction <Row, Row> {
		private static final long serialVersionUID = -4197833778656802284L;
		static int MAX_DEPTH_ALLOWED_FOR_VIZ = 14;
		private final TypeInformation <?> labelType;

		public PruneTreeMapper(TypeInformation <?> labelType) {
			this.labelType = labelType;
		}

		@Override
		public void reduce(Iterable <Row> values, Collector <Row> out) throws Exception {
			LOG.info("PruneTreeMapper start");
			TreeModelDataConverter model = new TreeModelDataConverter(this.labelType);

			List <Row> modelRows = new ArrayList <>();
			for (Row row : values) {
				modelRows.add(row);
			}
			model.load(modelRows);

			for (Node node : model.roots) {
				pruneTree(node, 0, MAX_DEPTH_ALLOWED_FOR_VIZ);
			}
			model.save(model, out);
			LOG.info("PruneTreeMapper end");
		}
	}
}
