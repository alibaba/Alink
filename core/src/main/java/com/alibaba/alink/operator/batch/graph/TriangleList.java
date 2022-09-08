package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.library.clustering.directed.TriangleListing;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

/**
 * Return the numbers of triangles of the input graph.
 *
 * @author qingzhao
 */
public class TriangleList {
	public static DataSet <Tuple3 <Long, Long, Long>> run(Graph <Long, Double, Double> graph) throws Exception {
		DataSet <TriangleListing.Result <LongValue>> inn = graph
			.translateGraphIds(new LongToLongValue())
			.translateVertexValues(new DoubleToDoubleValue())
			.translateEdgeValues(new DoubleToDoubleValue())
			.run(new TriangleListing <>());
		return inn.flatMap(new FlatMapOut());
	}

	public static class LongToLongValue implements TranslateFunction <Long, LongValue> {
		private static final long serialVersionUID = -8328414272957924783L;

		@Override
		public LongValue translate(Long value, LongValue reuse) {
			return new LongValue(value);
		}
	}

	public static class DoubleToDoubleValue implements TranslateFunction <Double, DoubleValue> {
		private static final long serialVersionUID = -1396593185269273081L;

		@Override
		public DoubleValue translate(Double value, DoubleValue reuse) {
			return new DoubleValue(value);
		}
	}

	public static class FlatMapOut
		implements FlatMapFunction <TriangleListing.Result <LongValue>, Tuple3 <Long, Long, Long>> {
		private static final long serialVersionUID = -7744611721605759120L;

		@Override
		public void flatMap(TriangleListing.Result <LongValue> value, Collector <Tuple3 <Long, Long, Long>> out) {
			Tuple3 <Long, Long, Long> temp = new Tuple3 <>();
			temp.f0 = value.getVertexId0().getValue();
			temp.f1 = value.getVertexId1().getValue();
			temp.f2 = value.getVertexId2().getValue();
			out.collect(temp);
		}
	}
}
