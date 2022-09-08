package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.Row;

public class GraphTransformUtils {
	public static final class Edge2Row<T, M> implements MapFunction <Edge <T, M>, Row> {

		private static final long serialVersionUID = 3385288293299927018L;

		@Override
		public Row map(Edge <T, M> value) throws Exception {
			Row r = new Row(2);
			r.setField(0, value.f0);
			r.setField(1, value.f1);
			return r;
		}
	}

	public static class EdgeVal2Row<T, M> implements MapFunction <Edge <T, M>, Row> {

		private static final long serialVersionUID = -85177069939050780L;

		@Override
		public Row map(Edge <T, M> value) throws Exception {
			Row r = new Row(3);
			r.setField(0, value.f0);
			r.setField(1, value.f1);
			r.setField(2, value.f2);
			return r;
		}
	}

	public static final class MapVerticesTreeDepth implements MapFunction <Vertex <Long, NullValue>, Double> {
		private static final long serialVersionUID = -564758571385086986L;

		@Override
		public Double map(Vertex <Long, NullValue> value) throws Exception {
			return 1.;
		}
	}

	public static final class MapVertices<T> implements MapFunction <Vertex <String, T>, Row> {
		private static final long serialVersionUID = -7385474878437163213L;

		@Override
		public Row map(Vertex <String, T> value) throws Exception {
			Row r = new Row(2);
			r.setField(0, value.f0);
			r.setField(1, value.f1);
			return r;
		}
	}
}
