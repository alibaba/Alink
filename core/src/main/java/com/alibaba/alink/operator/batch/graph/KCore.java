package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * This algorithm iteratively delete all vertices whose degree is not larger than k ,so that it will select
 * a graph whose vertices all have degrees larger than k.
 * Dealing with undirectedGraph, we groupby the edge dataset so that we can get the degree of vertices
 * through the numbers of vertices appear in the field 0 or 1 in the dataset, and iteratively delete
 * edges denoting small degree until the remaining dataset meets the requirement.
 *
 * @author qingzhao
 */
public class KCore {
	public static int k;
	public int maxIter;

	/**
	 * @param k       Remove all vertices with degree not larger than k.
	 * @param maxIter The maximum number of iterations to run.
	 */
	public KCore(int k, int maxIter) {
		this.k = k;
		this.maxIter = maxIter;
	}

	public DataSet <Edge <Long, Double>> run(DataSet <Edge <Long, Double>> edges, Boolean directed) {
		DataSet <Tuple5 <Long, Long, Long, Long, Double>> initialState;
		if (directed) {
			initialState = edges.flatMap(new FlatMapFunction
				<Edge <Long, Double>, Tuple5 <Long, Long, Long, Long, Double>>() {
				private static final long serialVersionUID = 3415098877090917677L;

				@Override
				public void flatMap(Edge <Long, Double> value,
									Collector <Tuple5 <Long, Long, Long, Long, Double>> out) {
					Tuple5 <Long, Long, Long, Long, Double> res = new Tuple5 <Long, Long, Long, Long, Double>();
					res.f0 = value.f0;
					res.f1 = value.f1;
					res.f2 = -1L;
					res.f3 = -1L;
					res.f4 = 0.;
					out.collect(res);
					res.f0 = value.f1;
					res.f1 = value.f0;
					out.collect(res);
				}
			});
		} else {
			initialState = edges.flatMap(new FlatMapFunction
				<Edge <Long, Double>, Tuple5 <Long, Long, Long, Long, Double>>() {
				private static final long serialVersionUID = -1356257363097879387L;

				@Override
				public void flatMap(Edge <Long, Double> value,
									Collector <Tuple5 <Long, Long, Long, Long, Double>> out) {
					Tuple5 <Long, Long, Long, Long, Double> res = new Tuple5 <Long, Long, Long, Long, Double>();
					res.f0 = value.f0;
					res.f1 = value.f1;
					res.f2 = -1L;
					res.f3 = -1L;
					res.f4 = 0.;
					out.collect(res);
				}
			});
		}
		DataSet <Tuple5 <Long, Long, Long, Long, Double>> outState = operation(initialState);
		return outState.map(new MapFunction <Tuple5 <Long, Long, Long, Long, Double>, Edge <Long, Double>>() {
			private static final long serialVersionUID = -1652848589684719913L;

			@Override
			public Edge <Long, Double> map(Tuple5 <Long, Long, Long, Long, Double> value) throws Exception {
				return new Edge <Long, Double>(value.f0, value.f1, 1.);
			}
		});
	}

	public DataSet <Tuple5 <Long, Long, Long, Long, Double>> operation(
		DataSet <Tuple5 <Long, Long, Long, Long, Double>> initialState) {
		IterativeDataSet <Tuple5 <Long, Long, Long, Long, Double>> state = initialState
			.iterate(this.maxIter);
		//Count numbers with field 0, and then filter edges denoting small degree.
		DataSet <Tuple5 <Long, Long, Long, Long, Double>> secondState = state
			.groupBy(0)
			.reduceGroup(new ReduceOnFirstField())
			.filter(new FilterSmallOnesOnFirstField(k)).name("firstStep");
		//If there is no vertices with small degree, then break the iteration.
		DataSet <Tuple5 <Long, Long, Long, Long, Double>> thirdState = secondState
			.groupBy(1)
			.reduceGroup(new ReduceOnSecondField()).name("secondStep");
		//seems fussy, but may not avoid
		DataSet <Tuple5 <Long, Long, Long, Long, Double>> outState = state
			.closeWith(
				thirdState.filter(new FilterSmallOnesOnSecondField(k)).name("filterSmallOne"),
				thirdState.filter(new FilterLargeOnesOnThiState(k)).name("filterLargeOne"));

		return outState;
	}

	public static class ReduceOnFirstField
		implements
		GroupReduceFunction <Tuple5 <Long, Long, Long, Long, Double>, Tuple5 <Long, Long, Long, Long, Double>> {
		private static final long serialVersionUID = 263920722211539724L;

		@Override
		public void reduce(Iterable <Tuple5 <Long, Long, Long, Long, Double>> values,
						   Collector <Tuple5 <Long, Long, Long, Long, Double>> out) throws Exception {
			long counter = 0L;
			List <Tuple5 <Long, Long, Long, Long, Double>> l = new ArrayList <>();
			for (Tuple5 <Long, Long, Long, Long, Double> i : values) {
				counter += 1L;
				l.add(i);
			}
			for (Tuple5 <Long, Long, Long, Long, Double> i : l) {
				out.collect(new Tuple5 <>(i.f0, i.f1, counter, i.f3, i.f4));
			}
		}
	}

	public static class ReduceOnSecondField
		implements
		GroupReduceFunction <Tuple5 <Long, Long, Long, Long, Double>, Tuple5 <Long, Long, Long, Long, Double>> {
		private static final long serialVersionUID = 7840099990204577056L;

		@Override
		public void reduce(Iterable <Tuple5 <Long, Long, Long, Long, Double>> values,
						   Collector <Tuple5 <Long, Long, Long, Long, Double>> out) throws Exception {
			long counter = 0L;
			List <Tuple5 <Long, Long, Long, Long, Double>> l = new ArrayList <>();
			for (Tuple5 <Long, Long, Long, Long, Double> i : values) {
				counter += 1L;
				l.add(i);
			}
			for (Tuple5 <Long, Long, Long, Long, Double> i : l) {
				out.collect(new Tuple5 <>(i.f0, i.f1, i.f2, counter, i.f4));
			}
		}
	}

	public static class FilterSmallOnesOnFirstField
		implements FilterFunction <Tuple5 <Long, Long, Long, Long, Double>> {
		private static final long serialVersionUID = -4414815465890029511L;
		private long k;

		private FilterSmallOnesOnFirstField(long k) {
			this.k = k;
		}

		@Override
		public boolean filter(Tuple5 <Long, Long, Long, Long, Double> value) throws Exception {
			return value.f2 > this.k;
		}
	}

	public static class FilterSmallOnesOnSecondField
		implements FilterFunction <Tuple5 <Long, Long, Long, Long, Double>> {
		private static final long serialVersionUID = 156799354134467716L;
		private long k;

		private FilterSmallOnesOnSecondField(long k) {
			this.k = k;
		}

		@Override
		public boolean filter(Tuple5 <Long, Long, Long, Long, Double> value) throws Exception {
			return value.f3 > this.k;
		}
	}

	public static class FilterLargeOnesOnThiState
		implements FilterFunction <Tuple5 <Long, Long, Long, Long, Double>> {
		private static final long serialVersionUID = 1257898737107879380L;
		private long k;

		private FilterLargeOnesOnThiState(long k) {
			this.k = k;
		}

		@Override
		public boolean filter(Tuple5 <Long, Long, Long, Long, Double> value) throws Exception {
			return value.f3 <= this.k;
		}
	}

}