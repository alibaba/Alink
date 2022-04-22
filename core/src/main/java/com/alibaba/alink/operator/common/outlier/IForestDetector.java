package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.common.dataproc.NumericalTypeCastMapper;
import com.alibaba.alink.operator.common.outlier.IForestModelDetector.IForestModel;
import com.alibaba.alink.params.dataproc.HasTargetType.TargetType;
import com.alibaba.alink.params.dataproc.NumericalTypeCastParams;
import com.alibaba.alink.params.outlier.IForestTrainParams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.ThreadLocalRandom;

public class IForestDetector extends OutlierDetector {
	private static final double DEFAULT_THRESHOLD = 0.5;

	public IForestDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	protected Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast)
		throws Exception {

		series = OutlierUtil.getMTable(series, params);

		NumericalTypeCastMapper numericalTypeCastMapper = new NumericalTypeCastMapper(
			series.getSchema(),
			new Params()
				.set(NumericalTypeCastParams.SELECTED_COLS, series.getColNames())
				.set(NumericalTypeCastParams.TARGET_TYPE, TargetType.DOUBLE)
		);

		int numRows = series.getNumRow();

		List<Row> rows = new ArrayList <>(numRows);

		for (int i = 0; i < numRows; ++i) {
			rows.add(numericalTypeCastMapper.map(series.getRow(i)));
		}

		series = new MTable(rows, series.getSchemaStr());

		IForestTrain iForestTrain = new IForestTrain(params);
		IForestPredict iForestPredict = new IForestPredict(params);

		iForestPredict.loadModel(iForestTrain.train(series));

		int iStart = detectLast ? series.getNumRow() - 1 : 0;

		Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[series.getNumRow() - iStart];
		for (int i = iStart; i < series.getNumRow(); i++) {
			double score = iForestPredict.predict(series.getRow(i));

			if (isPredDetail) {
				results[i - iStart] = Tuple3.of(score > DEFAULT_THRESHOLD, score, new HashMap <>());
			} else {
				results[i - iStart] = Tuple3.of(score > DEFAULT_THRESHOLD, null, null);
			}
		}

		return results;
	}

	public static class Node implements Serializable {

		private static final long serialVersionUID = 2089955209876272479L;

		public int f = -1;
		public double val = 0.0;
		public double w = 1.0;

		public int l = -1;
		public int r = -1;
	}

	public final static class IForestTrain implements IForestTrainParams <IForestTrain> {
		private final Params params;

		public IForestTrain() {
			params = new Params();
		}

		public IForestTrain(Params params) {
			this.params = params == null ? new Params() : params;
		}

		private static class Context {
			public IForestDetector.Node nd;

			public int h;
			public int s;
			public int e;
		}

		private static final double LOG2 = Math.log(2);
		private static final double EPS = 1e-6;
		private static final double DOUBLE_EPS = 2 * 1e-6;

		public IForestModel train(MTable input) {
			int numTrees = params.get(IForestTrainParams.NUM_TREES);

			int subSamplingSize = Math.min(params.get(IForestTrainParams.SUBSAMPLING_SIZE), input.getNumRow());
			params.set(IForestTrainParams.SUBSAMPLING_SIZE, subSamplingSize);

			int heightLimit = (int) Math.ceil(Math.log(subSamplingSize) / LOG2);

			Random rnd = ThreadLocalRandom.current();

			IForestModel model = new IForestModel();

			model.meta = params;

			for (int i = 0; i < numTrees; ++i) {
				MTable subInput;

				if (subSamplingSize == input.getNumRow()) {
					subInput = input;
				} else {
					subInput = input.sampleWithSize(subSamplingSize, rnd);
				}

				model.trees.add(iTree(subInput, heightLimit, rnd));
			}

			return model;
		}

		public List <IForestDetector.Node> iTree(final MTable input, final int limit, final Random rnd) {
			final List <Row> a = input.getRows();
			final int numAtts = input.getNumCol();
			final List <IForestDetector.Node> model = new ArrayList <>();

			Stack <Context> stack = new Stack <>();

			Context root = new Context();
			root.nd = new IForestDetector.Node();
			root.h = 0;
			root.s = 0;
			root.e = a.size();
			model.add(root.nd);
			stack.push(root);

			while (!stack.empty()) {

				Context ctx = stack.pop();

				ctx.nd.w = ctx.e - ctx.s;

				if (ctx.nd.w <= 1 || ctx.h >= limit) {
					continue;
				}

				BitSet h = new BitSet(numAtts);

				while (h.cardinality() < (numAtts - 1)) {
					double max = -Double.MAX_VALUE, min = Double.MAX_VALUE;

					// select feature.
					int f = rnd.nextInt(numAtts);
					while (h.get(f)) {
						f = (f + 1) % numAtts;
					}
					h.set(f);

					// random val
					for (int i = ctx.s; i < ctx.e; ++i) {
						max = Math.max((double) a.get(i).getField(f), max);
						min = Math.min((double) a.get(i).getField(f), min);
					}

					if (max - min <= DOUBLE_EPS) {
						continue;
					}

					double val = min + EPS + ((max - min - DOUBLE_EPS) * rnd.nextDouble());

					while (val - min <= EPS || max - val <= EPS) {
						val = min + EPS + ((max - min - DOUBLE_EPS) * rnd.nextDouble());
					}

					// fill node val.
					ctx.nd.f = f;
					ctx.nd.val = val;

					// split.

					int l = ctx.s, r = ctx.e - 1;

					while (l <= r) {
						while (l <= r && (double) a.get(l).getField(f) <= val) {
							l++;
						}

						while (l <= r && (double) a.get(r).getField(f) > val) {
							r--;
						}

						if (l < r) {
							Row tmp = a.get(l);
							a.set(l, a.get(r));
							a.set(r, tmp);
						}
					}

					Context rCtx = new Context();
					rCtx.nd = new IForestDetector.Node();
					rCtx.h = ctx.h + 1;
					rCtx.s = l;
					rCtx.e = ctx.e;
					stack.push(rCtx);

					Context lCtx = new Context();
					lCtx.nd = new IForestDetector.Node();
					lCtx.h = ctx.h + 1;
					lCtx.s = ctx.s;
					lCtx.e = l;
					stack.push(lCtx);

					// serialize to model.
					ctx.nd.l = model.size();
					model.add(lCtx.nd);
					ctx.nd.r = model.size();
					model.add(rCtx.nd);

					break;
				}
			}

			return model;
		}

		@Override
		public Params getParams() {
			return params;
		}
	}

	public final static class IForestPredict implements WithParams <IForestPredict> {

		private final Params params;

		private transient int numTrees;
		private transient double avgPathLength;

		private transient List <List <Node>> iForest;

		private final static double EULER_CONSTANT = 0.5772156649;

		public IForestPredict() {
			this(new Params());
		}

		public IForestPredict(Params params) {
			this.params = params == null ? new Params() : params;
		}

		public void loadModel(IForestModel model) {
			numTrees = model.meta.get(IForestTrainParams.NUM_TREES);
			avgPathLength = c(model.meta.get(IForestTrainParams.SUBSAMPLING_SIZE));
			iForest = model.trees;
		}

		public double predict(Row row) {
			double sum = 0.0;

			for (List <Node> root : iForest) {
				Node node = root.get(0);
				int pathLen = 0;
				while (node.f > 0) {
					if (((double) row.getField(node.f)) <= node.val) {
						node = root.get(node.l);
					} else {
						node = root.get(node.r);
					}
					pathLen++;
				}

				sum += pathLen + c(node.w);
			}

			return s(sum / numTrees, avgPathLength);
		}

		private static double c(double n) {
			if (n <= 1.0) {
				return 0;
			}
			return 2.0 * (Math.log(n - 1) + EULER_CONSTANT) - (2.0 * (n - 1) / n);
		}

		private static double s(double eh, double c) {
			return Math.pow(2, -1.0 * eh / c);
		}

		@Override
		public Params getParams() {
			return params;
		}
	}
}
