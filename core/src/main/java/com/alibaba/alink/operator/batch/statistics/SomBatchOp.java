package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Tensor;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.SomJni;
import com.alibaba.alink.params.statistics.SomParams;
import org.apache.commons.lang3.ArrayUtils;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Self-Organized Map algorithm.
 * <p>
 * reference:
 * 1. http://davis.wpi.edu/~matt/courses/soms/
 * 2. https://github.com/JustGlowing/minisom
 * 3. https://clarkdatalabs.github.io/soms/SOM_NBA
 * <p>
 * A rule of thumb to set the size of the grid for a dimensionality
 * reduction task is that it should contain 5*Sqrt(N) neurons
 * where N is the number of samples in the dataset to analyze.
 * E.g. if your dataset has 150 samples, 5*Sqrt(150) = 61.23
 * hence a map 8-by-8 should perform well.
 */

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {
	@PortSpec(PortType.DATA),
	@PortSpec(PortType.DATA)
})

@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("Som")
@NameEn("Som")
public final class SomBatchOp extends BatchOperator <SomBatchOp>
	implements SomParams <SomBatchOp> {

	public final static String[] COL_NAMES = new String[] {"meta", "xidx", "yidx", "weights", "cnt"};
	public final static TypeInformation[] COL_TYPES = new TypeInformation[] {AlinkTypes.STRING, AlinkTypes.LONG, AlinkTypes.LONG,
		AlinkTypes.STRING, AlinkTypes.LONG};
	public final static boolean DO_PREDICTION = true;
	private static final long serialVersionUID = -6014481798410706652L;

	public SomBatchOp() {
		this(new Params());
	}

	public SomBatchOp(Params params) {
		super(params);
	}

	@Override
	public SomBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		String tensorColName = getVectorCol();
		final int numIters = getNumIters();
		final int xdim = getXdim();
		final int ydim = getYdim();
		final int vdim = getVdim();
		final String meta = String.format("%d,%d,%d,r", xdim, ydim, vdim);
		final boolean eval = getEvaluation();

		// count number of traning samples
		DataSet <Long> numSamples = in.getDataSet()
			.mapPartition(new MapPartitionFunction <Row, Long>() {
				private static final long serialVersionUID = -4852925590649190739L;

				@Override
				public void mapPartition(Iterable <Row> iterable, Collector <Long> collector) throws Exception {
					long cnt = 0L;
					for (Row r : iterable) {
						cnt++;
					}
					collector.collect(cnt);
				}
			})
			.reduce(new ReduceFunction <Long>() {
				private static final long serialVersionUID = -6343518193952236485L;

				@Override
				public Long reduce(Long aLong, Long t1) throws Exception {
					return aLong + t1;
				}
			});

		// initialize the model by randomly select xdim * ydim samples from the training data
		DataSet <Tuple3 <Long, Long, String>> initModel = in.select(tensorColName).getDataSet()
			.mapPartition(new RichMapPartitionFunction <Row, Tuple3 <Long, Long, String>>() {
				private static final long serialVersionUID = -1154161394939821199L;
				List <Row> selectedRows;

				@Override
				public void open(Configuration parameters) throws Exception {
					selectedRows = new ArrayList <>(xdim * ydim);

					long n = (long) getRuntimeContext().getBroadcastVariable("numSamples").get(0);
					if (n < xdim * ydim) {
						throw new RuntimeException("xdim * ydim > num training samples");
					}

					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("Initializing model, num training samples: " + n);
					}
				}

				@Override
				public void mapPartition(Iterable <Row> iterable, Collector <Tuple3 <Long, Long, String>> collector)
					throws Exception {
					Random random = new Random();
					int cnt = 0;
					for (Row r : iterable) {
						if (cnt < xdim * ydim) {
							selectedRows.add(r);
						} else {
							boolean keep = random.nextDouble() < (double) (xdim * ydim) / (cnt + 1);
							if (keep) {
								int pick = random.nextInt(xdim * ydim);
								selectedRows.set(pick, r);
							}
						}
						cnt++;
					}

					int pos = 0;
					for (int i = 0; i < xdim; i++) {
						for (int j = 0; j < ydim; j++) {
							Object object = selectedRows.get(pos).getField(0);
							if (object instanceof DenseVector) {
								object = VectorUtil.serialize(object);
							} else if (object instanceof SparseVector) {
								object = VectorUtil.serialize(object);
							}
							collector.collect(Tuple3.of((long) i, (long) j, (String) (object)));
							pos++;
						}
					}
				}
			})
			.withBroadcastSet(numSamples, "numSamples")
			.setParallelism(1)
			.name("init_model");

		IterativeDataSet <Tuple3 <Long, Long, String>> loop = initModel.iterate(numIters).setParallelism(1);

		DataSet <Tuple3 <Long, Long, String>> updatedModel;
		updatedModel = in.select(tensorColName).getDataSet()
			.mapPartition(new SomTask(getParams()))
			.withBroadcastSet(loop, "initModel")
			.withBroadcastSet(numSamples, "numSamples")
			.setParallelism(1)
			.name("som_train");

		DataSet <Tuple3 <Long, Long, String>> finalModel = loop.closeWith(updatedModel);

		// output the model
		DataSet <Row> model = in.select(tensorColName).getDataSet()
			.mapPartition(new RichMapPartitionFunction <Row, Row>() {
				private static final long serialVersionUID = -7426117483145285343L;

				@Override
				public void open(Configuration parameters) throws Exception {
					if (getRuntimeContext().getNumberOfParallelSubtasks() != 1) {
						throw new RuntimeException("parallelism should be 1");
					}
				}

				@Override
				public void mapPartition(Iterable <Row> iterable, Collector <Row> collector) throws Exception {
					List <Tuple3 <Long, Long, String>> bcModel = getRuntimeContext().getBroadcastVariable("somModel");
					if (bcModel.size() != xdim * ydim) {
						throw new RuntimeException("unexpected");
					}
					SomModel model = new SomModel(xdim, ydim, vdim);
					model.init(bcModel);
					model.initCount();

					float[] v = new float[vdim];
					int[] bmu = new int[2];

					for (Row r : iterable) {
						DenseVector tensor = VectorUtil.getDenseVector(r.getField(0));
						double[] data = tensor.getData();
						for (int i = 0; i < vdim; i++) {
							v[i] = (float) data[i];
						}
						model.findBMU(v, bmu);
						model.increaseCount(bmu, 1L);
					}

					long[][] counts = model.getCounts();
					List <Tuple3 <Long, Long, String>> weights = model.getWeights();

					for (Tuple3 <Long, Long, String> w : weights) {
						collector.collect(Row.of(meta, w.f0, w.f1, w.f2, counts[w.f0.intValue()][w.f1.intValue()]));
					}
				}
			})
			.withBroadcastSet(finalModel, "somModel")
			.setParallelism(1)
			.name("count");

		setOutput(model, COL_NAMES, COL_TYPES);

		// do prediction
		if (DO_PREDICTION) {
			DataSet <Row> pred = in.getDataSet()
				.map(new RichMapFunction <Row, Row>() {
					private static final long serialVersionUID = 5561628297750641436L;
					transient SomModel model;

					@Override
					public void open(Configuration parameters) throws Exception {
						List <Tuple3 <Long, Long, String>> bcModel = getRuntimeContext().getBroadcastVariable(
							"somModel");
						if (bcModel.size() != xdim * ydim) {
							throw new RuntimeException("unexpected");
						}
						model = new SomModel(xdim, ydim, vdim);
						model.init(bcModel);
					}

					@Override
					public Row map(Row r) throws Exception {
						float[] v = new float[vdim];
						int[] bmu = new int[2];
						DenseVector tensor = VectorUtil.getDenseVector(r.getField(0));
						double[] data = tensor.getData();
						for (int i = 0; i < vdim; i++) {
							v[i] = (float) data[i];
						}
						model.findBMU(v, bmu);

						Row o = new Row(r.getArity() + 2);
						for (int i = 0; i < r.getArity(); i++) {
							o.setField(i, r.getField(i));
						}
						for (int i = 0; i < 2; i++) {
							o.setField(i + r.getArity(), (long) bmu[i]);
						}
						return o;
					}
				})
				.withBroadcastSet(finalModel, "somModel");

			Table table = DataSetConversionUtil.toTable(getMLEnvironmentId(), pred,
				ArrayUtils.addAll(in.getColNames(), new String[] {"xidx", "yidx"}),
				ArrayUtils.addAll(in.getColTypes(), new TypeInformation <?>[] {AlinkTypes.LONG, AlinkTypes.LONG}));
			this.setSideOutputTables(new Table[] {table});
		}

		return this;
	}

	private static class SomTask extends RichMapPartitionFunction <Row, Tuple3 <Long, Long, String>> {
		private static final long serialVersionUID = 6117856294526477050L;
		Params params;

		transient SomSolver solver = null;

		public SomTask(Params params) {
			this.params = params;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println("\n ** step " + getIterationRuntimeContext().getSuperstepNumber());
				System.out.println(new Date().toString() + ": " + "start step ...");
			}

			if (solver == null) {
				final int numIters = params.get(SomParams.NUM_ITERS);
				final int xdim = params.get(SomParams.XDIM);
				final int ydim = params.get(SomParams.YDIM);
				final int vdim = params.get(SomParams.VDIM);
				final double learnRate = params.get(SomParams.LEARN_RATE);
				final double sigma = params.get(SomParams.SIGMA);
				List <Tuple3 <Long, Long, String>> bcModel = getRuntimeContext().getBroadcastVariable("initModel");
				if (bcModel.size() != xdim * ydim) {
					throw new RuntimeException("unexpected");
				}
				List <Long> bcNumSamples = getRuntimeContext().getBroadcastVariable("numSamples");
				long numSamples = bcNumSamples.get(0);
				solver = new SomSolver(xdim, ydim, vdim, learnRate, sigma, (long) numIters * numSamples);
				solver.init(bcModel);
			}
		}

		@Override
		public void close() throws Exception {
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(new Date().toString() + ": " + "close step ...");
			}
		}

		@Override
		public void mapPartition(Iterable <Row> iterable, Collector <Tuple3 <Long, Long, String>> collector)
			throws Exception {
			final int vdim = params.get(SomParams.VDIM);
			final int BATCH_SIZE = 64 * 1024;
			float[] batch = new float[BATCH_SIZE * vdim];
			int batchCnt = 0;

			for (Row row : iterable) {
				DenseVector tensor = VectorUtil.getDenseVector(row.getField(0));
				double[] data = tensor.getData();
				int pos = batchCnt * vdim;
				for (int i = 0; i < vdim; i++) {
					batch[pos + i] = (float) data[i];
				}
				batchCnt++;
				if (batchCnt >= BATCH_SIZE) {
					solver.updateBatch(batch, batchCnt);
					batchCnt = 0;
				}
			}

			if (batchCnt > 0) {
				solver.updateBatch(batch, batchCnt);
				batchCnt = 0;
			}

			List <Tuple3 <Long, Long, String>> weights = solver.getWeights();
			for (Tuple3 <Long, Long, String> w : weights) {
				collector.collect(w);
			}
		}
	}

	public static class SomSolver {
		private int xdim;
		private int ydim;
		private int vdim;
		private double learnRate;
		private double sigma;
		private long currStepNo = 0L;
		private long maxStepNo;
		private float[] weights;
		private SomJni somJni;

		public SomSolver(int xdim, int ydim, int vdim, double learnRate, double sigma, long maxStepNo) {
			this.xdim = xdim;
			this.ydim = ydim;
			this.vdim = vdim;
			this.learnRate = learnRate;
			this.sigma = sigma;
			this.maxStepNo = maxStepNo;
			this.currStepNo = 0L;
			weights = new float[xdim * ydim * vdim];

			somJni = new SomJni();

			if (maxStepNo > 0) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println(String.format("xdim=%d,ydim=%d,vdim=%d,learnRate=%f,sigma=%f,maxStepNo=%d",
						xdim, ydim, vdim, learnRate, sigma, maxStepNo));
				}
			}
		}

		private static double decayFunction(double v, double currStepNo, double maxStepNo) {
			return v / (1.0 + 2.0 * currStepNo / maxStepNo);
		}

		public static int getNeuronPos(int x, int y, int xdim, int ydim, int vdim) {
			return (y * xdim + x) * vdim;
		}

		public void init(List <Tuple3 <Long, Long, String>> model) {
			for (Tuple3 <Long, Long, String> neron : model) {
				int x = neron.f0.intValue();
				int y = neron.f1.intValue();
				String tensorStr = neron.f2;
				DenseVector tensor = VectorUtil.getDenseVector(tensorStr);
				double[] data = tensor.getData();
				if (data.length != vdim) {
					throw new RuntimeException("Invalid data length: " + data.length);
				}
				int pos = getNeuronPos(x, y, xdim, ydim, vdim);
				for (int i = 0; i < vdim; i++) {
					this.weights[pos + i] = (float) data[i];
				}
			}
		}

		public void updateBatch(float[] batch, int cnt) {
			float lr = (float) decayFunction(this.learnRate, this.currStepNo, this.maxStepNo);
			float sig = (float) decayFunction(this.sigma, this.currStepNo, this.maxStepNo);

			somJni.updateBatchJava(weights, batch, cnt, lr, sig, xdim, ydim, vdim);

			this.currStepNo += cnt;
		}

		public List <Tuple3 <Long, Long, String>> getWeights() {
			List <Tuple3 <Long, Long, String>> ret = new ArrayList <>(xdim * ydim);

			for (int i = 0; i < xdim; i++) {
				for (int j = 0; j < ydim; j++) {
					int pos = getNeuronPos(i, j, xdim, ydim, vdim);
					StringBuilder sbd = new StringBuilder();
					for (int k = 0; k < vdim; k++) {
						if (k > 0) {
							sbd.append(",");
						}
						sbd.append(weights[pos + k]);
					}
					ret.add(Tuple3.of((long) i, (long) j, sbd.toString()));
				}
			}

			return ret;
		}
	}

	public static class SomModel {
		private int xdim;
		private int ydim;
		private int vdim;

		private float[] weights;
		private int[] bmu0;

		private long[][] counts;
		private float[][] umatrix;

		private SomJni somJni;

		public SomModel(int xdim, int ydim, int vdim) {
			this.xdim = xdim;
			this.ydim = ydim;
			this.vdim = vdim;
			weights = new float[xdim * ydim * vdim];
			bmu0 = new int[2];

			somJni = new SomJni();
		}

		private static float squaredDistance(float[] v1, int s1, float[] v2, int s2, int n) {
			float s = 0.F;
			for (int i = 0; i < n; i++) {
				s += (v1[s1 + i] - v2[s2 + i]) * (v1[s1 + i] - v2[s2 + i]);
			}
			return s;
		}

		public int getNeuronPos(int x, int y) {
			return (y * xdim + x) * vdim;
		}

		public void setNeuron(int x, int y, String tensorStr) {
			Tensor tensor = Tensor.parse(tensorStr);
			double[] data = tensor.getData();
			if (data.length != vdim) {
				throw new RuntimeException("invalid data length: " + data.length);
			}
			int pos = getNeuronPos(x, y);
			for (int i = 0; i < vdim; i++) {
				this.weights[pos + i] = (float) data[i];
			}
		}

		public void init(List <Tuple3 <Long, Long, String>> model) {
			for (Tuple3 <Long, Long, String> neron : model) {
				int x = neron.f0.intValue();
				int y = neron.f1.intValue();
				String tensorStr = neron.f2;
				DenseVector tensor = VectorUtil.parseDense(tensorStr);
				double[] data = tensor.getData();
				if (data.length != vdim) {
					throw new RuntimeException("Invalid data length: " + data.length);
				}
				int pos = getNeuronPos(x, y);
				for (int i = 0; i < vdim; i++) {
					this.weights[pos + i] = (float) data[i];
				}
			}
		}

		public void initCount() {
			this.counts = new long[xdim][ydim];
		}

		public void increaseCount(int[] who, long c) {
			this.counts[who[0]][who[1]] += c;
		}

		public long[][] getCounts() {
			return this.counts;
		}

		private float findBMU(final float[] v, int[] bmu) {
			float minD2 = somJni.findBmuJava(weights, new float[xdim * ydim], v, bmu0, xdim, ydim, vdim);

			if (bmu != null) {
				bmu[0] = bmu0[0];
				bmu[1] = bmu0[1];
			}
			return minD2;
		}

		public List <Tuple3 <Long, Long, String>> getWeights() {
			List <Tuple3 <Long, Long, String>> ret = new ArrayList <>(xdim * ydim);

			for (int i = 0; i < xdim; i++) {
				for (int j = 0; j < ydim; j++) {
					int pos = getNeuronPos(i, j);
					StringBuilder sbd = new StringBuilder();
					for (int k = 0; k < vdim; k++) {
						if (k > 0) {
							sbd.append(",");
						}
						sbd.append(weights[pos + k]);
					}
					ret.add(Tuple3.of((long) i, (long) j, sbd.toString()));
				}
			}

			return ret;
		}

		public void createUMatrix() {
			this.umatrix = new float[xdim][ydim];
			for (int i = 0; i < xdim; i++) {
				for (int j = 0; j < ydim; j++) {
					float sum = 0.F;
					int cnt = 0;

					for (int k = -1; k <= 1; k += 2) {
						for (int l = -1; l <= 1; l += 2) {
							int xtarget = i + k;
							int ytarget = j + l;
							if (xtarget < 0 || xtarget >= xdim) {
								continue;
							}
							if (ytarget < 0 || ytarget >= ydim) {
								continue;
							}
							sum += squaredDistance(this.weights, getNeuronPos(i, j),
								this.weights, getNeuronPos(xtarget, ytarget), vdim);
							cnt++;
						}
					}

					this.umatrix[i][j] = (float) Math.sqrt(sum / cnt);
				}
			}
		}

		public float getUMatrixValue(int x, int y) {
			return this.umatrix[x][y];
		}

		public void writeToFile(String xFn, String yFn) throws Exception {
			StringBuilder sbdx = new StringBuilder();
			StringBuilder sbdy = new StringBuilder();
			for (int i = 0; i < ydim; i++) {
				for (int j = 0; j < xdim; j++) {
					if (j > 0) {
						sbdx.append(" ");
						sbdy.append(" ");
					}
					int pos = getNeuronPos(j, i);
					sbdx.append(weights[pos + 0]);
					sbdy.append(weights[pos + 1]);
				}
				sbdx.append("\n");
				sbdy.append("\n");
			}

			{
				FileOutputStream out = new FileOutputStream(xFn);
				out.write(sbdx.toString().getBytes());
				out.close();
			}

			{
				FileOutputStream out = new FileOutputStream(yFn);
				out.write(sbdy.toString().getBytes());
				out.close();
			}
		}

		public void writePointsToFile(String fn, List <Double> x, List <Double> y) throws Exception {
			int n = x.size();
			StringBuilder sbd = new StringBuilder();

			for (int i = 0; i < n; i++) {
				sbd.append(x.get(i)).append(" ").append(y.get(i)).append("\n");
			}

			FileOutputStream out = new FileOutputStream(fn);
			out.write(sbd.toString().getBytes());
			out.close();
		}
	}

}
