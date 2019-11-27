package com.alibaba.alink.common.comqueue.communication;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CommunicateFunction;
import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.common.io.directreader.DistributedInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * An implement of {@link CommunicateFunction} that do the AllReduce.
 *
 * AllReduce is a communication primitive widely used in MPI. In our implementation, all workers do reduce on
 * a partition of the whole data and they all get the final reduce result.
 *
 * There're mainly three stages:
 * <ol>
 *     <li>1. All workers send the there partial data to other workers for reduce.</li>
 *     <li>2. All workers do reduce on all data it received and then send partial results to others.</li>
 *     <li>3. All workers merge partial results into final result and put it into session context with pre-defined
 *     object name.</li>
 * </ol>
 */
public class AllReduce extends CommunicateFunction {
	private static final int TRANSFER_BUFFER_SIZE = 1024 * 4;
	private static final Logger LOG = LoggerFactory.getLogger(AllReduce.class);
	private final String bufferName;
	private final String lengthName;
	private final SerializableBiConsumer <double[], double[]> op;

	/**
	 * Create an AllReduce communicate function.
	 *
	 * The whole double array object of given name will be performed on and the reduce function is SUM by default.
	 *
	 * @param bufferName name of double[] object to perform all-reduce.
	 */
	public AllReduce(String bufferName) {
		this(bufferName, null);
	}

	/**
	 * Create an AllReduce communicate function for a subset of double array object.
	 * <p>
	 * The reduce function is SUM by default.
	 *
	 * @param bufferName name of double[] object to perform all-reduce.
	 * @param lengthName object name of the length. Sub-range of [0, length) of the double array will be all-reduced.
	 */
	public AllReduce(String bufferName, String lengthName) {
		this(bufferName, lengthName, SUM);
	}

	/**
	 *Create an AllReduce communicate function.
	 *
	 * @param bufferName name of double[] object to perform all-reduce.
	 * @param lengthName object name of the length. Sub-range of [0, length) of the double array will be all-reduced.
	 * @param op the all-reduce function.
	 */
	public AllReduce(String bufferName, String lengthName, SerializableBiConsumer <double[], double[]> op) {
		this.bufferName = bufferName;
		this.lengthName = lengthName;
		this.op = op;
	}

	public static <T> DataSet <T> allReduce(
		DataSet <T> input,
		final String bufferName,
		final String lengthName,
		final SerializableBiConsumer <double[], double[]> op,
		final int sessionId) {
		final String transferBufferName = UUID.randomUUID().toString();

		return input
			.mapPartition(new AllReduceSend <T>(bufferName, lengthName, transferBufferName, sessionId))
			.withBroadcastSet(input, "barrier")
			.returns(
				new TupleTypeInfo <>(Types.INT, Types.INT, PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO))
			.name("AllReduceSend")
			.partitionCustom(new Partitioner <Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key;
				}
			}, 0)
			.name("AllReduceBroadcastRaw")
			.mapPartition(new AllReduceSum(bufferName, lengthName, sessionId, op))
			.returns(
				new TupleTypeInfo <>(Types.INT, Types.INT, PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO))
			.name("AllReduceSum")
			.partitionCustom(new Partitioner <Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key;
				}
			}, 0)
			.name("AllReduceBroadcastSum")
			.mapPartition(new AllReduceRecv <T>(bufferName, lengthName, sessionId))
			.returns(input.getType())
			.name("AllReduceRecv");
	}

	/**
	 * The all-reduce operation which does elementwise sum operation.
	 */
	public final static SerializableBiConsumer <double[], double[]> SUM
		= new SerializableBiConsumer <double[], double[]>() {
		@Override
		public void accept(double[] a, double[] b) {
			for (int i = 0; i < a.length; ++i) {
				a[i] += b[i];
			}
		}
	};

	/**
	 * The all-reduce operation which does elementwise max operation.
	 */
	public final static SerializableBiConsumer <double[], double[]> MAX
		= new SerializableBiConsumer <double[], double[]>() {
		@Override
		public void accept(double[] a, double[] b) {
			for (int i = 0; i < a.length; ++i) {
				a[i] = Math.max(a[i], b[i]);
			}
		}
	};

	/**
	 * The all-reduce operation which does elementwise min operation.
	 */
	public final static SerializableBiConsumer <double[], double[]> MIN
		= new SerializableBiConsumer <double[], double[]>() {
		@Override
		public void accept(double[] a, double[] b) {
			for (int i = 0; i < a.length; ++i) {
				a[i] = Math.min(a[i], b[i]);
			}
		}
	};

	private static int pieces(int len) {
		int div = len / TRANSFER_BUFFER_SIZE;
		int mod = len % TRANSFER_BUFFER_SIZE;

		return mod == 0 ? div : div + 1;
	}

	private static int lastLen(int len) {
		int mod = len % TRANSFER_BUFFER_SIZE;
		return mod == 0 ? TRANSFER_BUFFER_SIZE : mod;
	}

	@Override
	public <T> DataSet <T> communicateWith(DataSet <T> input, int sessionId) {
		return allReduce(input, this.bufferName, this.lengthName, op, sessionId);
	}

	public interface SerializableBiConsumer<T, U> extends BiConsumer <T, U>, Serializable {
		/**
		 * copy from java sdk
		 */
		default SerializableBiConsumer <T, U> andThen(SerializableBiConsumer <? super T, ? super U> after) {
			Objects.requireNonNull(after);

			return (l, r) -> {
				accept(l, r);
				after.accept(l, r);
			};
		}
	}

	private static class AllReduceSend<T> extends RichMapPartitionFunction <T, Tuple3 <Integer, Integer, double[]>> {
		private final String bufferName;
		private final String lengthName;
		private final String transferBufferName;
		private final int sessionId;

		AllReduceSend(String bufferName, String lengthName, String transferBufferName, int sessionId) {
			this.bufferName = bufferName;
			this.lengthName = lengthName;
			this.transferBufferName = transferBufferName;
			this.sessionId = sessionId;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			LOG.info("taskId: {}, collect open", getRuntimeContext().getIndexOfThisSubtask());
		}

		@Override
		public void close() throws Exception {
			super.close();
			LOG.info("taskId: {}, collect end", getRuntimeContext().getIndexOfThisSubtask());
		}

		@Override
		public void mapPartition(Iterable <T> values, Collector <Tuple3 <Integer, Integer, double[]>> out)
			throws Exception {
			ComContext context = new ComContext(sessionId, getIterationRuntimeContext());
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int numOfSubTasks = getRuntimeContext().getNumberOfParallelSubtasks();
			int iterNum = getIterationRuntimeContext().getSuperstepNumber();

			LOG.info("taskId: {}, collect start", taskId);

			double[] sendBuf = context.getObj(bufferName);
			int sendLen = lengthName != null ? context.getObj(lengthName) : sendBuf.length;
			double[] transBuf = null;
			if (iterNum == 1) {
				transBuf = new double[TRANSFER_BUFFER_SIZE];
				context.putObj(transferBufferName, transBuf);
			} else {
				transBuf = context.getObj(transferBufferName);
			}

			int pieces = pieces(sendLen);

			LOG.info("taskId: {}, len: {}, pieces: {}", taskId, sendLen, pieces);

			DistributedInfo distributedInfo = new DefaultDistributedInfo();

			int agg = 0;
			for (int i = 0; i < numOfSubTasks; ++i) {
				int startPos = (int) distributedInfo.startPos(i, numOfSubTasks, pieces);
				int cnt = (int) distributedInfo.localRowCnt(i, numOfSubTasks, pieces);

				for (int j = 0; j < cnt; ++j) {
					int bufStart = (startPos + j) * TRANSFER_BUFFER_SIZE;
					// the last
					if (startPos + j == pieces - 1) {
						System.arraycopy(sendBuf, bufStart, transBuf, 0, lastLen(sendLen));
					} else {
						System.arraycopy(sendBuf, bufStart, transBuf, 0, TRANSFER_BUFFER_SIZE);
					}
					agg++;
					out.collect(Tuple3.of(i, startPos + j, transBuf));
				}
			}

			LOG.info("taskId: {}, collect end, agg: {}", taskId, agg);
		}
	}

	private static class AllReduceSum extends RichMapPartitionFunction <Tuple3 <Integer, Integer, double[]>,
		Tuple3 <Integer, Integer, double[]>> {
		private final String bufferName;
		private final String lengthName;
		private final int sessionId;
		private final SerializableBiConsumer <double[], double[]> op;

		AllReduceSum(String bufferName, String lengthName, int sessionId,
					 SerializableBiConsumer <double[], double[]> op) {
			this.bufferName = bufferName;
			this.lengthName = lengthName;
			this.sessionId = sessionId;
			this.op = op;
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Integer, Integer, double[]>> values,
								 Collector <Tuple3 <Integer, Integer, double[]>> out) {
			ComContext context = new ComContext(this.sessionId, getIterationRuntimeContext());

			Iterator <Tuple3 <Integer, Integer, double[]>> it = values.iterator();
			if (!it.hasNext()) {
				return;
			}

			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int numOfSubTasks = getRuntimeContext().getNumberOfParallelSubtasks();

			LOG.info("taskId: {}, AllReduceSum start", taskId);
			double[] sendBuf = context.getObj(bufferName);
			int sendLen = lengthName != null ? context.getObj(lengthName) : sendBuf.length;
			LOG.info("taskId: {}, AllReduceSum sendLen: {}", taskId, sendLen);
			int pieces = pieces(sendLen);
			DistributedInfo distributedInfo = new DefaultDistributedInfo();

			int startPos = (int) distributedInfo.startPos(taskId, numOfSubTasks, pieces);
			int cnt = (int) distributedInfo.localRowCnt(taskId, numOfSubTasks, pieces);

			LOG.info("taskId: {}, AllReduceSum cnt: {}", taskId, cnt);

			double[][] sum = new double[cnt][];
			double[] agg = new double[cnt];
			do {
				Tuple3 <Integer, Integer, double[]> val = it.next();
				int localPos = val.f1 - startPos;
				if (sum[localPos] == null) {
					sum[localPos] = val.f2;
					agg[localPos]++;
				} else {
					op.accept(sum[localPos], val.f2);
				}
			} while (it.hasNext());

			for (int i = 0; i < numOfSubTasks; ++i) {
				for (int j = 0; j < cnt; ++j) {
					out.collect(Tuple3.of(i, startPos + j, sum[j]));
				}
			}

			LOG.info("taskId: {} AllReduceSum agg: {}", taskId, JsonConverter.toJson(agg));
			LOG.info("taskId: {}, AllReduceSum end", taskId);
		}
	}

	private static class AllReduceRecv<T> extends RichMapPartitionFunction <Tuple3 <Integer, Integer, double[]>, T> {
		private final String bufferName;
		private final String lengthName;
		private final int sessionId;

		AllReduceRecv(String bufferName, String lengthName, int sessionId) {
			this.bufferName = bufferName;
			this.lengthName = lengthName;
			this.sessionId = sessionId;
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Integer, Integer, double[]>> values, Collector <T> out)
			throws Exception {
			ComContext context = new ComContext(sessionId, getIterationRuntimeContext());
			Iterator <Tuple3 <Integer, Integer, double[]>> it = values.iterator();
			if (!it.hasNext()) {
				return;
			}
			double[] recvBuf = context.getObj(bufferName);
			int recvLen = lengthName != null ? context.getObj(lengthName) : recvBuf.length;
			int pieces = pieces(recvLen);
			do {
				Tuple3 <Integer, Integer, double[]> val = it.next();
				if (val.f1 == pieces - 1) {
					System.arraycopy(val.f2, 0, recvBuf, val.f1 * TRANSFER_BUFFER_SIZE, lastLen(recvLen));
				} else {
					System.arraycopy(val.f2, 0, recvBuf, val.f1 * TRANSFER_BUFFER_SIZE, TRANSFER_BUFFER_SIZE);
				}
			} while (it.hasNext());
		}
	}
}
