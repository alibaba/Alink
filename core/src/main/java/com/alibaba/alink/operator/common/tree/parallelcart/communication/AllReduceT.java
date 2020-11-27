package com.alibaba.alink.operator.common.tree.parallelcart.communication;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CommunicateFunction;
import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.common.io.directreader.DistributedInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * An implement of {@link CommunicateFunction} that do the AllReduce.
 * <p>
 * AllReduce is a communication primitive widely used in MPI. In our implementation, all workers do reduce on
 * a partition of the whole data and they all get the final reduce result.
 * <p>
 * There're mainly three stages:
 * <ol>
 * <li>1. All workers send the there partial data to other workers for reduce.</li>
 * <li>2. All workers do reduce on all data it received and then send partial results to others.</li>
 * <li>3. All workers merge partial results into final result and put it into session context with pre-defined
 * object name.</li>
 * </ol>
 */
public class AllReduceT<B> extends CommunicateFunction {
	// always big object, so send the object one by one to worker.
	private static final int TRANSFER_BUFFER_SIZE = 1;
	private static final Logger LOG = LoggerFactory.getLogger(AllReduceT.class);
	private static final long serialVersionUID = -1081251494034109951L;
	private final String bufferName;
	private final String lengthName;
	private final SerializableBiConsumer <B[], B[]> op;
	private final Class <B> elementClass;

	/**
	 * Create an AllReduce communicate function.
	 *
	 * @param bufferName name of double[] object to perform all-reduce.
	 * @param lengthName object name of the length. Sub-range of [0, length) of the double array will be all-reduced.
	 * @param op         the all-reduce function.
	 */
	public AllReduceT(String bufferName, String lengthName, SerializableBiConsumer <B[], B[]> op,
					  Class <B> elementClass) {
		this.bufferName = bufferName;
		this.lengthName = lengthName;
		this.op = op;
		this.elementClass = elementClass;
	}

	public static <T, B> DataSet <T> allReduce(
		DataSet <T> input,
		final String bufferName,
		final String lengthName,
		final SerializableBiConsumer <B[], B[]> op,
		final int sessionId,
		final Class <B> elementClass) {
		final String transferBufferName = UUID.randomUUID().toString();

		TypeInformation <B[]> type = Types.OBJECT_ARRAY(Types.GENERIC(elementClass));

		return input
			.mapPartition(new AllReduceSend <>(bufferName, lengthName, transferBufferName, sessionId, elementClass))
			.withBroadcastSet(input, "barrier")
			.returns(
				new TupleTypeInfo <>(Types.INT, Types.INT, type)
			)
			.name("AllReduceSend")
			.partitionCustom(new Partitioner <Integer>() {
				private static final long serialVersionUID = -4605373044396957398L;

				@Override
				public int partition(Integer key, int numPartitions) {
					return key;
				}
			}, 0)
			.name("AllReduceBroadcastRaw")
			.mapPartition(new AllReduceSum <>(bufferName, lengthName, sessionId, op))
			.returns(
				new TupleTypeInfo <>(Types.INT, Types.INT, type)
			)
			.name("AllReduceSum")
			.partitionCustom(new Partitioner <Integer>() {
				private static final long serialVersionUID = -5499691435112716956L;

				@Override
				public int partition(Integer key, int numPartitions) {
					return key;
				}
			}, 0)
			.name("AllReduceBroadcastSum")
			.mapPartition(new AllReduceRecv <T, B>(bufferName, lengthName, sessionId))
			.returns(input.getType())
			.name("AllReduceRecv");
	}

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
		return allReduce(input, this.bufferName, this.lengthName, op, sessionId, elementClass);
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

	private static class AllReduceSend<T, B> extends RichMapPartitionFunction <T, Tuple3 <Integer, Integer, B[]>> {
		private static final long serialVersionUID = 7772757421079527212L;
		private final String bufferName;
		private final String lengthName;
		private final String transferBufferName;
		private final int sessionId;
		private final Class <B> elementClass;

		AllReduceSend(String bufferName, String lengthName, String transferBufferName, int sessionId,
					  Class <B> elementClass) {
			this.bufferName = bufferName;
			this.lengthName = lengthName;
			this.transferBufferName = transferBufferName;
			this.sessionId = sessionId;
			this.elementClass = elementClass;
		}

		@Override
		public void mapPartition(Iterable <T> values, Collector <Tuple3 <Integer, Integer, B[]>> out)
			throws Exception {
			if (getRuntimeContext().getNumberOfParallelSubtasks() == 1) {
				return;
			}
			ComContext context = new ComContext(sessionId, getIterationRuntimeContext());
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int numOfSubTasks = getRuntimeContext().getNumberOfParallelSubtasks();
			int iterNum = getIterationRuntimeContext().getSuperstepNumber();

			LOG.info("taskId: {}, AllReduceSend start", taskId);

			B[] sendBuf = context.getObj(bufferName);
			int sendLen = lengthName != null ? context.getObj(lengthName) : sendBuf.length;
			B[] transBuf;
			if (iterNum == 1) {
				transBuf = (B[]) Array.newInstance(elementClass, TRANSFER_BUFFER_SIZE);
				context.putObj(transferBufferName, transBuf);
			} else {
				transBuf = context.getObj(transferBufferName);
			}

			int pieces = pieces(sendLen);

			LOG.info("taskId: {}, len: {}, pieces: {}", taskId, sendLen, pieces);

			DistributedInfo distributedInfo = new DefaultDistributedInfo();

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
					out.collect(Tuple3.of(i, startPos + j, transBuf));
				}
			}

			LOG.info("taskId: {}, AllReduceSend end", taskId);
		}
	}

	private static class AllReduceSum<B> extends RichMapPartitionFunction <Tuple3 <Integer, Integer, B[]>,
		Tuple3 <Integer, Integer, B[]>> {
		private static final long serialVersionUID = -1513792018030661902L;
		private final String bufferName;
		private final String lengthName;
		private final int sessionId;
		private final SerializableBiConsumer <B[], B[]> op;

		AllReduceSum(String bufferName, String lengthName, int sessionId,
					 SerializableBiConsumer <B[], B[]> op) {
			this.bufferName = bufferName;
			this.lengthName = lengthName;
			this.sessionId = sessionId;
			this.op = op;
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Integer, Integer, B[]>> values,
								 Collector <Tuple3 <Integer, Integer, B[]>> out) {
			if (getRuntimeContext().getNumberOfParallelSubtasks() == 1) {
				return;
			}
			ComContext context = new ComContext(this.sessionId, getIterationRuntimeContext());

			Iterator <Tuple3 <Integer, Integer, B[]>> it = values.iterator();
			if (!it.hasNext()) {
				return;
			}

			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int numOfSubTasks = getRuntimeContext().getNumberOfParallelSubtasks();

			LOG.info("taskId: {}, AllReduceSum start", taskId);
			B[] sendBuf = context.getObj(bufferName);
			int sendLen = lengthName != null ? context.getObj(lengthName) : sendBuf.length;
			int pieces = pieces(sendLen);
			DistributedInfo distributedInfo = new DefaultDistributedInfo();

			int startPos = (int) distributedInfo.startPos(taskId, numOfSubTasks, pieces);
			int cnt = (int) distributedInfo.localRowCnt(taskId, numOfSubTasks, pieces);

			List <B[]> sum = new ArrayList <>(cnt);
			for (int i = 0; i < cnt; ++i) {
				sum.add(null);
			}

			do {
				Tuple3 <Integer, Integer, B[]> val = it.next();
				int localPos = val.f1 - startPos;
				if (sum.get(localPos) == null) {
					sum.set(localPos, val.f2);
				} else {
					op.accept(sum.get(localPos), val.f2);
				}
			} while (it.hasNext());

			for (int i = 0; i < numOfSubTasks; ++i) {
				for (int j = 0; j < cnt; ++j) {
					out.collect(Tuple3.of(i, startPos + j, sum.get(j)));
				}
			}

			LOG.info("taskId: {}, AllReduceSum end", taskId);
		}
	}

	private static class AllReduceRecv<T, B> extends RichMapPartitionFunction <Tuple3 <Integer, Integer, B[]>, T> {
		private static final long serialVersionUID = 2526497225558653932L;
		private final String bufferName;
		private final String lengthName;
		private final int sessionId;

		AllReduceRecv(String bufferName, String lengthName, int sessionId) {
			this.bufferName = bufferName;
			this.lengthName = lengthName;
			this.sessionId = sessionId;
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Integer, Integer, B[]>> values, Collector <T> out)
			throws Exception {
			if (getRuntimeContext().getNumberOfParallelSubtasks() == 1) {
				return;
			}
			ComContext context = new ComContext(sessionId, getIterationRuntimeContext());
			Iterator <Tuple3 <Integer, Integer, B[]>> it = values.iterator();
			if (!it.hasNext()) {
				return;
			}
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			LOG.info("taskId: {}, AllReduceRecv start", taskId);
			B[] recvBuf = context.getObj(bufferName);
			int recvLen = lengthName != null ? context.getObj(lengthName) : recvBuf.length;
			int pieces = pieces(recvLen);
			do {
				Tuple3 <Integer, Integer, B[]> val = it.next();
				if (val.f1 == pieces - 1) {
					System.arraycopy(val.f2, 0, recvBuf, val.f1 * TRANSFER_BUFFER_SIZE, lastLen(recvLen));
				} else {
					System.arraycopy(val.f2, 0, recvBuf, val.f1 * TRANSFER_BUFFER_SIZE, TRANSFER_BUFFER_SIZE);
				}
			} while (it.hasNext());
			LOG.info("taskId: {}, AllReduceRecv end", taskId);
		}
	}
}
