package com.alibaba.alink.operator.common.tree.parallelcart.communication;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CommunicateFunction;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.common.io.directreader.DistributedInfo;
import com.alibaba.alink.operator.common.tree.parallelcart.fakeserializer.FakeDoublePrimitiveArrayType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

public class ReduceScatter extends CommunicateFunction {
	/**
	 * the structure of the serialized Tuple<Integer, Integer, double[]> is
	 * |------|-----|--------------------|-------------|
	 * | int  | int | the array size(int)| double array|
	 * |------|-----|--------------------|-------------|
	 *
	 * and when serialized to buffer, it becomes
	 * |------------------------------------|------|-----|--------------------|-------------|
	 * |the length of serialized buffer(int)| int  | int | the array size(int)| double array|
	 * |------------------------------------|------|-----|--------------------|-------------|
	 *
	 * so 4 + 4 + 4 + 4 + 4094 * 8 = 32768 bytes (32KB as the default value of taskmanager.memory.segment-size)
	 *
	 * it is perfect. :)
	 */
	public static final int TRANSFER_BUFFER_SIZE = 4094;
	private static final Logger LOG = LoggerFactory.getLogger(ReduceScatter.class);
	private static final long serialVersionUID = -5868324335725015627L;
	private final String bufferName;
	private final String recvbufName;
	private final String recvcntsName;
	private final AllReduce.SerializableBiConsumer <double[], double[]> op;

	public ReduceScatter(
		String bufferName,
		String recvbufName,
		String recvcntsName,
		AllReduce.SerializableBiConsumer <double[], double[]> op) {
		this.bufferName = bufferName;
		this.op = op;
		this.recvbufName = recvbufName;
		this.recvcntsName = recvcntsName;
	}

	@Override
	public <T> DataSet <T> communicateWith(DataSet <T> input, int sessionId) {
		final String transferBufferName = UUID.randomUUID().toString();
		return input
			.mapPartition(new ReduceSend <T>(bufferName, recvcntsName, transferBufferName, sessionId))
			.withBroadcastSet(input, "barrier")
			.returns(
				new TupleTypeInfo <>(Types.INT, Types.INT,
					FakeDoublePrimitiveArrayType.FAKE_DOUBLE_PRIMITIVE_ARRAY_TYPE))
			.name("ReduceSend")
			.partitionCustom(new Partitioner <Integer>() {
				private static final long serialVersionUID = 3268524067347394279L;

				@Override
				public int partition(Integer key, int numPartitions) {
					return key;
				}
			}, 0)
			.name("ReduceBroadcastRaw")
			.mapPartition(new ReduceSum(recvcntsName, sessionId, op))
			.returns(
				new TupleTypeInfo <>(Types.INT, Types.INT,
					FakeDoublePrimitiveArrayType.FAKE_DOUBLE_PRIMITIVE_ARRAY_TYPE))
			.name("ReduceSum")
			.partitionCustom(new Partitioner <Integer>() {
				private static final long serialVersionUID = 4153050313317398157L;

				@Override
				public int partition(Integer key, int numPartitions) {
					return key;
				}
			}, 0)
			.name("ReduceBroadcastSum")
			.mapPartition(new ScatterRecv <T>(recvbufName, recvcntsName, sessionId))
			.returns(input.getType())
			.name("ScatterRecv");
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

	private static class ReduceSend<T> extends RichMapPartitionFunction <T, Tuple3 <Integer, Integer, double[]>> {
		private static final long serialVersionUID = -1122497764898022914L;
		private final String bufferName;
		private final String transferBufferName;
		private final String recvcntsName;
		private final int sessionId;
		private Tuple3 <Integer, Integer, double[]> outputBuf;
		private DistributedInfo distributedInfo;

		ReduceSend(String bufferName, String recvcntsName, String transferBufferName, int sessionId) {
			this.bufferName = bufferName;
			this.transferBufferName = transferBufferName;
			this.recvcntsName = recvcntsName;
			this.sessionId = sessionId;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			outputBuf = new Tuple3 <>();
			distributedInfo = new DefaultDistributedInfo();
		}

		@Override
		public void mapPartition(Iterable <T> values, Collector <Tuple3 <Integer, Integer, double[]>> out)
			throws Exception {
			if (getRuntimeContext().getNumberOfParallelSubtasks() == 1) {
				return;
			}
			ComContext context = new ComContext(sessionId, getIterationRuntimeContext());
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int iterNum = getIterationRuntimeContext().getSuperstepNumber();

			LOG.info("taskId: {}, ReduceSend start", taskId);
			double[] sendBuf = context.getObj(bufferName);
			int[] recvcnts = context.getObj(recvcntsName);

			int totalPieces = 0;
			for (int recvcnt : recvcnts) {
				totalPieces += pieces(recvcnt);
			}

			double[] transBuf;
			if (iterNum == 1) {
				transBuf = new double[TRANSFER_BUFFER_SIZE];
				context.putObj(transferBufferName, transBuf);
			} else {
				transBuf = context.getObj(transferBufferName);
			}

			int piecesAgg = 0;
			int sendOffset = 0;
			for (int i = 0; i < recvcnts.length; ++i) {
				int curPieces = pieces(recvcnts[i]);

				for (int j = 0; j < curPieces; ++j) {
					if (j == curPieces - 1) {
						int len = lastLen(recvcnts[i]);
						System.arraycopy(sendBuf, sendOffset, transBuf, 0, len);
						sendOffset += len;
					} else {
						System.arraycopy(sendBuf, sendOffset, transBuf, 0, TRANSFER_BUFFER_SIZE);
						sendOffset += TRANSFER_BUFFER_SIZE;
					}
					outputBuf.setFields(
						(int) distributedInfo.where(piecesAgg, context.getNumTask(), totalPieces),
						piecesAgg,
						transBuf
					);
					out.collect(outputBuf);
					piecesAgg++;
				}
			}

			LOG.info("taskId: {}, ReduceSend end", taskId);
		}
	}

	private static class ReduceSum extends RichMapPartitionFunction <Tuple3 <Integer, Integer, double[]>,
		Tuple3 <Integer, Integer, double[]>> {
		private static final long serialVersionUID = 8374168645659944661L;
		private final String recvcntsName;
		private final int sessionId;
		private final AllReduce.SerializableBiConsumer <double[], double[]> op;
		private int[] offset;
		ArrayList <double[]> sum;
		Tuple3 <Integer, Integer, double[]> outBuf;
		DistributedInfo distributedInfo = new DefaultDistributedInfo();

		ReduceSum(String recvcntsName, int sessionId,
				  AllReduce.SerializableBiConsumer <double[], double[]> op) {
			this.sessionId = sessionId;
			this.recvcntsName = recvcntsName;
			this.op = op;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Integer, Integer, double[]>> values,
								 Collector <Tuple3 <Integer, Integer, double[]>> out) {
			if (getRuntimeContext().getNumberOfParallelSubtasks() == 1) {
				return;
			}

			ComContext context = new ComContext(this.sessionId, getIterationRuntimeContext());
			if (context.getStepNo() == 1) {
				offset = new int[getRuntimeContext().getNumberOfParallelSubtasks()];
				sum = new ArrayList <>();
				outBuf = new Tuple3 <>();
			}

			Iterator <Tuple3 <Integer, Integer, double[]>> it = values.iterator();
			if (!it.hasNext()) {
				return;
			}

			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int numOfSubTasks = getRuntimeContext().getNumberOfParallelSubtasks();

			LOG.info("taskId: {}, ReduceSum start", taskId);
			int[] recvcnts = context.getObj(recvcntsName);

			int totalPieces = 0;

			for (int i = 0; i < recvcnts.length; ++i) {
				offset[i] = totalPieces;
				totalPieces += pieces(recvcnts[i]);
			}

			int startPos = (int) distributedInfo.startPos(taskId, numOfSubTasks, totalPieces);
			int cnt = (int) distributedInfo.localRowCnt(taskId, numOfSubTasks, totalPieces);

			LOG.info("taskId: {}, ReduceSum cnt: {}", taskId, cnt);
			int sumSize = sum.size();
			if (sumSize < cnt) {
				sum.ensureCapacity(cnt * 2);

				for (int i = sumSize; i < cnt * 2; ++i) {
					sum.add(new double[TRANSFER_BUFFER_SIZE]);
				}
			}

			for (int i = 0; i < cnt; ++i) {
				Arrays.fill(sum.get(i), 0.0);
			}

			long timer = 0;
			do {
				Tuple3 <Integer, Integer, double[]> val = it.next();
				long start = System.currentTimeMillis();
				int localPos = val.f1 - startPos;
				op.accept(sum.get(localPos), val.f2);
				long end = System.currentTimeMillis();
				timer += end - start;
			} while (it.hasNext());

			LOG.info("taskId: {}, ReduceSum time: {}", taskId, timer);

			timer = 0;
			for (int i = 0; i < cnt; ++i) {
				long start = System.currentTimeMillis();
				int toTaskId = Arrays.binarySearch(offset, startPos + i);

				toTaskId = toTaskId >= 0 ? toTaskId : -toTaskId - 2;

				while (toTaskId < numOfSubTasks && recvcnts[toTaskId] == 0) {
					toTaskId++;
				}

				if (recvcnts[toTaskId] == 0) {
					throw new IllegalStateException(
						"It should not be empty in the scatter task. Maybe it is an issue."
					);
				}
				long end = System.currentTimeMillis();
				timer += end - start;
				outBuf.setFields(toTaskId, startPos + i - offset[toTaskId], sum.get(i));
				out.collect(outBuf);
			}
			LOG.info("taskId: {}, ScatterSend time: {}", taskId, timer);
			LOG.info("taskId: {}, ReduceSum end", taskId);
		}
	}

	private static class ScatterRecv<T> extends RichMapPartitionFunction <Tuple3 <Integer, Integer, double[]>, T> {
		private static final long serialVersionUID = 5441679946742930637L;
		private final String bufferName;
		private final String recvcntsName;
		private final int sessionId;

		ScatterRecv(String bufferName, String recvcntsName, int sessionId) {
			this.bufferName = bufferName;
			this.recvcntsName = recvcntsName;
			this.sessionId = sessionId;
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Integer, Integer, double[]>> values, Collector <T> out)
			throws Exception {
			if (getRuntimeContext().getNumberOfParallelSubtasks() == 1) {
				return;
			}
			ComContext context = new ComContext(sessionId, getIterationRuntimeContext());
			Iterator <Tuple3 <Integer, Integer, double[]>> it = values.iterator();
			if (!it.hasNext()) {
				return;
			}
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			LOG.info("taskId: {}, ScatterRecv start", taskId);
			double[] recvBuf = context.getObj(bufferName);
			int[] recvcnts = context.getObj(recvcntsName);
			int len = recvcnts[context.getTaskId()];
			int pieces = pieces(len);
			do {
				Tuple3 <Integer, Integer, double[]> val = it.next();

				if (val.f1 == pieces - 1) {
					System.arraycopy(val.f2, 0, recvBuf, val.f1 * TRANSFER_BUFFER_SIZE, lastLen(len));
				} else {
					System.arraycopy(val.f2, 0, recvBuf, val.f1 * TRANSFER_BUFFER_SIZE, TRANSFER_BUFFER_SIZE);
				}
			} while (it.hasNext());
			LOG.info("taskId: {}, ScatterRecv end", taskId);
		}
	}
}
