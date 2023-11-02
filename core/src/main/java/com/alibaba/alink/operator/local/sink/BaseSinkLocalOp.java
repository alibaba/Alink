package com.alibaba.alink.operator.local.sink;

import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.exceptions.AkIllegalOperationException;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.IOTaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.utils.MTableSerializeLocalOp;
import com.alibaba.alink.operator.local.utils.TensorSerializeLocalOp;
import com.alibaba.alink.operator.local.utils.VectorSerializeLocalOp;
import com.alibaba.alink.params.shared.HasNumThreads;
import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.util.List;

/**
 * The base class for all data sinks.
 *
 * @param <T>
 */
@OutputPorts()
public abstract class BaseSinkLocalOp<T extends BaseSinkLocalOp <T>> extends LocalOperator <T> {

	protected BaseSinkLocalOp(Params params) {
		super(params);
	}

	@Override
	public T linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		return sinkFrom(in
			.link(new VectorSerializeLocalOp())
			.link(new MTableSerializeLocalOp())
			.link(new TensorSerializeLocalOp())
		);
	}

	protected abstract T sinkFrom(LocalOperator <?> in);

	@Override
	public final MTable getOutputTable() {
		throw new AkIllegalOperationException("Sink Operator has no output data.");
	}

	@Override
	protected final MTable[] getSideOutputTables() {
		throw new AkIllegalOperationException("Sink Operator has no side-output data.");
	}

	public static <T> void output(final List <T> input, final OutputFormat <T> outputFormat, Params params) {
		int numThreads = LocalOperator.getParallelism();

		//if (params.contains(HasNumThreads.NUM_THREADS)) {
		//	numThreads = params.get(HasNumThreads.NUM_THREADS);
		//}

		output(input, outputFormat, numThreads);
	}

	public static <T> void output(final List <T> input, final OutputFormat <T> outputFormat, final int numThreads) {

		if (outputFormat == null) {
			return;
		}

		final int numRows = input.size();

		final IOTaskRunner ioTaskRunner = new IOTaskRunner();

		try {
			// Todo: need to mock the configuration.
			outputFormat.configure(new Configuration());

			if (outputFormat instanceof InitializeOnMaster) {
				((InitializeOnMaster) outputFormat).initializeGlobal(numThreads);
			}

			final byte[] serialized = SerializationUtils.serialize(outputFormat);

			for (int i = 0; i < numThreads; ++i) {
				final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(i, numThreads, numRows);
				final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(i, numThreads, numRows);
				final int curThread = i;

				if (cnt <= 0) {continue;}

				ioTaskRunner.submit(() -> {
					OutputFormat <T> localOutputFormat = SerializationUtils.deserialize(serialized);

					// Todo: need to mock the configuration.
					localOutputFormat.configure(new Configuration());

					if (localOutputFormat instanceof RichOutputFormat) {
						//((RichOutputFormat <Row>) localOutputFormat)
						//	.setRuntimeContext(new LocalOutputFormatRuntimeContext());
					}

					try {
						localOutputFormat.open(curThread, numThreads);

						for (int j = start; j < start + cnt; ++j) {
							localOutputFormat.writeRecord(input.get(j));
						}

					} catch (IOException e) {
						if (localOutputFormat instanceof CleanupWhenUnsuccessful) {
							try {
								((CleanupWhenUnsuccessful) localOutputFormat).tryCleanupOnError();
							} catch (Exception sub) {
								sub.printStackTrace();
								// pass
							}
						}

						e.printStackTrace();

					} finally {
						try {
							localOutputFormat.close();
						} catch (Exception ex) {
							ex.printStackTrace();
						}
					}
					}
				);
			}

		} catch (IOException e) {
			throw new AkIllegalStateException("Output format error.", e);
		} finally {
			if (outputFormat instanceof FinalizeOnMaster) {
				try {
					((FinalizeOnMaster) outputFormat).finalizeGlobal(numThreads);
				} catch (IOException e) {
					// pass
				}
			}
		}

		ioTaskRunner.join();
	}

	///**
	// * Todo: need to mock the runtime context.
	// */
	//public static class LocalOutputFormatRuntimeContext implements RuntimeContext {
	//	@Override
	//	public String getTaskName() {
	//		return null;
	//	}
	//
	//	@Override
	//	public MetricGroup getMetricGroup() {
	//		return null;
	//	}
	//
	//	@Override
	//	public int getNumberOfParallelSubtasks() {
	//		return 0;
	//	}
	//
	//	@Override
	//	public int getMaxNumberOfParallelSubtasks() {
	//		return 0;
	//	}
	//
	//	@Override
	//	public int getIndexOfThisSubtask() {
	//		return 0;
	//	}
	//
	//	@Override
	//	public int getAttemptNumber() {
	//		return 0;
	//	}
	//
	//	@Override
	//	public String getTaskNameWithSubtasks() {
	//		return null;
	//	}
	//
	//	@Override
	//	public ExecutionConfig getExecutionConfig() {
	//		return null;
	//	}
	//
	//	@Override
	//	public ClassLoader getUserCodeClassLoader() {
	//		return null;
	//	}
	//
	//	@Override
	//	public <V, A extends Serializable> void addAccumulator(String name,
	//														   Accumulator <V, A> accumulator) {
	//
	//	}
	//
	//	@Override
	//	public <V, A extends Serializable> Accumulator <V, A> getAccumulator(String name) {
	//		return null;
	//	}
	//
	//	@Override
	//	public Map <String, Accumulator <?, ?>> getAllAccumulators() {
	//		return null;
	//	}
	//
	//	@Override
	//	public IntCounter getIntCounter(String name) {
	//		return null;
	//	}
	//
	//	@Override
	//	public LongCounter getLongCounter(String name) {
	//		return null;
	//	}
	//
	//	@Override
	//	public DoubleCounter getDoubleCounter(String name) {
	//		return null;
	//	}
	//
	//	@Override
	//	public Histogram getHistogram(String name) {
	//		return null;
	//	}
	//
	//	@Override
	//	public boolean hasBroadcastVariable(String name) {
	//		return false;
	//	}
	//
	//	@Override
	//	public <RT> List <RT> getBroadcastVariable(String name) {
	//		return null;
	//	}
	//
	//	@Override
	//	public <T, C> C getBroadcastVariableWithInitializer(String name,
	//														BroadcastVariableInitializer <T, C> initializer) {
	//		return null;
	//	}
	//
	//	@Override
	//	public DistributedCache getDistributedCache() {
	//		return null;
	//	}
	//
	//	@Override
	//	public <T> ValueState <T> getState(ValueStateDescriptor <T> stateProperties) {
	//		return null;
	//	}
	//
	//	@Override
	//	public <T> ListState <T> getListState(ListStateDescriptor <T> stateProperties) {
	//		return null;
	//	}
	//
	//	@Override
	//	public <T> ReducingState <T> getReducingState(
	//		ReducingStateDescriptor <T> stateProperties) {
	//		return null;
	//	}
	//
	//	@Override
	//	public <IN, ACC, OUT> AggregatingState <IN, OUT> getAggregatingState(
	//		AggregatingStateDescriptor <IN, ACC, OUT> stateProperties) {
	//		return null;
	//	}
	//
	//	@Override
	//	public <T, ACC> FoldingState <T, ACC> getFoldingState(
	//		FoldingStateDescriptor <T, ACC> stateProperties) {
	//		return null;
	//	}
	//
	//	@Override
	//	public <UK, UV> MapState <UK, UV> getMapState(
	//		MapStateDescriptor <UK, UV> stateProperties) {
	//		return null;
	//	}
	//}
}
