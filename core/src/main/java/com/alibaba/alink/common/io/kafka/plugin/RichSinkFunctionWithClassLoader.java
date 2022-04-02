package com.alibaba.alink.common.io.kafka.plugin;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class RichSinkFunctionWithClassLoader extends RichSinkFunction <Row>
	implements CheckpointedFunction, CheckpointListener {

	private KafkaClassLoaderFactory factory;
	private RichSinkFunction <Row> internal;

	public RichSinkFunctionWithClassLoader(KafkaClassLoaderFactory factory, RichSinkFunction <Row> internal) {
		this.factory = factory;
		this.internal = internal;
	}

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		factory.doAsThrowRuntime(() -> internal.setRuntimeContext(t));
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		return factory.doAsThrowRuntime(internal::getRuntimeContext);
	}

	@Override
	public IterationRuntimeContext getIterationRuntimeContext() {
		return factory.doAsThrowRuntime(internal::getIterationRuntimeContext);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		factory.doAsThrowRuntime(() -> internal.open(parameters));
	}

	@Override
	public void close() throws Exception {
		factory.doAsThrowRuntime(internal::close);
	}

	@Override
	public void invoke(Row value) throws Exception {
		factory.doAsThrowRuntime(() -> internal.invoke(value));
	}

	@Override
	public void invoke(Row value, Context context) throws Exception {
		factory.doAsThrowRuntime(() -> internal.invoke(value, context));
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (internal instanceof CheckpointListener) {
			factory.doAsThrowRuntime(() -> ((CheckpointListener) internal).notifyCheckpointComplete(checkpointId));
		} else {
			throw new IllegalStateException("Internal is not the CheckpointListener.");
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (internal instanceof CheckpointedFunction) {
			factory.doAsThrowRuntime(() -> ((CheckpointedFunction) internal).snapshotState(context));
		} else {
			throw new IllegalStateException("Internal is not the CheckpointedFunction.");
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		if (internal instanceof CheckpointedFunction) {
			factory.doAsThrowRuntime(() -> ((CheckpointedFunction) internal).initializeState(context));
		} else {
			throw new IllegalStateException("Internal is not the CheckpointedFunction.");
		}
	}

	private void writeObject(ObjectOutputStream stream)
		throws IOException {

		InstantiationUtil.serializeObject(stream, factory);
		InstantiationUtil.serializeObject(stream, internal);
	}

	private void readObject(ObjectInputStream stream)
		throws IOException, ClassNotFoundException {

		factory = InstantiationUtil.deserializeObject(stream, Thread.currentThread().getContextClassLoader());
		internal = InstantiationUtil.deserializeObject(stream, factory.create());
	}
}
