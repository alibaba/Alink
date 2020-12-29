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

	private final KafkaClassLoaderFactory factory;
	private final byte[] serializedRichSinkFunction;

	private transient RichSinkFunction <Row> internal;

	public RichSinkFunctionWithClassLoader(KafkaClassLoaderFactory factory, RichSinkFunction <Row> internal) {
		this.factory = factory;
		this.internal = internal;

		try {
			serializedRichSinkFunction = InstantiationUtil.serializeObject(internal);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private RichSinkFunction <Row> getRichSinkFunction() {

		if (internal == null) {
			try {
				internal = InstantiationUtil.deserializeObject(serializedRichSinkFunction, factory.create());
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		return internal;
	}

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		factory.doAsThrowRuntime(() -> getRichSinkFunction().setRuntimeContext(t));
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		return factory.doAsThrowRuntime(getRichSinkFunction()::getRuntimeContext);
	}

	@Override
	public IterationRuntimeContext getIterationRuntimeContext() {
		return factory.doAsThrowRuntime(getRichSinkFunction()::getIterationRuntimeContext);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		factory.doAsThrowRuntime(() -> getRichSinkFunction().open(parameters));
	}

	@Override
	public void close() throws Exception {
		factory.doAsThrowRuntime(getRichSinkFunction()::close);
	}

	@Override
	public void invoke(Row value) throws Exception {
		factory.doAsThrowRuntime(() -> getRichSinkFunction().invoke(value));
	}

	@Override
	public void invoke(Row value, Context context) throws Exception {
		factory.doAsThrowRuntime(() -> getRichSinkFunction().invoke(value, context));
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (internal instanceof CheckpointListener) {
			factory.doAsThrowRuntime(() -> ((CheckpointListener) getRichSinkFunction()).notifyCheckpointComplete(checkpointId));
		} else {
			throw new IllegalStateException("Internal is not the CheckpointListener.");
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (internal instanceof CheckpointedFunction) {
			factory.doAsThrowRuntime(() -> ((CheckpointedFunction) getRichSinkFunction()).snapshotState(context));
		} else {
			throw new IllegalStateException("Internal is not the CheckpointedFunction.");
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		if (getRichSinkFunction() instanceof CheckpointedFunction) {
			factory.doAsThrowRuntime(() -> ((CheckpointedFunction) getRichSinkFunction()).initializeState(context));
		} else {
			throw new IllegalStateException("Internal is not the CheckpointedFunction.");
		}
	}
}
