package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;

import java.io.IOException;

public class RichParallelSourceFunctionWithClassLoader extends RichParallelSourceFunction <Row> implements
	CheckpointListener,
	ResultTypeQueryable <Row>,
	CheckpointedFunction {

	private final ClassLoaderFactory factory;
	private final byte[] serializedRichParallelSourceFunction;

	private transient RichParallelSourceFunction <Row> internal;

	public RichParallelSourceFunctionWithClassLoader(
		ClassLoaderFactory factory,
		RichParallelSourceFunction <Row> internal) {

		this.factory = factory;
		this.internal = internal;

		try {
			serializedRichParallelSourceFunction = InstantiationUtil.serializeObject(internal);
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException(e.getMessage(),e);
		}
	}

	private RichParallelSourceFunction <Row> getRichParallelSourceFunction() {

		if (internal == null) {
			try {
				internal = InstantiationUtil.deserializeObject(serializedRichParallelSourceFunction, factory.create());
			} catch (IOException | ClassNotFoundException e) {
				throw new AkUnclassifiedErrorException(e.getMessage(),e);
			}
		}

		return internal;
	}

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		factory.doAsThrowRuntime(() -> getRichParallelSourceFunction().setRuntimeContext(t));
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		return factory.doAsThrowRuntime(getRichParallelSourceFunction()::getRuntimeContext);
	}

	@Override
	public IterationRuntimeContext getIterationRuntimeContext() {
		return factory.doAsThrowRuntime(getRichParallelSourceFunction()::getIterationRuntimeContext);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		factory.doAsThrowRuntime(() -> getRichParallelSourceFunction().open(parameters));
	}

	@Override
	public void close() throws Exception {
		factory.doAsThrowRuntime(getRichParallelSourceFunction()::close);
	}

	@Override
	public TypeInformation <Row> getProducedType() {
		if (internal instanceof ResultTypeQueryable) {
			return factory.doAsThrowRuntime(
				((ResultTypeQueryable <Row>) getRichParallelSourceFunction())::getProducedType);
		} else {
			throw new IllegalStateException("Internal is not the ResultTypeQueryable.");
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (internal instanceof CheckpointListener) {
			factory.doAsThrowRuntime(() -> (CheckpointListener) getRichParallelSourceFunction())
				.notifyCheckpointComplete(checkpointId);
		} else {
			throw new IllegalStateException("Internal is not the CheckpointListener.");
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (internal instanceof CheckpointedFunction) {
			factory.doAsThrowRuntime(() -> (CheckpointedFunction) getRichParallelSourceFunction()).snapshotState(
				context);
		} else {
			throw new IllegalStateException("Internal is not the CheckpointedFunction.");
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		if (internal instanceof CheckpointedFunction) {
			factory.doAsThrowRuntime(() -> (CheckpointedFunction) getRichParallelSourceFunction()).initializeState(
				context);
		} else {
			throw new IllegalStateException("Internal is not the CheckpointedFunction.");
		}
	}

	@Override
	public void run(SourceContext <Row> ctx) throws Exception {
		factory.doAsThrowRuntime(() -> getRichParallelSourceFunction().run(ctx));
	}

	@Override
	public void cancel() {
		factory.doAsThrowRuntime(getRichParallelSourceFunction()::cancel);
	}
}
