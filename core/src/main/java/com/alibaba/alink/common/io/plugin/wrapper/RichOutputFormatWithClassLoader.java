package com.alibaba.alink.common.io.plugin.wrapper;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;

import java.io.IOException;

public class RichOutputFormatWithClassLoader extends RichOutputFormat <Row>
	implements InitializeOnMaster, FinalizeOnMaster {

	private static final long serialVersionUID = 604359344643992350L;

	private final ClassLoaderFactory factory;
	private final byte[] serializedOutputFormat;

	private transient OutputFormat <Row> outputFormat;

	public RichOutputFormatWithClassLoader(ClassLoaderFactory factory, final OutputFormat <Row> outputFormat) {
		this.factory = factory;
		this.outputFormat = outputFormat;

		try {
			serializedOutputFormat = InstantiationUtil.serializeObject(outputFormat);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private OutputFormat <Row> getOutputFormat() {
		if (outputFormat == null) {
			try {
				outputFormat = InstantiationUtil.deserializeObject(serializedOutputFormat, factory.create());
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		return outputFormat;
	}

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		if (getOutputFormat() instanceof RichOutputFormat) {
			factory.doAsThrowRuntime(() -> ((RichOutputFormat <Row>) getOutputFormat()).setRuntimeContext(t));
		} else {
			super.setRuntimeContext(t);
		}
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		if (getOutputFormat() instanceof RichOutputFormat) {
			return factory.doAsThrowRuntime(() -> ((RichOutputFormat <Row>) getOutputFormat()).getRuntimeContext());
		} else {
			return super.getRuntimeContext();
		}
	}

	@Override
	public void configure(Configuration parameters) {
		getOutputFormat().configure(parameters);
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		factory.doAsThrowRuntime(() -> getOutputFormat().open(taskNumber, numTasks));
	}

	@Override
	public void writeRecord(Row record) throws IOException {
		factory.doAsThrowRuntime(() -> getOutputFormat().writeRecord(record));
	}

	@Override
	public void close() throws IOException {
		factory.doAsThrowRuntime(() -> getOutputFormat().close());
	}

	@Override
	public void finalizeGlobal(int parallelism) throws IOException {
		if (getOutputFormat() instanceof FinalizeOnMaster) {
			factory.doAsThrowRuntime(() -> ((FinalizeOnMaster) getOutputFormat()).finalizeGlobal(parallelism));
		}
	}

	@Override
	public void initializeGlobal(int parallelism) throws IOException {
		if (getOutputFormat() instanceof InitializeOnMaster) {
			factory.doAsThrowRuntime(() -> ((InitializeOnMaster) getOutputFormat()).initializeGlobal(parallelism));
		}
	}
}
