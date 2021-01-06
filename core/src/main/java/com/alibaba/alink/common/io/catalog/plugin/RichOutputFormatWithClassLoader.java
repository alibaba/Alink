package com.alibaba.alink.common.io.catalog.plugin;

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class RichOutputFormatWithClassLoader implements OutputFormat <Row>,
	InitializeOnMaster, FinalizeOnMaster {

	private static final long serialVersionUID = 604359344643992350L;

	private ClassLoaderFactory factory;
	private OutputFormat <Row> outputFormat;

	public RichOutputFormatWithClassLoader(ClassLoaderFactory factory, final OutputFormat <Row> outputFormat) {
		this.factory = factory;
		this.outputFormat = outputFormat;
	}

	@Override
	public void configure(Configuration parameters) {
		outputFormat.configure(parameters);
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		factory.doAsThrowRuntime(() -> outputFormat.open(taskNumber, numTasks));
	}

	@Override
	public void writeRecord(Row record) throws IOException {
		factory.doAsThrowRuntime(() -> outputFormat.writeRecord(record));
	}

	@Override
	public void close() throws IOException {
		factory.doAsThrowRuntime(() -> outputFormat.close());
	}

	@Override
	public void finalizeGlobal(int parallelism) throws IOException {
		if (outputFormat instanceof FinalizeOnMaster) {
			factory.doAsThrowRuntime(() -> ((FinalizeOnMaster) outputFormat).finalizeGlobal(parallelism));
		}
	}

	private void writeObject(ObjectOutputStream stream)
		throws IOException {
		InstantiationUtil.serializeObject(stream, factory);
		InstantiationUtil.serializeObject(stream, outputFormat);
	}

	private void readObject(ObjectInputStream stream)
		throws IOException, ClassNotFoundException {
		factory = InstantiationUtil.deserializeObject(stream, Thread.currentThread().getContextClassLoader());
		outputFormat = InstantiationUtil.deserializeObject(stream, factory.create());
	}

	@Override
	public void initializeGlobal(int parallelism) throws IOException {
		if (outputFormat instanceof InitializeOnMaster) {
			factory.doAsThrowRuntime(() -> ((InitializeOnMaster) outputFormat).initializeGlobal(parallelism));
		}
	}
}
