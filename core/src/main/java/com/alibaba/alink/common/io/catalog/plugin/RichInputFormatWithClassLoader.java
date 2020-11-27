package com.alibaba.alink.common.io.catalog.plugin;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.util.InstantiationUtil;

import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.Arrays;

public class RichInputFormatWithClassLoader<T> extends RichInputFormat <T, InputSplit> {
	private static final long serialVersionUID = 604359344643992350L;

	private ClassLoaderFactory factory;
	private RichInputFormat <T, InputSplit> inputFormat;

	public RichInputFormatWithClassLoader(ClassLoaderFactory factory,
										  RichInputFormat <T, InputSplit> inputFormat) {
		this.factory = factory;
		this.inputFormat = inputFormat;
	}

	@Override
	public void configure(Configuration parameters) {
		factory.doAsThrowRuntime(() -> inputFormat.configure(parameters));
	}

	@Override
	public void openInputFormat() throws IOException {
		factory.doAsThrowRuntime(() -> inputFormat.openInputFormat());
	}

	@Override
	public void closeInputFormat() throws IOException {
		factory.doAsThrowRuntime(() -> inputFormat.closeInputFormat());
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return factory.doAsThrowRuntime(() -> inputFormat.getStatistics(cachedStatistics));
	}

	@Override
	public InputSplitWithClassLoader[] createInputSplits(int minNumSplits) throws IOException {
		return factory.doAsThrowRuntime(() -> Arrays.stream(inputFormat.createInputSplits(minNumSplits))
			.map(x -> new InputSplitWithClassLoader(factory, x))
			.toArray(InputSplitWithClassLoader[]::new));
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
		return factory.doAsThrowRuntime(() -> {
				InputSplit[] raw = (InputSplit[]) Array.newInstance(
					((InputSplitWithClassLoader) inputSplits[0]).getInputSplit().getClass(),
					inputSplits.length
				);

				for (int i = 0; i < inputSplits.length; ++i) {
					raw[i] = ((InputSplitWithClassLoader) inputSplits[i]).getInputSplit();
				}

				return new InputSplitAssignerWithClassLoader(
					factory,
					inputFormat.getInputSplitAssigner(raw)
				);
			}
		);
	}

	@Override
	public void open(InputSplit split) throws IOException {
		factory.doAsThrowRuntime(() -> inputFormat.open(((InputSplitWithClassLoader) split).getInputSplit()));
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return factory.doAsThrowRuntime(() -> inputFormat.reachedEnd());
	}

	@Override
	public T nextRecord(T reuse) throws IOException {
		return factory.doAsThrowRuntime(() -> inputFormat.nextRecord(reuse));
	}

	@Override
	public void close() throws IOException {
		factory.doAsThrowRuntime(() -> inputFormat.close());
	}

	private void writeObject(ObjectOutputStream stream)
		throws IOException {
		InstantiationUtil.serializeObject(stream, factory);
		InstantiationUtil.serializeObject(stream, inputFormat);
	}

	private void readObject(ObjectInputStream stream)
		throws IOException, ClassNotFoundException {
		factory = InstantiationUtil.deserializeObject(stream, Thread.currentThread().getContextClassLoader());
		inputFormat = InstantiationUtil.deserializeObject(stream, factory.create());
	}
}
