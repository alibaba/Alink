package com.alibaba.alink.common.io.plugin.wrapper;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.util.InstantiationUtil;

import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

public class RichInputFormatGenericWithClassLoader<OT, T extends InputSplit> extends RichInputFormat <OT, InputSplit> {
	private static final long serialVersionUID = 604359344643992350L;

	private final ClassLoaderFactory factory;
	private final byte[] serializedInputFormat;

	private transient RichInputFormat <OT, T> inputFormat;

	public RichInputFormatGenericWithClassLoader(
		ClassLoaderFactory factory, RichInputFormat <OT, T> inputFormat) {

		this.factory = factory;
		this.inputFormat = inputFormat;

		try {
			serializedInputFormat = InstantiationUtil.serializeObject(inputFormat);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private RichInputFormat <OT, T> getInputFormat() {
		if (inputFormat == null) {
			try {
				inputFormat = InstantiationUtil.deserializeObject(serializedInputFormat, factory.create());
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		return inputFormat;
	}

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		super.setRuntimeContext(t);
		getInputFormat().setRuntimeContext(t);
	}

	@Override
	public void configure(Configuration parameters) {
		factory.doAsThrowRuntime(() -> getInputFormat().configure(parameters));
	}

	@Override
	public void openInputFormat() throws IOException {
		factory.doAsThrowRuntime(() -> getInputFormat().openInputFormat());
	}

	@Override
	public void closeInputFormat() throws IOException {
		factory.doAsThrowRuntime(() -> getInputFormat().closeInputFormat());
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return factory.doAsThrowRuntime(() -> getInputFormat().getStatistics(cachedStatistics));
	}

	@Override
	public InputSplitWithClassLoader[] createInputSplits(int minNumSplits) throws IOException {
		return factory.doAsThrowRuntime(() -> Arrays.stream(getInputFormat().createInputSplits(minNumSplits))
			.map(x -> new InputSplitWithClassLoader(factory, x))
			.toArray(InputSplitWithClassLoader[]::new));
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
		return factory.doAsThrowRuntime(() -> {
				T[] raw = (T[]) Array.newInstance(
					((InputSplitWithClassLoader) inputSplits[0]).getInputSplit().getClass(),
					inputSplits.length
				);

				for (int i = 0; i < inputSplits.length; ++i) {
					raw[i] = (T) ((InputSplitWithClassLoader) inputSplits[i]).getInputSplit();
				}

				return new InputSplitAssignerWithClassLoader(
					factory,
					getInputFormat().getInputSplitAssigner(raw)
				);
			}
		);
	}

	@Override
	public void open(InputSplit split) throws IOException {
		factory.doAsThrowRuntime(() -> getInputFormat().open((T) ((InputSplitWithClassLoader) split).getInputSplit()));
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return factory.doAsThrowRuntime(() -> getInputFormat().reachedEnd());
	}

	@Override
	public OT nextRecord(OT reuse) throws IOException {
		return factory.doAsThrowRuntime(() -> getInputFormat().nextRecord(reuse));
	}

	@Override
	public void close() throws IOException {
		factory.doAsThrowRuntime(() -> getInputFormat().close());
	}
}
