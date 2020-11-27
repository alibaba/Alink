package com.alibaba.alink.common.io.catalog.plugin;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.util.InstantiationUtil;

import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class InputSplitWithClassLoader implements InputSplit {
	private static final long serialVersionUID = -7993725711297269105L;
	private ClassLoaderFactory factory;
	private InputSplit inputSplit;

	public InputSplitWithClassLoader(ClassLoaderFactory factory, InputSplit inputSplit) {
		this.factory = factory;
		this.inputSplit = inputSplit;
	}

	public InputSplit getInputSplit() {
		return inputSplit;
	}

	@Override
	public int getSplitNumber() {
		return inputSplit.getSplitNumber();
	}

	private void writeObject(ObjectOutputStream stream)
		throws IOException {
		InstantiationUtil.serializeObject(stream, factory);
		InstantiationUtil.serializeObject(stream, inputSplit);
	}

	private void readObject(ObjectInputStream stream)
		throws IOException, ClassNotFoundException {
		factory = InstantiationUtil.deserializeObject(stream, Thread.currentThread().getContextClassLoader());
		inputSplit = InstantiationUtil.deserializeObject(stream, factory.create());
	}
}
