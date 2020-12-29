package com.alibaba.alink.common.io.catalog.plugin;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.util.InstantiationUtil;

import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class InputSplitWithClassLoader implements InputSplit {
	private static final long serialVersionUID = -7993725711297269105L;
	private final ClassLoaderFactory factory;
	private final byte[] serializedInputSplit;

	private transient InputSplit inputSplit;

	public InputSplitWithClassLoader(ClassLoaderFactory factory, InputSplit inputSplit) {
		this.factory = factory;
		this.inputSplit = inputSplit;

		try {
			serializedInputSplit = InstantiationUtil.serializeObject(inputSplit);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public InputSplit getInputSplit() {

		if (inputSplit == null) {
			try {
				inputSplit = InstantiationUtil.deserializeObject(serializedInputSplit, factory.create());
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		return inputSplit;
	}

	@Override
	public int getSplitNumber() {
		return getInputSplit().getSplitNumber();
	}
}
