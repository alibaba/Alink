package com.alibaba.alink.operator.common.io.dummy;

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

/**
 * An {@link RichOutputFormat} which swallows all.
 *
 * @param <T>
 */
public class DummyOutputFormat<T> extends RichOutputFormat <T> implements InitializeOnMaster, FinalizeOnMaster {

	private static final long serialVersionUID = 6179356362975140981L;

	@Override
	public void finalizeGlobal(int i) throws IOException {

	}

	@Override
	public void initializeGlobal(int i) throws IOException {

	}

	@Override
	public void configure(Configuration configuration) {

	}

	@Override
	public void open(int i, int i1) throws IOException {

	}

	@Override
	public void writeRecord(T t) throws IOException {

	}

	@Override
	public void close() throws IOException {

	}
}
