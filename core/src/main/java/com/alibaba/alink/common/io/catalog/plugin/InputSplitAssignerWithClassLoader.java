package com.alibaba.alink.common.io.catalog.plugin;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;

import java.util.ArrayList;
import java.util.List;

public class InputSplitAssignerWithClassLoader implements InputSplitAssigner {
	private final ClassLoaderFactory factory;
	private final InputSplitAssigner assigner;

	public InputSplitAssignerWithClassLoader(ClassLoaderFactory factory, InputSplitAssigner assigner) {
		this.factory = factory;
		this.assigner = assigner;
	}

	@Override
	public InputSplit getNextInputSplit(String host, int taskId) {
		try {
			return factory.doAs(() -> {
				InputSplit raw = assigner.getNextInputSplit(host, taskId);
				return raw == null ? null : new InputSplitWithClassLoader(factory, raw);
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void returnInputSplit(List <InputSplit> splits, int taskId) {
		try {
			factory.doAs(() -> {
				List <InputSplit> newSplit = new ArrayList <>(splits.size());

				for (InputSplit inputSplit : splits) {
					newSplit.add(((InputSplitWithClassLoader) inputSplit).getInputSplit());
				}

				assigner.returnInputSplit(newSplit, taskId);
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
