package com.alibaba.alink.common.comqueue;

import org.apache.flink.api.java.DataSet;

import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

public class BaseComQueueTest implements Serializable {

	@Test
	public void optimize() {
		IterativeComQueue queue = new IterativeComQueue()
			.add(new MyComputeFunction())
			.add(new MyComputeFunction())
			.add(new MyCommunicateFunction())
			.add(new MyCommunicateFunction())
			.add(new MyComputeFunction())
			.add(new MyComputeFunction())
			.add(new MyCommunicateFunction());

		queue.exec();

		Assert.assertTrue(
			queue.toString().matches(
				"\\{\"completeResult\":null,\"maxIter\":2147483647,\"sessionId\":[0-9]*" +
					",\"queue\":\"ChainedComputation,MyCommunicateFunction,MyCommunicateFunction," +
					"ChainedComputation,MyCommunicateFunction\",\"compareCriterion\":null\\}"));
	}

	@Test
	public void optimize1() {
		IterativeComQueue queue = new IterativeComQueue();

		queue.exec();

		Assert.assertTrue(queue.toString().matches(
			"\\{\"completeResult\":null,\"maxIter\":2147483647,\"sessionId\":[0-9]*,\"queue\":\"\"" +
				",\"compareCriterion\":null\\}"));
	}

	@Test
	public void optimize2() {
		IterativeComQueue queue = new IterativeComQueue()
			.add(new MyComputeFunction());

		queue.exec();

		Assert.assertTrue(queue.toString().matches(
			"\\{\"completeResult\":null,\"maxIter\":2147483647,\"sessionId\":[0-9]*,\"queue\":\"MyComputeFunction\"" +
				",\"compareCriterion\":null\\}"));
	}

	@Test
	public void optimize3() {
		IterativeComQueue queue = new IterativeComQueue()
			.add(new MyCommunicateFunction());

		queue.exec();

		Assert.assertTrue(queue.toString().matches(
			"\\{\"completeResult\":null,\"maxIter\":2147483647,\"sessionId\":[0-9]*,\"queue\":\"MyCommunicateFunction\"" +
				",\"compareCriterion\":null\\}"));
	}

	private static class MyComputeFunction extends ComputeFunction {
		@Override
		public void calc(ComContext context) {
			return;
		}
	}

	private static class MyCommunicateFunction extends CommunicateFunction {
		@Override
		public <T> DataSet <T> communicateWith(DataSet <T> input, int sessionId) {
			return input;
		}
	}
}