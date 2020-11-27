package com.alibaba.alink.common.comqueue;

import org.apache.flink.api.java.DataSet;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

public class BaseComQueueTest extends AlinkTestBase implements Serializable {

	private static final long serialVersionUID = -1733941319857716986L;

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

		Assert.assertEquals(Integer.MAX_VALUE, queue.getMaxIter());
		Assert.assertEquals(5, queue.getQueue().size());
		Assert.assertTrue(queue.getQueue().get(0) instanceof ChainedComputation);
		Assert.assertTrue(queue.getQueue().get(1) instanceof MyCommunicateFunction);
		Assert.assertTrue(queue.getQueue().get(2) instanceof MyCommunicateFunction);
		Assert.assertTrue(queue.getQueue().get(3) instanceof ChainedComputation);
		Assert.assertTrue(queue.getQueue().get(4) instanceof MyCommunicateFunction);
		Assert.assertNull(queue.getCompareCriterion());
	}

	@Test
	public void optimize1() {
		IterativeComQueue queue = new IterativeComQueue();

		queue.exec();

		Assert.assertEquals(Integer.MAX_VALUE, queue.getMaxIter());
		Assert.assertEquals(1, queue.getQueue().size());
		Assert.assertTrue(queue.getQueue().get(0) instanceof BaseComQueue.DistributeData);
		Assert.assertNull(queue.getCompareCriterion());
	}

	@Test
	public void optimize2() {
		IterativeComQueue queue = new IterativeComQueue()
			.add(new MyComputeFunction());

		queue.exec();

		Assert.assertEquals(Integer.MAX_VALUE, queue.getMaxIter());
		Assert.assertEquals(1, queue.getQueue().size());
		Assert.assertTrue(queue.getQueue().get(0) instanceof ChainedComputation);
		Assert.assertNull(queue.getCompareCriterion());
	}

	@Test
	public void optimize3() {
		IterativeComQueue queue = new IterativeComQueue()
			.add(new MyCommunicateFunction());

		queue.exec();

		Assert.assertEquals(Integer.MAX_VALUE, queue.getMaxIter());
		Assert.assertEquals(2, queue.getQueue().size());
		Assert.assertTrue(queue.getQueue().get(0) instanceof BaseComQueue.DistributeData);
		Assert.assertTrue(queue.getQueue().get(1) instanceof MyCommunicateFunction);
		Assert.assertNull(queue.getCompareCriterion());
	}

	private static class MyComputeFunction extends ComputeFunction {
		private static final long serialVersionUID = -1829139615947353616L;

		@Override
		public void calc(ComContext context) {
			return;
		}
	}

	private static class MyCommunicateFunction extends CommunicateFunction {
		private static final long serialVersionUID = -8042022207911178708L;

		@Override
		public <T> DataSet <T> communicateWith(DataSet <T> input, int sessionId) {
			return input;
		}
	}
}