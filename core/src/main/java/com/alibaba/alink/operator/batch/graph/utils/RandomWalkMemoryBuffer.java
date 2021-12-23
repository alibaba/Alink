package com.alibaba.alink.operator.batch.graph.utils;

import java.util.concurrent.ArrayBlockingQueue;

public class RandomWalkMemoryBuffer {
	ArrayBlockingQueue <long[]> bufferedWalks;

	public RandomWalkMemoryBuffer(int cap) {
		bufferedWalks = new ArrayBlockingQueue <>(cap);
	}

	public void writeOneWalk(long[] walk) throws InterruptedException {
		bufferedWalks.put(walk);
	}

	public long[] readOneWalk() throws InterruptedException {
		return bufferedWalks.take();
	}

}
