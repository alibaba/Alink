package com.alibaba.alink.operator.batch.graph.walkpath;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.*;

public class RandomWalkPathEngineTest extends AlinkTestBase {

	private RandomWalkPathEngine getRandomWalkPathEngine(int walkLen) {
		Iterator <Long> vertexPool = Arrays.asList(1L, 2L, 3L).iterator();
		int numVertexPerBatch = 2, numWalksPerVertex = 5;
		RandomWalkPathEngine randomWalkPathEngine = new RandomWalkPathEngine(numVertexPerBatch, numWalksPerVertex,
			walkLen, vertexPool);
		return randomWalkPathEngine;
	}

	@Test
	public void testUpdatePathAndIsPathEnded() {
		int walkLen = 5;
		RandomWalkPathEngine randomWalkPathEngine = getRandomWalkPathEngine(walkLen);

		for (int i = 1; i < walkLen - 1; i++) {
			randomWalkPathEngine.updatePath(0, i);
			assertEquals(randomWalkPathEngine.canOutput(0), false);
		}

		randomWalkPathEngine.updatePath(0, 4);
		assertEquals(randomWalkPathEngine.canOutput(0), true);

		long[] path = randomWalkPathEngine.getOneWalkAndAddNewWalk(0);
		assertArrayEquals(path, new long[] {1, 1, 2, 3, 4});

	}

	@Test
	public void testUpdatePathAndIsPathEndedWithMinus1() {
		int walkLen = 5;
		RandomWalkPathEngine randomWalkPathEngine = getRandomWalkPathEngine(walkLen);

		randomWalkPathEngine.updatePath(0, -1);
		assertEquals(randomWalkPathEngine.canOutput(0), true);

		long[] path = randomWalkPathEngine.getOneWalkAndAddNewWalk(0);
		assertArrayEquals(path, new long[] {1});

	}

	@Test
	public void testGetOneWalkAndAddNewWalk() {
		int walkLen = 5;
		RandomWalkPathEngine randomWalkPathEngine = getRandomWalkPathEngine(walkLen);
		for (int i = 1; i < walkLen; i++) {
			randomWalkPathEngine.updatePath(0, i);
		}
		long[] finishedRandomWalk = randomWalkPathEngine.getOneWalkAndAddNewWalk(0);
		assertArrayEquals(finishedRandomWalk, new long[] {1, 1, 2, 3, 4});
	}

	@Test
	public void testGetOneWalkAndAddNewWalkWithMinus1() {
		int walkLen = 5;
		RandomWalkPathEngine randomWalkPathEngine = getRandomWalkPathEngine(walkLen);
		randomWalkPathEngine.updatePath(0, -1);
		long[] finishedRandomWalk = randomWalkPathEngine.getOneWalkAndAddNewWalk(0);
		assertArrayEquals(finishedRandomWalk, new long[] {1});
	}

	@Test
	public void testGetNextBatchOfVerticesToSampleFrom() {
		int walkLen = 5;
		RandomWalkPathEngine randomWalkPathEngine = getRandomWalkPathEngine(walkLen);

		// get vertices to sample from
		long[] verticesToSampleFrom = randomWalkPathEngine.getNextBatchOfVerticesToSampleFrom();
		assertArrayEquals(verticesToSampleFrom, new long[] {1, 1, 1, 1, 1, 2, 2, 2, 2, 2});

		for (int i = 1; i < walkLen; i++) {
			randomWalkPathEngine.updatePath(0, i);
		}
		randomWalkPathEngine.getOneWalkAndAddNewWalk(0);
		verticesToSampleFrom = randomWalkPathEngine.getNextBatchOfVerticesToSampleFrom();
		assertArrayEquals(verticesToSampleFrom, new long[] {3, 1, 1, 1, 1, 2, 2, 2, 2, 2});

		randomWalkPathEngine.updatePath(1, -1);
		randomWalkPathEngine.getOneWalkAndAddNewWalk(1);
		verticesToSampleFrom = randomWalkPathEngine.getNextBatchOfVerticesToSampleFrom();
		assertArrayEquals(verticesToSampleFrom, new long[] {3, 3, 1, 1, 1, 2, 2, 2, 2, 2});
	}

	@Test
	public void testGetNextVertexToSampleFrom() {
		int walkLen = 5;
		RandomWalkPathEngine randomWalkPathEngine = getRandomWalkPathEngine(walkLen);
		long vertexToSampleFrom = randomWalkPathEngine.getNextVertexToSampleFrom(0);
		assertTrue(vertexToSampleFrom == 1);

		randomWalkPathEngine.updatePath(0, -1);
		randomWalkPathEngine.getOneWalkAndAddNewWalk(0);
		vertexToSampleFrom = randomWalkPathEngine.getNextVertexToSampleFrom(0);
		assertTrue(vertexToSampleFrom == 3);
	}

}