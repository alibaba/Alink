package com.alibaba.alink.operator.common.pytorch;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ListSerializerTest extends AlinkTestBase {
	@Test
	public void testSerde() {
		List <?> inputs = new ArrayList <>(Arrays.asList(1, "a", new IntTensor(new int[] {1, 2})));
		ListSerializer listSerializer = new ListSerializer();
		byte[] bytes = listSerializer.serialize(inputs);
		List <?> outputs = listSerializer.deserialize(bytes);
		Assert.assertEquals(inputs.size(), outputs.size());
		for (int i = 0; i < inputs.size(); i += 1) {
			Assert.assertEquals(inputs.get(i), outputs.get(i));
		}
	}

	@Test
	public void testSerdeMultiThread() throws ExecutionException, InterruptedException {
		int numThreads = 100;
		ExecutorService executorService = new ThreadPoolExecutor(
			numThreads, numThreads, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue <>(numThreads));
		Future <?>[] futures = new Future[numThreads];
		for (int threadId = 0; threadId < numThreads; threadId += 1) {
			futures[threadId] = executorService.submit(new Runnable() {
				@Override
				public void run() {
					try {
						List <?> inputs = new ArrayList <>(Arrays.asList(1, "a", new IntTensor(new int[] {1, 2})));
						ListSerializer listSerializer = new ListSerializer();
						byte[] bytes = listSerializer.serialize(inputs);
						List <?> outputs = listSerializer.deserialize(bytes);
						Assert.assertEquals(inputs.size(), outputs.size());
						for (int i = 0; i < inputs.size(); i += 1) {
							Assert.assertEquals(inputs.get(i), outputs.get(i));
						}
					} catch (Exception e) {
						throw new AkUnclassifiedErrorException(e.getMessage(),e);
					}
				}
			});
		}
		for (int i = 0; i < numThreads; i += 1) {
			futures[i].get();
		}
	}
}
