package com.alibaba.alink.common.pyrunner;

import org.apache.flink.types.Row;

import com.alibaba.alink.testutil.categories.PyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PyMIMOCalcRunnerTest {

	@Category(PyTest.class)
	@Test
	public void testMultiThreadTiming() {
		Map <String, String> config = new HashMap <>();
		List <Row> rows = Arrays.asList(
			Row.of(),
			Row.of()
		);
		int n = 10;
		List <Thread> threads = new ArrayList <>();
		for (int i = 0; i < n; i += 1) {
			Thread t = new Thread(() -> {
				PyMIMOCalcRunner <SleepCalcHandler> runner =
					new PyMIMOCalcRunner <>("algo.misc.FactorialCalc", config::getOrDefault);
				runner.open();
				List <Row> output = runner.calc(rows);
				System.out.println(output);
				runner.close();
			});
			t.start();
			threads.add(t);
		}
		for (int i = 0; i < n; i += 1) {
			try {
				threads.get(i).join();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Category(PyTest.class)
	@Test
	public void testPlugin() {
		Map <String, String> config = new HashMap <>();
		List <Row> rows = Arrays.asList(
			Row.of(),
			Row.of()
		);

		PyMIMOCalcRunner <SleepCalcHandler> runner =
			new PyMIMOCalcRunner <>("algo.misc.FactorialCalc", config::getOrDefault);
		runner.open();
		List <Row> output = runner.calc(rows);
		System.out.println(output);
		runner.close();
	}

	public interface SleepCalcHandler extends PyMIMOCalcHandle {
	}
}
