package com.alibaba.alink.common.dl.utils;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;

public class DLLauncherUtils {
	public static Tuple2 <Integer, Integer> adjustNumWorkersPSs(Integer numWorkers, Integer numPSs, int parallelism) {
		if (numWorkers != null && numPSs != null) {    // no need to adjust
			return Tuple2.of(numWorkers, numPSs);
		}

		if (parallelism < 0) {
			throw new AkUnclassifiedErrorException(
				"Cannot decide #Workers and #PSs for TensorFlow task, as parallelism cannot be obtained.");
		}
		if (parallelism == 1) {
			return Tuple2.of(1, 0);
		}

		final int ratioNumWorkersOverNumPSs = 3;

		if (numWorkers == null && numPSs == null) {
			numPSs = Math.max(parallelism / (1 + ratioNumWorkersOverNumPSs), 1);
			numWorkers = parallelism - numPSs;
		} else if (numWorkers == null) {
			numWorkers = parallelism - numPSs;
		} else {    // numPSs == null
			numPSs = parallelism - numWorkers;
		}
		return Tuple2.of(numWorkers, numPSs);
	}
}
