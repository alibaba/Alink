package com.alibaba.alink.operator.local.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.shared.HasNumThreads;

public class MTableParallelUtil {

	public static int getNumThreads(Params params) {
		if (params.contains(HasNumThreads.NUM_THREADS)) {
			return params.get(HasNumThreads.NUM_THREADS);
		}

		return LocalOperator.getDefaultNumThreads();
	}

	public interface TraverseRunnable {
		void run(MTable mTable, int index, int start, int end);
	}

	public static <T> void traverse(MTable mTable, final int numThreads, TraverseRunnable runnable) {
		final int numRow = mTable.getNumRow();

		TaskRunner taskRunner = new TaskRunner();

		for (int i = 0; i < numThreads; ++i) {
			final int index = i;
			final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(i, numThreads, numRow);
			final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(i, numThreads, numRow);

			taskRunner.submit(() -> runnable.run(mTable, index, start, start + cnt));
		}

		taskRunner.join();
	}
}
