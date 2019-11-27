package com.alibaba.alink.operator.common.tree.paralleltree;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class BufferPool {
	private static final int EXTEND_SIZE = 1024;
	private List <double[]> pool = new ArrayList <>();
	private BitSet inUse = new BitSet();
	private int curId = 0;
	private int bufLen;
	private int inUseLen = 0;

	public BufferPool(int bufLen) {
		this.bufLen = bufLen;

		extend(EXTEND_SIZE);
	}

	public double[] get(int id) {
		return pool.get(id);
	}

	public int nextValidId() {
		int len = inUseLen;
		int id = curId;

		if (!inUse.get(id)) {
			inUse.set(id, true);
			return id;
		}

		id = (id + 1) % len;

		while (id != curId && inUse.get(id)) {
			id = (id + 1) % len;
		}

		if (id == curId) {
			extend(EXTEND_SIZE);

			curId = len;
			inUse.set(curId, true);

			return curId;
		}

		curId = id;
		inUse.set(curId, true);

		return curId;
	}

	public void release(int id) {
		inUse.set(id, false);
	}

	private void extend(int cap) {
		int len = inUseLen;
		for (int i = 0; i < cap; ++i) {
			inUse.set(len + i, false);
			pool.add(new double[bufLen]);
		}

		inUseLen += cap;
	}
}
