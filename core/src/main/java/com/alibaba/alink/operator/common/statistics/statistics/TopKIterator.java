package com.alibaba.alink.operator.common.statistics.statistics;

import com.alibaba.alink.operator.common.statistics.statistics.SummaryResultCol;

import java.util.Comparator;
import java.util.PriorityQueue;

public class TopKIterator<T extends Comparable <T>> {

	final int largeK;
	final int smallK;
	PriorityQueue <T> priqueL;
	PriorityQueue <T> priqueS;

	public TopKIterator(int smallK, int largeK) {
		this.largeK = largeK;
		this.smallK = smallK;
		priqueL = new PriorityQueue <>(largeK + 1);
		priqueS = new PriorityQueue <>(smallK + 1,
			new Comparator <T>() {
				@Override
				public int compare(T o1, T o2) {
					return -o1.compareTo(o2);
				}
			}
		);
	}

	public void visit(T obj) {
		if (obj == null) {
			return;
		}
		if (priqueL.size() < largeK) {
			priqueL.add(obj);
		} else {
			priqueL.add(obj);
			priqueL.poll();
		}
		if (priqueS.size() < smallK) {
			priqueS.add(obj);
		} else {
			priqueS.add(obj);
			priqueS.poll();
		}
	}

	public void finalResult(SummaryResultCol src) {
		int large = priqueL.size();
		int small = priqueS.size();
		src.topItems = new Object[large];
		src.bottomItems = new Object[small];
		for (int i = 0; i < small; i++) {
			src.bottomItems[small - i - 1] = priqueS.poll();
		}
		for (int i = 0; i < large; i++) {
			src.topItems[large - i - 1] = priqueL.poll();
		}
	}

}
