package com.alibaba.alink.operator.common.statistics.statistics;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class FrequencyIterator<T> {
	private final int capacity;
	private HashMap <T, Long> mapFreq;
	private boolean inRange = true;

	public FrequencyIterator(int capacity) {
		this.capacity = capacity;
		if (capacity <= 0 || capacity == Integer.MAX_VALUE) {
			throw new AkIllegalArgumentException("Wrong capacity value for Statistic Frequency.");
		}
		mapFreq = new HashMap <>(capacity + 1);
	}

	public void visit(T val) {
		if (inRange && null != val) {
			this.mapFreq.merge(val, 1L, Long::sum);
			if (mapFreq.size() > this.capacity) {
				this.inRange = false;
				this.mapFreq.clear();
			}
		}
	}

	public void finalResult(SummaryResultCol src) {
		if (inRange) {
			src.freq = new TreeMap <Object, Long>();
			Iterator <Entry <T, Long>> it = this.mapFreq.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry <T, Long> e = it.next();
				src.freq.put(e.getKey(), e.getValue());
			}
		} else {
			src.freq = null;
		}
	}
}
