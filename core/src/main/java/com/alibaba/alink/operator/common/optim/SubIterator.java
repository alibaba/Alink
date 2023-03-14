package com.alibaba.alink.operator.common.optim;

import java.util.Iterator;
import java.util.List;

public class SubIterator<T> implements Iterator <T>, Iterable <T> {
	final List <T> list;
	final int numSubs;
	final int curSub;
	final int listSize;
	int cursor;

	public SubIterator(List <T> list, int numSubs, int curSub) {
		this.list = list;
		this.numSubs = numSubs;
		this.curSub = curSub;
		this.listSize = list.size();
		cursor = curSub;
	}

	@Override
	public boolean hasNext() {
		return cursor < listSize;
	}

	@Override
	public T next() {
		T result = null;
		if (hasNext()) {
			result = list.get(cursor);
			cursor += numSubs;
		}
		return result;
	}

	@Override
	public Iterator <T> iterator() {
		return this;
	}
}
