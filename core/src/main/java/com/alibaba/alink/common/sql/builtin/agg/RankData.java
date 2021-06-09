package com.alibaba.alink.common.sql.builtin.agg;

public class RankData {

	long startNumber;
	long initialStartNumber = 0;
	long dataCount = 0;
	Number[] thisData;
	boolean sameOfLast = true;

	public RankData() {
		startNumber = initialStartNumber;
	}

	public RankData(long initial) {
		this();
		initialStartNumber = initial;
	}

	public void reset() {
		startNumber = initialStartNumber;
		dataCount = 0;
		thisData = null;
	}

	public void addData(Number... thisData) {
		++dataCount;
		if (this.thisData == null) {
			sameOfLast = false;
			this.thisData = thisData;
		} else {
			sameOfLast = setData(this.thisData, thisData);
		}
	}

	public void updateRank() {
		if (!sameOfLast) {
			startNumber = dataCount;
		}
	}

	public void updateDenseRank() {
		if (!sameOfLast) {
			++startNumber;
		}
	}

	public void updateRowNumber() {
		startNumber = dataCount;
	}

	private static boolean setData(Number[] a, Number[] b) {
		boolean same = true;
		for (int i = 0; i < a.length; i++) {
			int thesame = compareNumber(a[i], b[i]);
			if (thesame != 0) {
				same = false;
			}
			a[i] = b[i];
		}
		return same;
	}

	public long count() {
		return startNumber;
	}

	private static boolean compareArray(Number[] a, Number[] b) {
		if (a == null || b == null) {
			return a == null && b == null;
		}
		for (int i = 0; i < a.length; i++) {
			int res = compareNumber(a[i], b[i]);
			if (res != 0) {
				return false;
			}
		}
		return true;
	}

	private static int compareNumber(Number a, Number b) {
		return Double.compare(a.doubleValue(), b.doubleValue());
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof RankData)) {
			return false;
		}
		if (startNumber != ((RankData) o).startNumber) {
			return false;
		}
		if (initialStartNumber != ((RankData) o).initialStartNumber) {
			return false;
		}
		if (dataCount != ((RankData) o).dataCount) {
			return false;
		}
		if (sameOfLast != ((RankData) o).sameOfLast) {
			return false;
		}
		return compareArray(thisData, ((RankData) o).thisData);
	}
}

