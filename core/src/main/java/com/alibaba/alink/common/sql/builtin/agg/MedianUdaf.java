package com.alibaba.alink.common.sql.builtin.agg;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.sql.builtin.agg.MedianUdaf.MedianData;

import java.util.Comparator;
import java.util.PriorityQueue;

public class MedianUdaf extends BaseUdaf<Number, MedianData> {
	boolean excludeLast = false;

	public MedianUdaf() {}

	public MedianUdaf(boolean excludeLast) {
		this.excludeLast = excludeLast;
	}

	@Override
	public void accumulate(MedianData acc, Object... values) {
		if (values[0] == null) {
			return;
		}
		acc.addData((Number) values[0]);
	}

	@Override
	public void resetAccumulator(MedianData acc) {
		acc.reset();
	}

	@Override
	public void retract(MedianData acc, Object... values) {
		if (values[0] == null) {
			return;
		}
		acc.removeData(((Number) values[0]).doubleValue());
	}

	@Override
	public void merge(MedianData acc, Iterable <MedianData> it) {
		for (MedianData medianData : it) {
			for (Double data : medianData.maxHeap) {
				acc.addData(data);
			}
			for (Double data : medianData.minHeap) {
				acc.addData(data);
			}
		}
	}

	@Override
	public Number getValue(MedianData accumulator) {
		return accumulator.getMedian();
	}

	@Override
	public MedianData createAccumulator() {
		return new MedianData(excludeLast);
	}

	public static class MedianData {
		PriorityQueue <Double> minHeap = new PriorityQueue<>();
		PriorityQueue <Double> maxHeap = new PriorityQueue<>(new Comparator <Double>() {
			@Override
			public int compare(Double i1,Double i2){
				if (i2 >= i1) {
					return 1;
				} else {
					return -1;
				}
			}
		});

		private boolean excludeLast;
		private Double thisData = null;

		NumberTypeHandle handle = null;

		public MedianData() {}

		public MedianData(boolean excludeLast) {
			this.excludeLast = excludeLast;
		}

		public void addData(Number data) {
			if (handle == null) {
				handle = new NumberTypeHandle(data);
			}
			double doubleData = data.doubleValue();
			if (excludeLast) {
				if (!(thisData == null)) {
					addPrivateData(thisData);
				}
				thisData = doubleData;
			} else {
				addPrivateData(doubleData);
			}
		}

		private void addPrivateData(double data) {
			if (maxHeap.size() == 0) {
				maxHeap.add(data);
			} else if (data > maxHeap.peek()) {
				minHeap.add(data);
				reBalance(minHeap, maxHeap);
			}  else {
				maxHeap.add(data);
				reBalance(maxHeap, minHeap);
			}
		}

		public void removeData(double data) {
			if (!minHeap.contains(data) && !maxHeap.contains(data)) {
				if (excludeLast && thisData == null) {
					throw new AkIllegalDataException("No data to retract.");
				}
				thisData = null;
				return;
			}
			if (minHeap.size() == 0) {
				maxHeap.remove(data);
			} else if (minHeap.peek() <= data) {
				minHeap.remove(data);
				reBalance(maxHeap, minHeap);
			} else {
				maxHeap.remove(data);
				reBalance(minHeap, maxHeap);
			}
		}

		public Number getMedian() {
			if (minHeap.size() == 0 && maxHeap.size() == 0) {
				return handle.transformData(0.0);
			}
			double res;
			if (minHeap.size() > maxHeap.size()) {
				res = minHeap.peek();
			} else if (minHeap.size() < maxHeap.size()) {
				res = maxHeap.peek();
			} else {
				res = (minHeap.peek() + maxHeap.peek()) / 2;
			}
			return handle.transformData(res);
		}

		private static void reBalance(PriorityQueue<Double> first, PriorityQueue<Double> second) {
			if (first.size() - second.size() > 1) {
				second.add(first.poll());
			}
		}

		public void reset() {
			thisData = null;
			minHeap.clear();
			maxHeap.clear();
		}

		@Override
		public boolean equals(Object o) {
			if (! (o instanceof MedianData)) {
				return false;
			}
			int thisSize = minHeap.size() + maxHeap.size();
			if (((MedianData) o).minHeap.size() + ((MedianData) o).maxHeap.size() != thisSize) {
				return false;
			}
			while (thisSize != 0) {
				double thisMedian = getMedian().doubleValue();
				if (thisMedian != ((MedianData) o).getMedian().doubleValue()) {
					return false;
				}
				removeData(thisData);
				((MedianData) o).removeData(thisData);
				--thisSize;
			}
			return true;
		}
	}
}
