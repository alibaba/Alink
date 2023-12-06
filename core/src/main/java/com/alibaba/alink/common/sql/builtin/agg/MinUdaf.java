package com.alibaba.alink.common.sql.builtin.agg;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;

import java.util.Comparator;
import java.util.PriorityQueue;

public class MinUdaf extends BaseUdaf <Object, MinUdaf.MinMaxData> {

	boolean excludeLast = false;

	public MinUdaf() {}

	public MinUdaf(boolean excludeLast) {
		this.excludeLast = excludeLast;
	}

	@Override
	public void accumulate(MinMaxData minMaxData, Object... values) {
		Object value = values[0];
		if (value == null) {
			return;
		}
		minMaxData.addData(value);
	}

	@Override
	public void retract(MinMaxData minMaxData, Object... values) {
		Object value = values[0];
		if (value == null) {
			return;
		}
		minMaxData.retract(value);
	}

	@Override
	public void resetAccumulator(MinMaxData minMaxData) {
		minMaxData.reset();
	}

	@Override
	public void merge(MinMaxData minMaxData, Iterable <MinMaxData> it) {
		for (MinMaxData data : it) {
			minMaxData.merge(data);
		}
	}

	@Override
	public Object getValue(MinMaxData accumulator) {
		return accumulator.heap.peek();
	}

	@Override
	public MinMaxData createAccumulator() {
		return new MinMaxData(true, excludeLast);
	}

	public static class MinMaxData {
		private PriorityQueue <Object> heap;
		private boolean excludeLast = false;
		private Object thisData = null;

		MinMaxData(boolean isMin) {
			if (isMin) {
				heap = new PriorityQueue <>(new Comparator <Object>() {
					@Override
					public int compare(Object i1, Object i2) {
						if (i2 == null) {
							return -1;
						}
						if(i1 == null && i2 != null) {
							return 1;
						}
						if (((Number) i2).doubleValue() < ((Number) i1).doubleValue()) {
							return 1;
						}
						return -1;
					}
				});
			} else {
				heap = new PriorityQueue <>(new Comparator <Object>() {
					@Override
					public int compare(Object i1, Object i2) {
						if (i1 == null && i2 == null) {
                            return -1;
						}
						if(i1 == null && i2 != null) {
							return -1;
						}
						if(i1 != null && i2 == null) {
							return -1;
						}
						if (((Number) i2).doubleValue() > ((Number) i1).doubleValue()) {
							return 1;
						}
						return -1;
					}
				});
			}
		}

		public MinMaxData(boolean isMin, boolean excludeLast) {
			this(isMin);
			this.excludeLast = excludeLast;
		}

		public void addData(Object data) {
			if (excludeLast) {
				if (thisData != null) {
					heap.add(thisData);
				}
				thisData = data;
			} else {
				heap.add(data);
			}
		}

		public void retract(Object data) {
			if (!heap.contains(data)) {
				if (excludeLast && thisData == null) {
					throw new AkIllegalDataException("No data to retract.");
				}
				thisData = null;
				return;
			}
			heap.remove(data);
		}

		public void reset() {
			thisData = null;
			heap.clear();
		}

		public void merge(MinMaxData data) {
			thisData = data.thisData;
			for (Object o : data.heap) {
				heap.add(o);
			}
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof MinMaxData)) {
				return false;
			}
			if (excludeLast != ((MinMaxData) o).excludeLast) {
				return false;
			}
			if (excludeLast && thisData != ((MinMaxData) o).thisData) {
				return false;
			}
			while (!heap.isEmpty() && !((MinMaxData) o).heap.isEmpty()) {
				if (!heap.poll().equals(((MinMaxData) o).heap.poll())) {
					return false;
				}
			}
			return heap.isEmpty() && ((MinMaxData) o).heap.isEmpty();
		}

	}
}
