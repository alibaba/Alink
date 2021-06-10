package com.alibaba.alink.common.sql.builtin.agg;


import com.alibaba.alink.common.sql.builtin.agg.ListAggUdaf.ListAggData;

public class ListAggUdaf extends BaseUdaf<String, ListAggData> {

	private boolean excludeLast = false;

	public ListAggUdaf() {}

	public ListAggUdaf(boolean excludeLast) {
		this.excludeLast = excludeLast;
	}

	@Override
	public String getValue(ListAggData accumulator) {
		if (accumulator.aggData == null) {
			return null;
		}
		return accumulator.aggData.toString();
	}

	@Override
	public ListAggData createAccumulator() {
		return new ListAggData(excludeLast);
	}

	@Override
	public void accumulate(ListAggData acc, Object... valueAndDelimiter) {
		acc.accumulate(valueAndDelimiter);
	}

	@Override
	public void resetAccumulator(ListAggData acc) {
		acc.aggData = null;
	}

	@Override
	public void retract(ListAggData acc, Object... valueAndDelimiter) {
		acc.retract(valueAndDelimiter);
	}

	@Override
	public void merge(ListAggData acc, Iterable <ListAggData> it) {
		acc.merge(it);
	}

	public static class ListAggData {
		StringBuilder aggData = null;
		private String delimiter = ",";
		private boolean meetFirstData = false;
		private boolean excludeLast = false;
		private Object thisData = null;

		public ListAggData() {}

		public ListAggData(boolean excludeLast) {
			this.excludeLast = excludeLast;
		}

		public void accumulate(Object... valueAndDelimiter) {
			Object value = valueAndDelimiter[0];
			if (value == null) {
				return;
			}
			if (excludeLast) {
				if (thisData == null) {
					aggData = null;
				} else {
					if (valueAndDelimiter.length == 2) {
						addData(thisData, valueAndDelimiter[1]);
					} else {
						addData(thisData);
					}
				}
				thisData = value;
			} else {
				addData(valueAndDelimiter);
			}
		}

		private void addData(Object... valueAndDelimiter) {
			Object value = valueAndDelimiter[0];
			if (!meetFirstData) {
				if (valueAndDelimiter.length == 2) {
					delimiter = (String) valueAndDelimiter[1];
				}
				meetFirstData = true;
			}
			if (value != null) {
				if (aggData != null) {
					aggData.append(delimiter).append(value);
				} else {
					aggData = new StringBuilder();
					aggData.append(value);
				}
			}
		}

		public void retract(Object... valueAndDelimiter) {
			Object value = valueAndDelimiter[0];
			if (value == null) {
				return;
			}
			if (aggData == null) {
				if (excludeLast && thisData == null) {
					throw new RuntimeException("No data to retract.");
				}
				thisData = null;
				return;
			}
			if (!meetFirstData) {
				if (valueAndDelimiter.length == 2) {
					delimiter = (String) valueAndDelimiter[1];
				}
				meetFirstData = true;
			}
			int index = aggData.indexOf(delimiter);
			if (index == -1) {
				aggData = null;
			} else {
				aggData.delete(0, index+1);
			}
		}


		public void merge(Iterable <ListAggData> it) {
			boolean addFirstDelimiter = aggData != null;
			boolean firstData = true;
			for (ListAggData listAggData : it) {
				if (listAggData.aggData != null) {
					if (firstData) {
						if (addFirstDelimiter) {
							aggData.append(listAggData.delimiter).append(listAggData.aggData);
						} else {
							aggData = new StringBuilder();
							aggData.append(listAggData.aggData);
						}
						firstData = false;
					} else {
						aggData.append(listAggData.delimiter).append(listAggData.aggData);
					}
				}
			}
		}


		@Override
		public boolean equals(Object o) {
			if (!(o instanceof ListAggData)) {
				return false;
			}
			StringBuilder otherData = ((ListAggData) o).aggData;
			return AggUtil.judgeNull(aggData, otherData, ListAggData::judgeEqual);
		}

		private static boolean judgeEqual(Object a, Object b) {
			return a.toString().equals(b.toString());
		}
	}
}
