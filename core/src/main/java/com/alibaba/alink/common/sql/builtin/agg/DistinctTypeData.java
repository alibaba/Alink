package com.alibaba.alink.common.sql.builtin.agg;

import java.util.HashMap;
import java.util.Map.Entry;

public class DistinctTypeData {

	static class DistinctData {
		HashMap <Object, Integer> mapData;
		boolean firstData = true;
		Object thisData = null;
		boolean excludeLast = false;

		DistinctData() {
			mapData = new HashMap <>();
		}

		DistinctData(boolean excludeLast) {
			this();
			this.excludeLast = excludeLast;
		}

		void addData(Object data) {
			if (firstData) {
				if (!excludeLast) {
					addData(data, 1);
				}
				thisData = data;
				firstData = false;
			} else {
				if (excludeLast) {
					addData(thisData, 1);
				} else {
					addData(data, 1);
				}
				thisData = data;
			}
		}

		void addData(Object data, int num) {
			addLocalData(data, num);
		}

		private void addLocalData(Object data, int num) {
			if (mapData.containsKey(data)) {
				mapData.put(data, mapData.get(data)+num);
			} else {
				mapData.put(data, num);
			}
		}

		void retractData(Object data) {
			if (!mapData.containsKey(data) || mapData.get(data) == 0) {
				if (excludeLast) {
					firstData = true;
				}
			} else {
				mapData.put(data, mapData.get(data) - 1);
			}
		}

		void merge(DistinctData modeData) {
			for (Entry <Object, Integer> entry : modeData.mapData.entrySet()) {
				this.addData(entry.getKey(), entry.getValue());
			}
		}

		void reset() {
			this.mapData.clear();
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof DistinctData)) {
				return false;
			}
			if (excludeLast != ((DistinctData) o).excludeLast) {
				return false;
			}
			if (excludeLast && (thisData != ((DistinctData) o).thisData)) {
				return false;
			}
			for (Entry<Object, Integer> entry : ((DistinctData) o).mapData.entrySet()) {
				if (mapData.containsKey(entry.getKey())) {
					if (!mapData.get(entry.getKey()).equals(entry.getValue())) {
						return false;
					}
				} else if (entry.getValue() != 0) {
					return false;
				}
			}
			return true;
		}
	}

	public static class ModeData extends DistinctData {

		ModeData() {
			super();
		}

		public ModeData(boolean dropLast) {
			super(dropLast);
		}

		Object getValue() {
			Object mode = null;
			int num = 0;
			if (mapData.size() == 0) {
				return null;
			}

			for (Entry <Object, Integer> entry : mapData.entrySet()) {
				int thisNum = entry.getValue();

				if (thisNum > num) {
					mode = entry.getKey();
					num = thisNum;
				}
			}
			return mode;
		}
	}

	public static class IsExistData extends DistinctData {

		IsExistData() {
			super();
		}

		void addData(Object data) {
			super.addData(data);
		}

		boolean getValue() {
			int num = mapData.get(thisData);
			return num > 1;
		}
	}

	public static class FreqData extends DistinctData {

		FreqData(boolean excludeLast) {
			super(excludeLast);
		}

		FreqData() {
			super();
		}

		void addData(Object data) {
			super.addData(data);
		}

		long getValue() {
			return mapData.getOrDefault(thisData, 0);
		}
	}
}
