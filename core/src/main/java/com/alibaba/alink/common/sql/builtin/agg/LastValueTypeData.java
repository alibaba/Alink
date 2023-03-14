package com.alibaba.alink.common.sql.builtin.agg;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;

public class LastValueTypeData {

	public static class LagData extends LastValueData {

		LagData(boolean considerNull) {
			this.k = -1;
			this.considerNull = considerNull;
		}

		void addLagData(Object data, int k, Object defaultData) {
			if (this.k == -1) {
				this.k = k;
			}
			if (needDataType()) {
				getType(data);
			}
			this.defaultData = defaultData;
			thisValue = new Node(data, -1);
			lastValue.append(thisValue);
			if (considerNull) {
				if (this.k == 0 && lastValue.dataSize > 1 || this.k != 0 && lastValue.dataSize > this.k) {
					lastValue.removeRoot();
				}
			} else {
				if (this.k == 0 && lastValue.noNullNum > 1 || this.k != 0 && lastValue.noNullNum > this.k) {
					lastValue.removeRoot();
				}
			}
		}

		void addLagData(Object data, int k) {
			addLagData(data, k, null);
		}

		Object getLagData() {
			return transformData(lastValue.lastKData(k, defaultData));
		}

		Object getLagDataConsiderNull() {
			return transformData(lastValue.lastKDataConsideringNull(k, defaultData));
		}

		private TypeInformation <?> dataType = null;
		private boolean passTrough = false;

		private boolean needDataType() {
			if (passTrough) {
				return false;
			}
			return dataType == null;
		}

		private void getType(Object data) {
			if (data == null) {
				return;
			}
			if (data instanceof Double) {
				dataType = Types.DOUBLE;
			} else if (data instanceof Long) {
				dataType = Types.LONG;
			} else if (data instanceof Integer) {
				dataType = Types.INT;
			} else if (data instanceof Short) {
				dataType = Types.SHORT;
			} else if (data instanceof Float) {
				dataType = Types.FLOAT;
			} else if (data instanceof Byte) {
				dataType = Types.BYTE;
			}  else if (data instanceof BigDecimal) {
				dataType = Types.BIG_DEC;
			}else {
				passTrough = true;
			}

		}

		Object transformData(Object inputData) {
			if (passTrough || inputData == null || dataType == null) {
				return inputData;
			}

			Double data = ((Number) inputData).doubleValue();
			if (Types.LONG.equals(dataType)) {
				return data.longValue();
			} else if (Types.INT.equals(dataType)) {
				return data.intValue();
			} else if (Types.SHORT.equals(dataType)) {
				return data.shortValue();
			} else if (Types.FLOAT.equals(dataType)) {
				return data.floatValue();
			} else if (Types.BYTE.equals(dataType)) {
				return data.byteValue();
			} else if (Types.BIG_DEC.equals(dataType)) {
				return inputData;
			} else {
				return data;
			}
		}
	}

	public static class LastValueData {
		int k;
		LinkedData lastValue = null;
		Node thisValue = null;
		public boolean hasParsed = false;
		Object defaultData = null;
		boolean considerNull;

		LastValueData(boolean considerNull) {
			this.lastValue = new LinkedData();
			this.considerNull = considerNull;
			this.k = -1;
		}

		public double parseData(Object data, double timeInterval) {
			if (!hasParsed) {
				hasParsed = true;
				return Double.parseDouble(data.toString());
			}
			return timeInterval;
		}

		public LastValueData() {
			this(false);
		}

		void reset() {
			lastValue = new LinkedData();
			thisValue = null;
		}

		void addData(Object data, Object timeStamp, int k, double timeInterval) {
			if (this.k == -1) {
				this.k = k;
			}
			long time;
			if (timeStamp instanceof Timestamp) {
				time = ((Timestamp) timeStamp).getTime();
			} else {
				time = (long) timeStamp;
			}
			thisValue = new Node(data, time);
			addLatestNode(lastValue, thisValue, this.k, time, timeInterval, considerNull);
		}

		void retractData() {
			lastValue.removeRoot();
		}

		Object getData() {
			return lastValue.lastKData(k, null);
		}

		Object getDataConsideringNull() {
			return lastValue.lastKDataConsideringNull(k, null);
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof LastValueData)) {
				return false;
			}
			LinkedData otherLastValue = ((LastValueData) o).lastValue;
			return AggUtil.judgeNull(lastValue, otherLastValue, LastValueTypeData::judgeLinkData);
		}

		static void addLatestNode(LinkedData linkedData, Node node, int k, long time, double timeInterval,
								  boolean considerNull) {
			linkedData.append(node);
			if (timeInterval != -1) {
				if (considerNull) {
					while (k == 0 && linkedData.dataSize > 1 || k != 0 && linkedData.dataSize > k
						|| linkedData.root != null && linkedData.root.time > time + timeInterval) {
						linkedData.removeRoot();
					}
				} else {
					while (k == 0 && linkedData.noNullNum > 1 || k != 0 && linkedData.noNullNum > k
						|| linkedData.root != null && linkedData.root.time > time + timeInterval) {
						linkedData.removeRoot();
					}
				}
			}
		}
	}

	public static boolean judgeLinkData(Object a, Object b) {
		Node aNode = ((LinkedData) a).root;
		Node bNode = ((LinkedData) b).root;
		while (aNode != null || bNode != null) {
			if (!aNode.getData().equals(bNode.getData())) {
				return false;
			}
		}
		if (null == aNode && null == bNode) {
			return true;
		}
		return false;
	}

	public static class LastTimeData extends LastValueData {

		@Override
		Object getData() {
			Long res = (Long) lastValue.lastTime(k);
			if (res == null) {
				return null;
			}
			return new Timestamp(res);
		}
	}

	public static class SumLastData extends LastValueData {
		NumberTypeHandle handle = null;

		@Override
		void addData(Object data, Object timeStamp, int k, double timeInterval) {
			if (handle == null) {
				handle = new NumberTypeHandle(data);
			}
			super.addData(data, timeStamp, k, timeInterval);
		}

		@Override
		Object getData() {
			double res = (double) lastValue.sumLastK(k);
			return handle.transformData(res);
		}
	}

	public static void merge(LastValueData acc, Iterable <LastValueData> it) {
		boolean firstData = acc.lastValue == null;
		for (LastValueData lastValueData : it) {
			if (lastValueData != null) {
				if (firstData) {
					acc = new LastValueData();
					firstData = false;
					acc.lastValue.root = lastValueData.lastValue.root;
					acc.lastValue.lastNode = lastValueData.lastValue.lastNode;
				} else {
					acc.lastValue.lastNode.setNext(lastValueData.lastValue.root);
					acc.lastValue.lastNode = lastValueData.lastValue.lastNode;
				}
				acc.lastValue.dataSize += lastValueData.lastValue.dataSize;
			}
		}
	}

	public static void merge(LagData acc, Iterable <LagData> it) {
		boolean firstData = acc.lastValue == null;
		for (LagData lastValueData : it) {
			if (lastValueData != null) {
				if (firstData) {
					acc = new LagData(lastValueData.considerNull);
					firstData = false;
					acc.lastValue.root = lastValueData.lastValue.root;
					acc.lastValue.lastNode = lastValueData.lastValue.lastNode;
				} else {
					acc.lastValue.lastNode.setNext(lastValueData.lastValue.root);
					acc.lastValue.lastNode = lastValueData.lastValue.lastNode;
				}
				acc.lastValue.dataSize += lastValueData.lastValue.dataSize;
			}
		}
	}

	public static class LinkedData implements Serializable {
		public Node root;
		public Node lastNode;
		public int dataSize;//the number of all the ip
		public int noNullNum = 0;

		public LinkedData() {
		}

		public LinkedData(Node node) {
			addRoot(node);
		}

		void addRoot(Node node) {
			root = node;
			lastNode = node;
			dataSize = 1;
			if (node.data != null) {
				++noNullNum;
			}
		}

		//remove the earliest data.
		void removeRoot() {
			if (root.data != null) {
				--noNullNum;
			}
			if (dataSize > 1) {
				dataSize -= 1;
				root = root.next;
			} else {
				dataSize -= 1;
				root = null;
				lastNode = null;
			}
		}

		void append(Node node) {
			if (root == null) {
				addRoot(node);
			} else {
				if (node.data != null) {
					++noNullNum;
				}
				dataSize += 1;
				lastNode.setNext(node);
				node.setPrevious(lastNode);
				lastNode = node;
			}
		}

		//return the last k data.
		public Object lastKData(int k, Object defaultData) {
			if (noNullNum == 0 || k > dataSize) {
				return defaultData;
			}
			int index = k;
			Node p = lastNode;
			while (index > 0) {
				p = p.getPrevious();
				while (p != null && p.data == null) {
					p = p.getPrevious();
				}
				if (p == null) {
					return defaultData;
				}
				--index;
			}
			return p.data;
		}

		public Object lastKDataConsideringNull(int k, Object defaultData) {
			if (dataSize == 0 || k > dataSize) {
				return defaultData;
			}
			int index = k;
			Node p = lastNode;
			while (index > 0) {
				p = p.getPrevious();
				if (p == null) {
					return defaultData;
				}
				--index;
			}
			return p.data;
		}

		//return the time of last k data.
		public Object lastTime(int k) {
			if (dataSize == 0 || k > dataSize) {
				return null;
			}
			int index = k;
			Node p = lastNode;
			while (index > 0) {
				p = p.getPrevious();
				while (p != null && p.data == null) {
					p = p.getPrevious();
				}
				if (p == null) {
					return null;
				}
				--index;
			}
			return p.time;
		}

		//return the sum of last k data.
		public Object sumLastK(int k) {
			if (dataSize == 0) {
				return 0.0;
			}
			double res = 0;
			int index = k;
			Node p = lastNode;
			while (index > 0) {
				res += ((Number) p.data).doubleValue();
				p = p.getPrevious();
				while (p != null && p.data == null) {
					p = p.getPrevious();
				}
				if (p == null) {
					return res;
				}
				--index;
			}

			return res;
		}
	}

	public static class Node implements Serializable {
		private Object data;
		private long time;
		private Node next = null;
		private Node previous = null;

		public Node(Object data, long time) {
			setData(data, time);
		}

		public void setData(Object data, long time) {
			this.data = data;
			this.time = time;
		}

		public Object getData() {
			return data;
		}

		public double getTime() {
			return time;
		}

		public void setNext(Node next) {
			this.next = next;
		}

		public Node getNext() {
			return next;
		}

		public boolean hasNext() {
			return !(next == null);
		}

		public void setPrevious(Node previous) {
			this.previous = previous;
		}

		public Node getPrevious() {
			return previous;
		}

		public boolean hasPrevious() {
			return !(previous == null);
		}
	}

}
