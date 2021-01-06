package com.alibaba.alink.common.io.catalog.odps;

import org.apache.flink.util.Preconditions;

public class ElementSplitUtil {
	public static ElementSplitUtil.ElementSegment[][] doSplit(long[] elementCounts, int numSplits) {
		Preconditions.checkArgument(elementCounts != null && allPositive(elementCounts),
			"element counts is null or contain illegal element! ");
		Preconditions.checkArgument(numSplits > 0, "num splits is less than 1! ");
		int numElements = elementCounts.length;
		if (numElements == 0) {
			return new ElementSplitUtil.ElementSegment[numSplits][0];
		} else {
			long totalCount = 0L;
			long[][] elementBoundaries = new long[numElements][2];

			for (int idx = 0; idx < numElements; ++idx) {
				long[] elementBoundary = new long[] {totalCount, 0L};
				totalCount += elementCounts[idx];
				elementBoundary[1] = totalCount - 1L;
				elementBoundaries[idx] = elementBoundary;
			}

			long averageSplitCount = totalCount / (long) numSplits;
			ElementSplitUtil.ElementSegment[][] splitResult = new ElementSplitUtil.ElementSegment[numSplits][];

			for (int splitIdx = 0; splitIdx < numSplits; ++splitIdx) {
				long splitStartAbsolutePos = (long) splitIdx * averageSplitCount;
				long splitCount = splitIdx < numSplits - 1 ? averageSplitCount
					: averageSplitCount + totalCount % (long) numSplits;
				long splitEndAbsolutePos = splitStartAbsolutePos + splitCount - 1L;
				int startElementId = floorElementLocation(elementBoundaries, splitStartAbsolutePos);
				int endElementId = floorElementLocation(elementBoundaries, splitEndAbsolutePos);
				int elementNums = endElementId + 1 - startElementId;
				ElementSplitUtil.ElementSegment[] elementsInOneSplit =
					new ElementSplitUtil.ElementSegment[elementNums];

				for (int partId = startElementId; partId <= endElementId; ++partId) {
					long startAbsolutePosition = Math.max(splitStartAbsolutePos, elementBoundaries[partId][0]);
					long endAbsolutePosition = Math.min(splitEndAbsolutePos, elementBoundaries[partId][1]);
					long startRelativePosition = startAbsolutePosition - elementBoundaries[partId][0];
					long partCount = endAbsolutePosition + 1L - startAbsolutePosition;
					elementsInOneSplit[partId - startElementId] = new ElementSplitUtil.ElementSegment(partId,
						startRelativePosition, partCount);
				}

				splitResult[splitIdx] = elementsInOneSplit;
			}

			return splitResult;
		}
	}

	private static boolean allPositive(long[] elementCounts) {
		for (long count : elementCounts) {
			if (count < 0L) {
				return false;
			}
		}

		return true;
	}

	private static int floorElementLocation(long[][] elements, long toSearch) {
		int len = elements.length;
		int left = 0;
		int right = len - 1;

		while (left <= right) {
			int mid = left + right >>> 1;
			if (elements[mid][0] < toSearch) {
				++left;
			} else {
				if (elements[mid][0] == toSearch) {
					return mid;
				}

				--right;
			}
		}

		return right;
	}

	private ElementSplitUtil() {
	}

	public static class ElementSegment {
		private final int elementId;
		private final long start;
		private final long count;

		public ElementSegment(int elementId, long start, long count) {
			this.elementId = elementId;
			this.start = start;
			this.count = count;
		}

		public int getElementId() {
			return this.elementId;
		}

		public long getStart() {
			return this.start;
		}

		public long getCount() {
			return this.count;
		}

		public boolean equals(Object o) {
			if (this == o) {
				return true;
			} else if (o != null && this.getClass() == o.getClass()) {
				ElementSplitUtil.ElementSegment that = (ElementSplitUtil.ElementSegment) o;
				if (this.elementId != that.elementId) {
					return false;
				} else if (this.start != that.start) {
					return false;
				} else {
					return this.count == that.count;
				}
			} else {
				return false;
			}
		}

		public int hashCode() {
			int result = this.elementId;
			result = 31 * result + (int) (this.start ^ this.start >>> 32);
			result = 31 * result + (int) (this.count ^ this.count >>> 32);
			return result;
		}

		public String toString() {
			return "ElementSegment{elementId=" + this.elementId + ", start=" + this.start + ", count=" + this.count
				+ '}';
		}
	}
}
