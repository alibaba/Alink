package com.alibaba.alink.operator.local.utils;

import java.util.LinkedList;
import java.util.PriorityQueue;

public class TopK {

	public interface IndexComparator {
		int compare(int indexA, int indexB);
	}

	public static void sort(int[] indices, int l, int r, IndexComparator comparator) {
		if (l >= r) {
			return;
		}

		LinkedList <int[]> stack = new LinkedList <>();
		stack.push(new int[] {l, r});

		while (!stack.isEmpty()) {
			int[] b = stack.pop();
			int l1 = b[0];
			int r1 = b[1] - 1;

			int p = indices[r1];
			int i = l1 - 1;

			for (int j = l1; j < r1; ++j) {
				if (comparator.compare(indices[j], p) < 0) {
					i = i + 1;
					swap(indices, i, j);
				}
			}

			i = i + 1;
			swap(indices, i, r1);

			if (b[0] < i) {
				stack.push(new int[] {b[0], i});
			}

			if (i + 1 < b[1]) {
				stack.push(new int[] {i + 1, b[1]});
			}
		}
	}

	public static void sortDoubleDesc(int[] indices, double[] data, int l, int r) {
		if (l >= r) {
			return;
		}

		LinkedList <int[]> stack = new LinkedList <>();
		stack.push(new int[] {l, r});

		while (!stack.isEmpty()) {
			int[] b = stack.pop();
			int l1 = b[0];
			int r1 = b[1] - 1;

			int p = indices[r1];
			int i = l1 - 1;

			for (int j = l1; j < r1; ++j) {
				if (data[indices[j]] > data[p]) {
					i = i + 1;
					swap(indices, i, j);
				}
			}

			i = i + 1;
			swap(indices, i, r1);

			if (b[0] < i) {
				stack.push(new int[] {b[0], i});
			}

			if (i + 1 < b[1]) {
				stack.push(new int[] {i + 1, b[1]});
			}
		}
	}

	public static void sortDoubleAsc(int[] indices, double[] data, int l, int r) {
		if (l >= r) {
			return;
		}

		LinkedList <int[]> stack = new LinkedList <>();
		stack.push(new int[] {l, r});

		while (!stack.isEmpty()) {
			int[] b = stack.pop();
			int l1 = b[0];
			int r1 = b[1] - 1;

			int p = indices[r1];
			int i = l1 - 1;

			for (int j = l1; j < r1; ++j) {
				if (data[indices[j]] < data[p]) {
					i = i + 1;
					swap(indices, i, j);
				}
			}

			i = i + 1;
			swap(indices, i, r1);

			if (b[0] < i) {
				stack.push(new int[] {b[0], i});
			}

			if (i + 1 < b[1]) {
				stack.push(new int[] {i + 1, b[1]});
			}
		}
	}

	public static void partialTopK(int[] indices, int k, int l, int r, IndexComparator comparator) {
		int l1 = l;
		int r1 = r - 1;

		int k1 = k;
		int k2 = r - l;

		if (l1 >= r1 || k1 >= k2) {
			sort(indices, l, Math.min(l + k, r), comparator);
			return;
		}

		while (k1 != k2) {
			int p = indices[r1];
			int i = l1 - 1;

			for (int j = l1; j < r1; ++j) {
				if (comparator.compare(indices[j], p) < 0) {
					i = i + 1;
					swap(indices, i, j);
				}
			}

			i = i + 1;

			swap(indices, i, r1);

			k2 = i - l1 + 1;

			if (k1 < k2) {
				r1 = i - 1;
			} else if (k1 > k2) {
				l1 = i + 1;
				k1 = k1 - k2;
			}
		}

		sort(indices, l, Math.min(l + k, r), comparator);
	}

	public static void buildHeap(int[] indices, int l, int r, IndexComparator comparator) {
		int heapSize = r - l;
		for (int i = heapSize / 2 - 1; i >= l; i--) {
			buildHeap(indices, l, r, i, comparator);
		}
	}

	public static void buildHeap(int[] indices, int l, int r, int p, IndexComparator comparator) {
		int heapSize = r - l;

		while (p < heapSize) {
			int left = 2 * p + 1;
			int right = 2 * p + 2;
			int maxP = p;

			if (left < heapSize && comparator.compare(indices[left + l], indices[p + l]) > 0) {
				maxP = left;
			}

			if (right < heapSize && comparator.compare(indices[right + l], indices[maxP + l]) > 0) {
				maxP = right;
			}

			if (p != maxP) {
				swap(indices, p + l, maxP + l);
				p = maxP;
			} else {
				break;
			}
		}
	}

	public static void heapTopK(int[] indices, int k, int l, int r, IndexComparator comparator) {
		if (r - l <= k) {
			sort(indices, l, r, comparator);
			return;
		}

		int localR = Math.min(l + k, r);

		buildHeap(indices, l, localR, comparator);

		for (int i = l + localR; i < r; i++) {
			if (comparator.compare(indices[i], indices[l]) < 0) {
				swap(indices, l, i);
				buildHeap(indices, l, localR, 0, comparator);
			}
		}

		sort(indices, l, localR, comparator);
	}

	public static void buildMaxHeap(int[] indices, double[] data, int l, int r) {
		int heapSize = r - l;
		for (int i = heapSize / 2 - 1; i >= l; i--) {
			buildMaxHeap(indices, data, l, r, i);
		}
	}

	public static void buildMaxHeap(int[] indices, double[] data, int l, int r, int p) {
		int heapSize = r - l;

		while (p < heapSize) {
			int left = 2 * p + 1;
			int right = 2 * p + 2;
			int maxP = p;

			if (left < heapSize && data[indices[left + l]] > data[indices[p + l]]) {
				maxP = left;
			}

			if (right < heapSize && data[indices[right + l]] > data[indices[maxP + l]]) {
				maxP = right;
			}

			if (p != maxP) {
				swap(indices, p + l, maxP + l);
				p = maxP;
			} else {
				break;
			}
		}
	}

	public static void heapMinTopK(int[] indices, double[] data, int k, int l, int r) {
		if (r - l <= k) {
			sortDoubleAsc(indices, data, l, r);
			return;
		}

		int localR = Math.min(l + k, r);

		buildMaxHeap(indices, data, l, localR);

		for (int i = l + localR; i < r; i++) {
			if (data[indices[i]] < data[indices[l]]) {
				swap(indices, l, i);
				buildMaxHeap(indices, data, l, localR, 0);
			}
		}

		sortDoubleAsc(indices, data, l, localR);
	}

	public static void buildMinHeap(int[] indices, double[] data, int l, int r) {
		int heapSize = r - l;
		for (int i = heapSize / 2 - 1; i >= l; i--) {
			buildMaxHeap(indices, data, l, r, i);
		}
	}

	public static void buildMinHeap(int[] indices, double[] data, int l, int r, int p) {
		int heapSize = r - l;

		while (p < heapSize) {
			int left = 2 * p + 1;
			int right = 2 * p + 2;
			int maxP = p;

			if (left < heapSize && data[indices[left + l]] < data[indices[p + l]]) {
				maxP = left;
			}

			if (right < heapSize && data[indices[right + l]] < data[indices[maxP + l]]) {
				maxP = right;
			}

			if (p != maxP) {
				swap(indices, p + l, maxP + l);
				p = maxP;
			} else {
				break;
			}
		}
	}

	public static void heapMaxTopK(int[] indices, double[] data, int k, int l, int r) {
		if (r - l <= k) {
			sortDoubleDesc(indices, data, l, r);
			return;
		}

		int localR = Math.min(l + k, r);

		buildMinHeap(indices, data, l, localR);

		for (int i = l + localR; i < r; i++) {
			if (data[indices[i]] > data[indices[l]]) {
				swap(indices, l, i);
				buildMinHeap(indices, data, l, localR, 0);
			}
		}
		sortDoubleDesc(indices, data, l, localR);
	}

	private static void swap(int[] indices, int i, int j) {
		int tmp = indices[i];
		indices[i] = indices[j];
		indices[j] = tmp;
	}

	public static void priorityQueueTopK(int[] indices, int k, int l, int r, IndexComparator comparator) {
		PriorityQueue <Integer> priorityQueue = new PriorityQueue <>(comparator::compare);

		for (int i = 0, i1 = l; i < k && i1 < r; ++i, ++i1) {
			priorityQueue.add(indices[i1]);
		}

		for (int i1 = l + k; i1 < r; ++i1) {
			Integer head = priorityQueue.peek();

			if (comparator.compare(head, indices[i1]) < 0) {
				priorityQueue.poll();
				priorityQueue.add(indices[i1]);
			}
		}

		for (int i = 0, i1 = l; i < k && i1 < r; ++i, ++i1) {
			indices[i1] = priorityQueue.poll();
		}

		sort(indices, l, Math.min(l + k, r),
			(indexA, indexB) -> Integer.compare(0, comparator.compare(indexA, indexB)));
	}
}
