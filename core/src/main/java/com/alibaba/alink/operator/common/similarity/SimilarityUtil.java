package com.alibaba.alink.operator.common.similarity;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;

import java.util.Arrays;
import java.util.PriorityQueue;

public class SimilarityUtil {
	public static final int K = 2;
	public static final double LAMBDA = 0.5;
	public static final int TOP_N = 10;
	public static final long MAX_MEMORY = 134217728;

	public static <T> Tuple2 <Double, T> updateQueue(PriorityQueue <Tuple2 <Double, T>> map, int topN,
													 Tuple2 <Double, T> newValue, Tuple2 <Double, T> head) {
		if (null == newValue) {
			return head;
		}
		if (map.size() < topN) {
			map.add(Tuple2.of(newValue.f0, newValue.f1));
			head = map.peek();
		} else {
			if (map.comparator().compare(head, newValue) < 0) {
				Tuple2 <Double, T> peek = map.poll();
				peek.f0 = newValue.f0;
				peek.f1 = newValue.f1;
				map.add(peek);
				head = map.peek();
			}
		}
		return head;
	}

	/**
	 * Support String[] input and String input.
	 * String: Split the string by character.
	 * String[]: return itself.
	 */
	public static String[] splitStringToWords(String s) {
		String[] res = new String[s.length()];
		for (int i = 0; i < s.length(); i++) {
			res[i] = s.substring(i, i + 1);
		}
		return res;
	}

	public static <T> int[] toInteger(T str) {
		int[] res = null;
		if (str instanceof String) {
			String s = (String) str;
			res = new int[s.length()];
			for (int i = 0; i < s.length(); i++) {
				res[i] = s.substring(i, i + 1).hashCode();
			}
		} else if (str instanceof String[]) {
			String[] s = (String[]) str;
			res = new int[s.length];
			for (int i = 0; i < s.length; i++) {
				res[i] = s[i].hashCode();
			}
		} else {
			throw new AkUnclassifiedErrorException("Only support String[] and String.");
		}
		Arrays.sort(res);
		return res;
	}
}
