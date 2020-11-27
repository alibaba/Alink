package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TextRank implements Serializable {
	private static final long serialVersionUID = 2333939392784379736L;

	/**
	 * Apply graph-based ranking algorithm.
	 */
	public static Row[] getKeyWords(Row row, double dampingFactor, int windowSize, int maxIteration, double epsilon) {
		List <String> id2Word = new ArrayList <>();
		// Create the undirected graph.
		HashMap <Integer, Double>[] sets = getWordId(row, id2Word, windowSize);
		int len = id2Word.size();
		for (int i = 0; i < len; i++) {
			for (Map.Entry <Integer, Double> entry : sets[i].entrySet()) {
				entry.setValue(entry.getValue() / sets[entry.getKey()].size());
			}
		}

		double n1 = (1.0 - dampingFactor) / len;
		double[] pre = new double[len];
		double[] nxt = new double[len];
		Arrays.fill(pre, 1.0 / len);
		// Iterate the graph-based ranking algorithm until convergence.
		for (int i = 0; i < maxIteration; i++) {
			double max = -1.0;
			for (int j = 0; j < len; j++) {
				double tmp = 0.0;
				HashMap <Integer, Double> set = sets[j];
				for (Map.Entry <Integer, Double> entry : set.entrySet()) {
					tmp += entry.getValue() * pre[entry.getKey()];
				}
				nxt[j] = n1 + dampingFactor * tmp;
				max = Math.max(max, Math.abs(nxt[j] - pre[j]));
			}
			double[] tmp = pre;
			pre = nxt;
			nxt = tmp;
			if (max < epsilon) {
				break;
			}
		}

		Row[] out = new Row[len];

		for (int i = 0; i < len; i++) {
			out[i] = new Row(3);
			out[i].setField(0, row.getField(0));
			out[i].setField(1, id2Word.get(i));
			out[i].setField(2, pre[i]);
		}

		return out;
	}

	/**
	 * Create the map between word and ID, and record the undirected graph.
	 *
	 * @param row:     article
	 * @param id2Word: words list with ID as index.
	 * @return two-dimension matrix, which records the co-occur relationship within a window between words.
	 */
	static HashMap <Integer, Double>[] getWordId(Row row, List <String> id2Word, int windowSize) {
		Map <String, Integer> word2Id = new HashMap <>();
		String str = row.getField(1).toString().replaceAll("[\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×]", "");
		List <String> words = Arrays.asList(str.trim().split("\\s+"));
		Integer ct = 0;
		for (String word : words) {
			if (!word2Id.containsKey(word)) {
				word2Id.put(word, ct++);
				id2Word.add(word);
			}
		}
		int len = id2Word.size();
		HashMap <Integer, Double>[] sets = new HashMap[len];
		for (int i = 0; i < sets.length; i++) {
			sets[i] = new HashMap <>();
		}
		for (int i = 0; i < words.size(); i++) {
			int end = i + windowSize;
			end = end < words.size() ? end : words.size();
			for (int j = i + 1; j < end; j++) {
				if (!words.get(i).equals(words.get(j))) {
					sets[word2Id.get(words.get(i))].put(word2Id.get(words.get(j)), 1.0);
					sets[word2Id.get(words.get(j))].put(word2Id.get(words.get(i)), 1.0);
				}
			}
		}
		return sets;
	}
}
