package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class RecommUtils {

	/**
	 * Priority queue used to track top k objects who have largest scores.
	 */
	public static class RecommPriorityQueue {
		private final int k;
		private final PriorityQueue <Tuple2 <Object, Double>> priorQueue;
		private Tuple2 <Object, Double> peekOfQueue;

		public RecommPriorityQueue(int k) {
			this.k = k;
			this.priorQueue = new PriorityQueue <>(new Comparator <Tuple2 <Object, Double>>() {
				@Override
				public int compare(Tuple2 <Object, Double> o1, Tuple2 <Object, Double> o2) {
					return Double.compare(o1.f1, o2.f1);
				}
			});
		}

		public void addOrReplace(Object objectId, double score) {
			if (priorQueue.size() < k) {
				priorQueue.add(Tuple2.of(objectId, score));
			} else {
				if (peekOfQueue == null) {
					peekOfQueue = priorQueue.peek();
				}
				boolean replace = score > peekOfQueue.f1;
				if (replace) {
					Tuple2 <Object, Double> head = priorQueue.poll();
					head.f0 = objectId;
					head.f1 = score;
					priorQueue.add(head);
					peekOfQueue = priorQueue.peek();
				}
			}
		}

		/**
		 * Get ordered list of objects. They are ordered by scores in DESCENDING order.
		 */
		public Tuple2 <List <Object>, List <Double>> getOrderedObjects() {
			List <Object> items = new ArrayList <>(priorQueue.size());
			List <Double> scores = new ArrayList <>(priorQueue.size());
			while (priorQueue.size() > 0) {
				Tuple2 <Object, Double> target = priorQueue.poll();
				items.add(target.f0);
				scores.add(target.f1);
			}
			Collections.reverse(items);
			Collections.reverse(scores);
			return Tuple2.of(items, scores);
		}

		public List <Row> getOrderedRows() {
			List <Row> rows = new ArrayList <>(priorQueue.size());
			while (priorQueue.size() > 0) {
				Tuple2 <Object, Double> target = priorQueue.poll();
				rows.add(Row.of(target.f0, target.f1));
			}
			Collections.reverse(rows);
			return rows;
		}
	}
}
