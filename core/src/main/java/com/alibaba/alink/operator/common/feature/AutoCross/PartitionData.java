package com.alibaba.alink.operator.common.feature.AutoCross;

import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.common.linalg.Vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PartitionData {
	private static Map <Integer, List <Tuple3 <Double, Double, Vector>>> data;

	static {
		data = new HashMap <>();
	}

	private static ReadWriteLock rwlock = new ReentrantReadWriteLock();

	public static void addData(int taskId, Tuple3 <Double, Double, Vector> s) {
		rwlock.writeLock().lock();
		try {
			data.compute(taskId, (k, v) -> {
				if (v == null) {
					v = new ArrayList <>();
				}
				v.add(s);
				return v;
			});
		} finally {
			rwlock.writeLock().unlock();
		}
	}

	public static void addData(int taskId, List <Tuple3 <Double, Double, Vector>> s) {
		rwlock.writeLock().lock();
		try {
			data.compute(taskId, (k, v) -> {
				if (v == null) {
					v = new ArrayList <>();
				}
				v.addAll(s);
				return v;
			});
		} finally {
			rwlock.writeLock().unlock();
		}
	}

	public static List <Tuple3 <Double, Double, Vector>> getData(int taskId) {
		rwlock.readLock().lock();
		try {
			return data.get(taskId);
		} finally {
			rwlock.readLock().unlock();
		}
	}
}
