package com.alibaba.alink.common.comqueue;

import org.apache.flink.api.java.tuple.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A 'state' is an object in the heap memory of task manager process,
 * shared across all tasks (threads) in the task manager.
 * <p>
 * Note that the 'state' is shared by all tasks on the same task manager,
 * users should guarantee that no two tasks modify a 'state' at the same time.
 * <p>
 * A 'state' is identified by 'handle' and 'taskId'.
 */
public class IterTaskObjKeeper implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(IterTaskObjKeeper.class);

	private static Map <Tuple2 <Long, Integer>, Object> states;

	/**
	 * A 'handle' is a unique identifier of a state.
	 */
	private static long handle = 0L;

	private static ReadWriteLock rwlock = new ReentrantReadWriteLock();

	static {
		states = new HashMap <>();
	}

	/**
	 * @note Should get a new handle on the client side and pass it to transformers.
	 */
	synchronized public static long getNewHandle() {
		return handle++;
	}

	public static void put(long handle, int taskId, Object state) {
		rwlock.writeLock().lock();
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("taskId: {}, Put handle: {}", taskId, handle);
			}
			states.put(Tuple2.of(handle, taskId), state);
			if (LOG.isDebugEnabled()) {
				LOG.debug("taskId: {}, Put handle succeeded: {}", taskId, handle);
			}
		} finally {
			rwlock.writeLock().unlock();
		}
	}

	public static <T> T get(long handle, int taskId) {
		rwlock.readLock().lock();
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("taskId: {}, Get handle: {}", taskId, handle);
			}
			if (states.containsKey(Tuple2.of(handle, taskId))) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("taskId: {}, Get handle succeeded: {}", taskId, handle);
				}
				return (T) states.get(Tuple2.of(handle, taskId));
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("taskId: {}, Get handle failed: {}", taskId, handle);
				}
				return null;
			}
		} finally {
			rwlock.readLock().unlock();
		}
	}

	public static <T> T remove(long handle, int taskId) {
		rwlock.writeLock().lock();
		try {
			return (T) states.remove(Tuple2.of(handle, taskId));
		} finally {
			rwlock.writeLock().unlock();
		}
	}

	public static void clear(long handle) {
		rwlock.writeLock().lock();
		try {
			Iterator <Map.Entry <Tuple2 <Long, Integer>, Object>> iter = states.entrySet().iterator();

			while (iter.hasNext()) {
				Map.Entry <Tuple2 <Long, Integer>, Object> next = iter.next();
				if (next.getKey().f0 == handle) {
					iter.remove();
					if (LOG.isDebugEnabled()) {
						LOG.debug("State of handle: {} && taskId: {} has been removed",
							next.getKey().f0, next.getKey().f1);
					}
				}
			}
		} finally {
			rwlock.writeLock().unlock();
		}
	}

//	public static <T> DataSet <T> clear(DataSet <T> model, final long handle) {
//		return clear(model, new long[] {handle});
//	}
//
//	public static <T> DataSet <T> clear(DataSet <T> model, final long handle[]) {
//		return model.mapPartition(new RichMapPartitionFunction <T, T>() {
//			@Override
//			public void mapPartition(Iterable <T> values, Collector <T> out) throws Exception {
//				for (T val : values) {
//					out.collect(val);
//				}
//
//				int taskId = getRuntimeContext().getIndexOfThisSubtask();
//				for (int i = 0; i < handle.length; ++i) {
//					if (LOG.isDebugEnabled()) {
//						LOG.debug("taskId: {}, start to clear handle: {}", taskId, handle[i]);
//					}
//					clear(handle[i]);
//					if (LOG.isDebugEnabled()) {
//						LOG.debug("taskId: {}, end to clear handle: {}", taskId, handle[i]);
//					}
//				}
//			}
//		}).returns(model.getType());
//	}

	public static boolean contains(long randId, int taskId) {
		rwlock.readLock().lock();
		try {
			return states.containsKey(Tuple2.of(randId, taskId));
		} finally {
			rwlock.readLock().unlock();
		}
	}
}
