package com.alibaba.alink.common.comqueue;

import org.apache.flink.api.java.tuple.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An static class that manage shared objects for {@link BaseComQueue}s.
 */
class SessionSharedObjs implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(SessionSharedObjs.class);

	private static HashMap<Tuple2<String, Integer>, Long> key2Handle = new HashMap<>();

	private static int sessionId = 0;
	private static ReadWriteLock rwlock = new ReentrantReadWriteLock();

	/**
	 * Get a new session id.
	 * All access operation should bind with a session id. This id is usually shared among compute/communicate
	 * function of an {@link IterativeComQueue}.
	 *
	 * @return new session id.
	 */
	synchronized static int getNewSessionId() {
		return sessionId++;
	}

	/**
	 * Put an object into session shared objects.
	 *
	 * @param objName object name
	 * @param session session id
	 * @param taskId  task id in flink.
	 * @param obj     the object to put
	 */
	static void put(String objName, int session, int taskId, Object obj) {
		rwlock.writeLock().lock();
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Put, taskId: {}, objName: {}, session: {}", taskId, objName, session);
			}

			Long handle = key2Handle.get(Tuple2.of(objName, session));

			if (handle == null) {
				handle = IterTaskObjKeeper.getNewHandle();
				key2Handle.put(Tuple2.of(objName, session), handle);
			}

			IterTaskObjKeeper.put(handle, taskId, obj);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Put, taskId: {}, objName: {}, session: {}, handle: {} succeeded", taskId, objName, session,
					handle);
			}
		} finally {
			rwlock.writeLock().unlock();
		}
	}

	/**
	 * Check if an object exists.
	 *
	 * @param objName object name
	 * @param session session id
	 * @param taskId  task id in flink.
	 * @return true if exists.
	 */
	static boolean contains(String objName, int session, int taskId) {
		rwlock.readLock().lock();
		try {
			Long handle = key2Handle.get(Tuple2.of(objName, session));
			if (handle != null) {
				return IterTaskObjKeeper.contains(handle, taskId);
			} else {
				return false;
			}
		} finally {
			rwlock.readLock().unlock();
		}
	}

	/**
	 * Get an object from session shared objects.
	 *
	 * @param objName object name
	 * @param session session id
	 * @param taskId  task id in flink.
	 * @param <T> type of object
	 * @return the object or on absence.
	 */
	static <T> T get(String objName, int session, int taskId) {
		rwlock.readLock().lock();
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Get, taskId: {}, key: {}, session: {}", taskId, objName, session);
			}
			Long handle = key2Handle.get(Tuple2.of(objName, session));

			if (handle == null) {
				return null;
			}

			return IterTaskObjKeeper.get(handle, taskId);
		} finally {
			rwlock.readLock().unlock();
		}
	}

	/**
	 * Remove an object from session shared objects.
	 *
	 * @param objName object name
	 * @param session session id
	 * @param taskId  task id in flink.
	 * @param <T> type of object
	 * @return the object or on absence.
	 */
	static <T> T remove(String objName, int session, int taskId) {
		rwlock.writeLock().lock();
		try {
			Long handle = key2Handle.get(Tuple2.of(objName, session));

			if (handle == null) {
				return null;
			}

			return (T) IterTaskObjKeeper.remove(handle, taskId);
		} finally {
			rwlock.writeLock().unlock();
		}
	}

	private static final Integer CACHED_DATA_OBJ_KEY = -1;

	/**
	 * Put partitioned dataset into shared session objects.
	 *
	 * @param objName object name
	 * @param session session id
	 * @param obj partial dataset in collection
	 */
	static void cachePartitionedData(String objName, int session, List obj) {
		rwlock.writeLock().lock();
		try {
			Long handle = key2Handle.get(Tuple2.of(objName, session));

			if (handle == null) {
				handle = IterTaskObjKeeper.getNewHandle();
				key2Handle.put(Tuple2.of(objName, session), handle);
			}

			Queue<List> cache = IterTaskObjKeeper.get(handle, CACHED_DATA_OBJ_KEY);

			if (cache == null) {
				cache = new LinkedList<>();
				IterTaskObjKeeper.put(handle, CACHED_DATA_OBJ_KEY, cache);
			}

			cache.add(obj);
		} finally {
			rwlock.writeLock().unlock();
		}
	}

	/**
	 * Redistribute cached dataset.
	 *
	 * @param objNames list of all cached dataset.
	 * @param session  session id
	 * @param taskId   task id in flink.
	 */
	static void distributeCachedData(List<String> objNames, int session, int taskId) {
		rwlock.writeLock().lock();
		try {
			for (String objName : objNames) {
				Long handle = key2Handle.get(Tuple2.of(objName, session));

				if (handle == null) {
					handle = IterTaskObjKeeper.getNewHandle();
					key2Handle.put(Tuple2.of(objName, session), handle);

					IterTaskObjKeeper.put(
						handle,
						taskId,
						new ArrayList<>()
					);

					continue;
				}

				Queue<List> cache = IterTaskObjKeeper.get(handle, CACHED_DATA_OBJ_KEY);

				if (cache == null) {
					IterTaskObjKeeper.put(
						handle,
						taskId,
						new ArrayList<>()
					);

					continue;
				}

				Object obj = cache.poll();

				if (obj == null) {
					IterTaskObjKeeper.put(
						handle,
						taskId,
						new ArrayList<>()
					);
				} else {
					IterTaskObjKeeper.put(
						handle,
						taskId,
						obj
					);
				}

				if (cache.isEmpty()) {
					IterTaskObjKeeper.remove(handle, CACHED_DATA_OBJ_KEY);
				} else {
					IterTaskObjKeeper.put(handle, CACHED_DATA_OBJ_KEY, cache);
				}
			}
		} finally {
			rwlock.writeLock().unlock();
		}
	}

	/**
	 * Clear all shared objects of this session.
	 *
	 * @param session the session id
	 */
	static void clear(int session) {
		rwlock.writeLock().lock();
		try {
			Iterator <Map.Entry <Tuple2 <String, Integer>, Long>> iter = key2Handle.entrySet().iterator();

			while (iter.hasNext()) {
				Map.Entry <Tuple2 <String, Integer>, Long> next = iter.next();
				if (next.getKey().f1.equals(session)) {
					IterTaskObjKeeper.clear(next.getValue());
					iter.remove();
					if (LOG.isDebugEnabled()) {
						LOG.debug("State of session: {} has been removed", session);
					}
				}
			}
		} finally {
			rwlock.writeLock().unlock();
		}
	}
}
