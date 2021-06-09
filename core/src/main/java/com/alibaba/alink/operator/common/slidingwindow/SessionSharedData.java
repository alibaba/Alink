package com.alibaba.alink.operator.common.slidingwindow;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SessionSharedData {

    /**
     * Session id.
     */
    private static int sessionId = 0;
    public synchronized static int getNewSessionId() {
        return sessionId++;
    }
    public static int getSessionId() {return sessionId;}

    private static ReadWriteLock rwlock = new ReentrantReadWriteLock();


    public static HashMap<Tuple2<String, Integer>, Object> handleWithoutTaskId = new HashMap<>();
    public static void put(String objName, int session, Object data) {
        rwlock.writeLock().lock();
        try {
            handleWithoutTaskId.put(Tuple2.of(objName, session), data);
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    public static Object get(String objName, int session) {
        rwlock.readLock().lock();
        try {
            return handleWithoutTaskId.getOrDefault(Tuple2.of(objName, session), null);
        } finally {
            rwlock.readLock().unlock();
        }
    }


    public static HashMap<Tuple3 <String, Integer, Integer>, Object> handleWithTaskId = new HashMap<>();

    public static void put(String objName, int session, int taskId, Object data) {
        rwlock.writeLock().lock();
        try {
            handleWithTaskId.put(Tuple3.of(objName, session, taskId), data);
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    public static Object get(String objName, int session, int taskId) {
        rwlock.readLock().lock();
        try {
            return handleWithTaskId.getOrDefault(Tuple3.of(objName, session, taskId), null);
        } finally {
            rwlock.readLock().unlock();
        }
    }
}
