package com.alibaba.alink.common.comqueue;

import org.apache.flink.api.common.functions.IterationRuntimeContext;

/**
 * Context used in BaseComQueue to access basic runtime information and shared objects.
 */
public class ComContext {
	private final int taskId;
	private final int numTask;
	private final int stepNo;
	private final int sessionId;

	public ComContext(int sessionId, IterationRuntimeContext runtimeContext) {
		this.sessionId = sessionId;
		this.numTask = runtimeContext.getNumberOfParallelSubtasks();
		this.taskId = runtimeContext.getIndexOfThisSubtask();
		this.stepNo = runtimeContext.getSuperstepNumber();
	}

	/**
	 * Get task Id, the same as {@link IterationRuntimeContext#getIndexOfThisSubtask()}.
	 * @return task id
	 */
	public int getTaskId() {
		return taskId;
	}

	/**
	 * Get task number, the same as {@link IterationRuntimeContext#getNumberOfParallelSubtasks()}.
	 *
	 * @return task number.
	 */
	public int getNumTask() {
		return numTask;
	}

	/**
	 * Get current iteration step number, the same as {@link IterationRuntimeContext#getSuperstepNumber()}.
	 * @return iteration step number.
	 */
	public int getStepNo() {
		return stepNo;
	}

	/**
	 * Put an object into shared objects for access of other QueueItem of the same taskId.
	 *
	 * @param objName object name
	 * @param obj     object itself.
	 */
	public void putObj(String objName, Object obj) {
		SessionSharedObjs.put(objName, sessionId, taskId, obj);
	}

	/**
	 * Get obj from shared objects.
	 *
	 * @param objName name of the object.
	 * @param <T>     type of the object.
	 * @return the object.
	 */
	public <T> T getObj(String objName) {
		return SessionSharedObjs.get(objName, sessionId, taskId);
	}

	/**
	 * Remove object from shared objects.
	 *
	 * @param objName name of the object.
	 * @param <T>     type of the object.
	 * @return the object.
	 */
	public <T> T removeObj(String objName) {
		return SessionSharedObjs.remove(objName, sessionId, taskId);
	}

	/**
	 * Check if there's is an object in shared objects with given name.
	 *
	 * @param objName name of the object.
	 * @return true if there is.
	 */
	public boolean containsObj(String objName) {
		return SessionSharedObjs.contains(objName, sessionId, taskId);
	}
}
