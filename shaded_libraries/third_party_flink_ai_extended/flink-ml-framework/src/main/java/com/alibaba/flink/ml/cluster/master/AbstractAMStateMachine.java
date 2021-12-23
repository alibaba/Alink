/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.ml.cluster.master;

import com.alibaba.flink.ml.cluster.BaseEventReporter;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.master.meta.AMMeta;
import com.alibaba.flink.ml.cluster.statemachine.event.EventHandler;
import com.alibaba.flink.ml.cluster.statemachine.InvalidStateTransitionException;
import com.alibaba.flink.ml.cluster.statemachine.StateMachine;
import com.alibaba.flink.ml.proto.AMStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * abstract application master state machine.
 * this class object handle AMEvent trigger transition and am status change.
 * subclass should implement buildStateMachine method to create application master state machine.
 */
public abstract class AbstractAMStateMachine implements EventHandler<AMEvent> {
	private static final Logger Logger = LoggerFactory.getLogger(AbstractAMStateMachine.class);

	protected final Lock writeLock;
	protected final Lock readLock;
	protected StateMachine<AMStatus, AMEventType, AMEvent> stateMachine;
	protected final AMService amService;
	protected final BaseEventReporter eventReporter;
	protected final AMMeta amMeta;
	protected final MLContext mlContext;
	protected final BlockingQueue<AMEvent> eventQueue = new ArrayBlockingQueue<>(1000);
	protected final ExecutorService exService;

	/**
	 * process AMEvent and do transition.
	 */
	class EventHandle implements Runnable {
		@Override
		public void run() {
			AMEvent event;
			while (true) {
				try {
					event = eventQueue.take();
					handle(event);
				} catch (InterruptedException e) {
					Logger.info("EventHandle thread interrupted, exiting with {} pending events", eventQueue.size());
					return;
				} catch (Exception e) {
					Logger.error("Failed to handle event", e);
					getAmService().handleStateTransitionError(null, e);
				}
			}
		}
	}

	/**
	 * create an am state machine.
	 * @param amService application master am service.
	 * @param amMeta application master meta information
	 * @param mlContext cluster am runtime context
	 * @param eventReporter report metric handler
	 */
	public AbstractAMStateMachine(AMService amService, AMMeta amMeta,
			MLContext mlContext, BaseEventReporter eventReporter) {
		this.amService = amService;
		this.eventReporter = eventReporter;
		this.amMeta = amMeta;
		this.mlContext = mlContext;
		ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
		this.readLock = readWriteLock.readLock();
		this.writeLock = readWriteLock.writeLock();
		this.stateMachine = buildStateMachine(mlContext, amMeta);
		exService = Executors.newFixedThreadPool(1, r -> {
			Thread eventThread = new Thread(r);
			eventThread.setDaemon(true);
			eventThread.setName("am_event_handler");
			return eventThread;
		});
		exService.submit(new EventHandle());
		Logger.info("start am_event_handler thread!");
	}

	/**
	 * subclass should implement this method to instance state machine object.
	 * @param mlContext machine learning cluster node runtime context.
	 * @param amMeta application master meta data.
	 * @return application master state machine.
	 */
	abstract protected StateMachine<AMStatus, AMEventType, AMEvent> buildStateMachine(MLContext mlContext,
			AMMeta amMeta);

	/**
	 * sent am event to state machine.
	 * @param event application master handle event.
	 * @return handle am event or not.
	 */
	public boolean sendEvent(AMEvent event) {
		try {
			return eventQueue.offer(event, 5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * state machine getter.
	 * @return am state machine.
	 */
	protected StateMachine<AMStatus, AMEventType, AMEvent> getStateMachine() {
		return this.stateMachine;
	}

	/**
	 * get state machine status.
 	 * @return state machine status.
	 */
	public AMStatus getInternalState() {
		readLock.lock();
		try {
			return getStateMachine().getCurrentState();
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * handle am event.
	 * @param amEvent application master event.
	 * @throws Exception
	 */
	@Override
	public void handle(AMEvent amEvent) throws Exception {
		try {
			writeLock.lock();
			if (0 != amEvent.getVersion() && amService.version() != amEvent.getVersion()) {
				return;
			}
			AMStatus oldState = getInternalState();
			try {
				getStateMachine().doTransition(amEvent.getType(), amEvent);
			} catch (InvalidStateTransitionException e) {
				e.printStackTrace();
				Logger.info("Can't handle this event at current state");
				if (oldState != getInternalState()) {
					Logger.info("Job Transitioned from " + oldState + " to " + getInternalState());
				}
				throw e;
			}
			Logger.info("AM doTransition:" + oldState.toString() + " => " + getInternalState().toString());

		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * return application master service.
	 * @return application master service.
	 */
	public AMService getAmService() {
		return amService;
	}

	/**
	 * @return BaseEventReporter
	 */
	public BaseEventReporter getEventReporter() {
		return eventReporter;
	}

	/**
	 * @return AMMeta object for handle AM meta.
	 */
	public AMMeta getAMMeta() {
		return amMeta;
	}

	/**
	 * @return am node runtime context.
	 */
	public MLContext getMLContext() {
		return mlContext;
	}

	/**
	 * stop am event handler thread.
	 */
	public void close() {
		if (null != exService && !exService.isShutdown()) {
			exService.shutdownNow();
			try {
				exService.awaitTermination(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Logger.info("close am_event_handler thread!");
		}
	}
}
