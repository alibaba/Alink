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

package com.alibaba.flink.ml.cluster.statemachine;

import com.alibaba.flink.ml.cluster.statemachine.transition.MultipleArcTransition;
import com.alibaba.flink.ml.cluster.statemachine.transition.SingleArcTransition;

import java.util.*;

final public class StateMachineBuilder
		<OPERAND, STATE extends Enum<STATE>,
				EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {

	private final StateMachineBuilder.TransitionsListNode transitionsListNode;

	private Map<STATE, Map<EVENTTYPE,
			Transition<OPERAND, STATE, EVENTTYPE, EVENT>>> stateMachineTable;

	private STATE defaultInitialState;

	private final boolean optimized;

	/**
	 * Constructor
	 *
	 * This is the only constructor in the API.
	 */
	public StateMachineBuilder(STATE defaultInitialState) {
		this.transitionsListNode = null;
		this.defaultInitialState = defaultInitialState;
		this.optimized = false;
		this.stateMachineTable = null;
	}

	private StateMachineBuilder
			(StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT> that,
					StateMachineBuilder.ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> t) {
		this.defaultInitialState = that.defaultInitialState;
		this.transitionsListNode
				= new StateMachineBuilder.TransitionsListNode(t, that.transitionsListNode);
		this.optimized = false;
		this.stateMachineTable = null;
	}

	private StateMachineBuilder
			(StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT> that,
					boolean optimized) {
		this.defaultInitialState = that.defaultInitialState;
		this.transitionsListNode = that.transitionsListNode;
		this.optimized = optimized;
		if (optimized) {
			makeStateMachineTable();
		} else {
			stateMachineTable = null;
		}
	}

	private interface ApplicableTransition
			<OPERAND, STATE extends Enum<STATE>,
					EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
		void apply(StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT> subject);
	}

	private class TransitionsListNode {
		final StateMachineBuilder.ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition;
		final StateMachineBuilder.TransitionsListNode next;

		TransitionsListNode
				(StateMachineBuilder.ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition,
						StateMachineBuilder.TransitionsListNode next) {
			this.transition = transition;
			this.next = next;
		}
	}

	static private class ApplicableSingleOrMultipleTransition
			<OPERAND, STATE extends Enum<STATE>,
					EVENTTYPE extends Enum<EVENTTYPE>, EVENT>
			implements StateMachineBuilder.ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> {
		final STATE preState;
		final EVENTTYPE eventType;
		final StateMachineBuilder.Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition;

		ApplicableSingleOrMultipleTransition
				(STATE preState, EVENTTYPE eventType,
						StateMachineBuilder.Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition) {
			this.preState = preState;
			this.eventType = eventType;
			this.transition = transition;
		}

		@Override
		public void apply
				(StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT> subject) {
			Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap
					= subject.stateMachineTable.get(preState);
			if (transitionMap == null) {
				// I use HashMap here because I would expect most EVENTTYPE's to not
				//  apply out of a particular state, so FSM sizes would be
				//  quadratic if I use EnumMap's here as I do at the top level.
				transitionMap = new HashMap<EVENTTYPE,
						Transition<OPERAND, STATE, EVENTTYPE, EVENT>>();
				subject.stateMachineTable.put(preState, transitionMap);
			}
			transitionMap.put(eventType, transition);
		}
	}

	/**
	 * @param preState pre-transition state
	 * @param postState post-transition state
	 * @param eventType stimulus for the transition
	 * @return a NEW StateMachineFactory just like {@code this} with the current
	 * transition added as a new legal transition.  This overload
	 * has no hook object.
	 *
	 * Note that the returned StateMachineFactory is a distinct
	 * object.
	 *
	 * This method is part of the API.
	 */
	public StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT>
	addTransition(STATE preState, STATE postState, EVENTTYPE eventType) {
		return addTransition(preState, postState, eventType, null);
	}

	/**
	 * @param preState pre-transition state
	 * @param postState post-transition state
	 * @param eventTypes List of stimuli for the transitions
	 * @return a NEW StateMachineFactory just like {@code this} with the current
	 * transition added as a new legal transition.  This overload
	 * has no hook object.
	 *
	 *
	 * Note that the returned StateMachineFactory is a distinct
	 * object.
	 *
	 * This method is part of the API.
	 */
	public StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(
			STATE preState, STATE postState, Set<EVENTTYPE> eventTypes) {
		return addTransition(preState, postState, eventTypes, null);
	}

	/**
	 * @param preState pre-transition state
	 * @param postState post-transition state
	 * @param eventTypes List of stimuli for the transitions
	 * @param hook transition hook
	 * @return a NEW StateMachineFactory just like {@code this} with the current
	 * transition added as a new legal transition
	 *
	 * Note that the returned StateMachineFactory is a distinct
	 * object.
	 *
	 * This method is part of the API.
	 */
	public StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(
			STATE preState, STATE postState, Set<EVENTTYPE> eventTypes,
			SingleArcTransition<OPERAND, EVENT> hook) {
		StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT> factory = null;
		for (EVENTTYPE event : eventTypes) {
			if (factory == null) {
				factory = addTransition(preState, postState, event, hook);
			} else {
				factory = factory.addTransition(preState, postState, event, hook);
			}
		}
		return factory;
	}

	/**
	 * @param preState pre-transition state
	 * @param postState post-transition state
	 * @param eventType stimulus for the transition
	 * @param hook transition hook
	 * @return a NEW StateMachineFactory just like {@code this} with the current
	 * transition added as a new legal transition
	 *
	 * Note that the returned StateMachineFactory is a distinct object.
	 *
	 * This method is part of the API.
	 */
	public StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT>
	addTransition(STATE preState, STATE postState,
			EVENTTYPE eventType,
			SingleArcTransition<OPERAND, EVENT> hook) {
		return new StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT>
				(this, new StateMachineBuilder.ApplicableSingleOrMultipleTransition<OPERAND, STATE, EVENTTYPE, EVENT>
						(preState, eventType, new StateMachineBuilder.SingleInternalArc(postState, hook)));
	}

	/**
	 * @param preState pre-transition state
	 * @param postStates valid post-transition states
	 * @param eventType stimulus for the transition
	 * @param hook transition hook
	 * @return a NEW StateMachineFactory just like {@code this} with the current
	 * transition added as a new legal transition
	 *
	 * Note that the returned StateMachineFactory is a distinct object.
	 *
	 * This method is part of the API.
	 */
	public StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT>
	addTransition(STATE preState, Set<STATE> postStates,
			EVENTTYPE eventType,
			MultipleArcTransition<OPERAND, EVENT, STATE> hook) {
		return new StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT>
				(this,
						new StateMachineBuilder.ApplicableSingleOrMultipleTransition<OPERAND, STATE, EVENTTYPE, EVENT>
								(preState, eventType, new StateMachineBuilder.MultipleInternalArc(postStates, hook)));
	}

	/**
	 * @return a StateMachineFactory just like {@code this}, except that if
	 * you won't need any synchronization to build a state machine
	 *
	 * Note that the returned StateMachineFactory is a distinct object.
	 *
	 * This method is part of the API.
	 *
	 * The only way you could distinguish the returned
	 * StateMachineFactory from {@code this} would be by
	 * measuring the performance of the derived
	 * {@code StateMachine} you can get from it.
	 *
	 * Calling this is optional.  It doesn't change the semantics of the factory,
	 * if you call it then when you use the factory there is no synchronization.
	 */
	public StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT>
	installTopology() {
		return new StateMachineBuilder<OPERAND, STATE, EVENTTYPE, EVENT>(this, true);
	}

	/**
	 * Effect a transition due to the effecting stimulus.
	 *
	 * @param oldState current state
	 * @param operand trigger to initiate the transition
	 * @param eventType causal eventType context
	 * @return transitioned state
	 */
	private STATE doTransition
	(OPERAND operand, STATE oldState, EVENTTYPE eventType, EVENT event)
			throws InvalidStateTransitionException {
		// We can assume that stateMachineTable is non-null because we call
		//  maybeMakeStateMachineTable() when we build an InnerStateMachine ,
		//  and this code only gets called from inside a working InnerStateMachine .
		Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap
				= stateMachineTable.get(oldState);
		if (transitionMap != null) {
			StateMachineBuilder.Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition
					= transitionMap.get(eventType);
			if (transition != null) {
				return transition.doTransition(operand, oldState, event, eventType);
			}
		}
		throw new InvalidStateTransitionException(oldState, eventType);
	}

	private synchronized void maybeMakeStateMachineTable() {
		if (stateMachineTable == null) {
			makeStateMachineTable();
		}
	}

	private void makeStateMachineTable() {
		Stack<ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>> stack =
				new Stack<ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>>();

		Map<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>
				prototype = new HashMap<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>();

		prototype.put(defaultInitialState, null);

		// I use EnumMap here because it'll be faster and denser.  I would
		//  expect most of the states to have at least one transition.
		stateMachineTable
				= new EnumMap<STATE, Map<EVENTTYPE,
				Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>(prototype);

		for (StateMachineBuilder.TransitionsListNode cursor = transitionsListNode;
		     cursor != null;
		     cursor = cursor.next) {
			stack.push(cursor.transition);
		}

		while (!stack.isEmpty()) {
			stack.pop().apply(this);
		}
	}

	private interface Transition<OPERAND, STATE extends Enum<STATE>,
			EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
		STATE doTransition(OPERAND operand, STATE oldState,
				EVENT event, EVENTTYPE eventType) throws InvalidStateTransitionException;
	}

	private class SingleInternalArc
			implements StateMachineBuilder.Transition<OPERAND, STATE, EVENTTYPE, EVENT> {

		private STATE postState;
		private SingleArcTransition<OPERAND, EVENT> hook; // transition hook

		SingleInternalArc(STATE postState,
				SingleArcTransition<OPERAND, EVENT> hook) {
			this.postState = postState;
			this.hook = hook;
		}

		@Override
		public STATE doTransition(OPERAND operand, STATE oldState,
				EVENT event, EVENTTYPE eventType) throws InvalidStateTransitionException {
			if (hook != null) {
				hook.transition(operand, event);
			}
			return postState;
		}
	}

	private class MultipleInternalArc
			implements StateMachineBuilder.Transition<OPERAND, STATE, EVENTTYPE, EVENT> {

		// Fields
		private Set<STATE> validPostStates;
		private MultipleArcTransition<OPERAND, EVENT, STATE> hook;  // transition hook

		MultipleInternalArc(Set<STATE> postStates,
				MultipleArcTransition<OPERAND, EVENT, STATE> hook) {
			this.validPostStates = postStates;
			this.hook = hook;
		}

		@Override
		public STATE doTransition(OPERAND operand, STATE oldState,
				EVENT event, EVENTTYPE eventType)
				throws InvalidStateTransitionException {
			STATE postState = hook.transition(operand, event);

			if (!validPostStates.contains(postState)) {
				throw new InvalidStateTransitionException(oldState, eventType);
			}
			return postState;
		}
	}

	/*
	 * @return a {@link StateMachine} that starts in
	 *         {@code initialState} and whose {@link Transition} s are
	 *         applied to {@code operand} .
	 *
	 *         This is part of the API.
	 *
	 * @param operand the object upon which the returned
	 *                {@link StateMachine} will operate.
	 * @param initialState the state in which the returned
	 *                {@link StateMachine} will start.
	 *
	 */
	public StateMachine<STATE, EVENTTYPE, EVENT>
	make(OPERAND operand, STATE initialState) {
		return new StateMachineBuilder.InternalStateMachine(operand, initialState);
	}

	/*
	 * @return a {@link StateMachine} that starts in the default initial
	 *          state and whose {@link Transition} s are applied to
	 *          {@code operand} .
	 *
	 *         This is part of the API.
	 *
	 * @param operand the object upon which the returned
	 *                {@link StateMachine} will operate.
	 *
	 */
	public StateMachine<STATE, EVENTTYPE, EVENT> make(OPERAND operand) {
		return new StateMachineBuilder.InternalStateMachine(operand, defaultInitialState);
	}

	private class InternalStateMachine
			implements StateMachine<STATE, EVENTTYPE, EVENT> {
		private final OPERAND operand;
		private STATE currentState;

		InternalStateMachine(OPERAND operand, STATE initialState) {
			this.operand = operand;
			this.currentState = initialState;
			if (!optimized) {
				maybeMakeStateMachineTable();
			}
		}

		@Override
		public synchronized STATE getCurrentState() {
			return currentState;
		}

		@Override
		public synchronized STATE doTransition(EVENTTYPE eventType, EVENT event)
				throws InvalidStateTransitionException {
			currentState = StateMachineBuilder.this.doTransition
					(operand, currentState, eventType, event);
			return currentState;
		}
	}
}

