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

import com.alibaba.flink.ml.cluster.statemachine.event.AbstractEvent;

/**
 * application master handle event.
 * application master state machine can process this event and trigger status change.
 */
public class AMEvent extends AbstractEvent<AMEventType> {
	private Object message;
	private long version;

	/**
	 * create am event object.
	 * @param amEventType am event type.
	 * @param message event message, different event type
	 * @param version machine learning cluster run version.
	 */
	public AMEvent(AMEventType amEventType, Object message, long version) {
		super(amEventType);
		this.message = message;
		this.version = version;
	}

	/**
	 * @return AMEvent additional message.
	 */
	public Object getMessage() {
		return message;
	}

	/**
	 * AMEvent additional message setter.
	 * @param message AMEvent additional message.
	 */
	public void setMessage(Object message) {
		this.message = message;
	}

	/**
	 * @return AMEvent version getter.
	 */
	public long getVersion() {
		return version;
	}

	/**
	 * AMEvent version setter.
	 * @param version AMEvent version setter.
	 */
	public void setVersion(long version) {
		this.version = version;
	}
}
