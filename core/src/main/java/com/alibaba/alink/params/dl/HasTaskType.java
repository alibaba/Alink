package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.dl.TaskType;

public interface HasTaskType<T> extends WithParams <T> {


	ParamInfo <TaskType> TASK_TYPE = ParamInfoFactory
		.createParamInfo("taskType", TaskType.class)
		.setDescription("Task type")
		.setRequired()
		.build();

	default TaskType getTaskType() {
		return get(TASK_TYPE);
	}

	default T setTaskType(TaskType taskType) {
		return set(TASK_TYPE, taskType);
	}

	default T setTaskType(String taskTypeStr) { return set(TASK_TYPE, TaskType.valueOf(taskTypeStr)); }
}
