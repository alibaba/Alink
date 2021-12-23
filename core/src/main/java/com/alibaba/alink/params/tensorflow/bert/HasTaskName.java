package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.dl.BertTaskName;

public interface HasTaskName<T> extends WithParams <T> {

	/**
	 * @cn 任务名
	 * @cn-name 任务名
	 */
	ParamInfo <BertTaskName> TASK_NAME = ParamInfoFactory
		.createParamInfo("taskName", BertTaskName.class)
		.setDescription("Task name")
		.setRequired()
		.build();

	default BertTaskName getTaskName() {
		return get(TASK_NAME);
	}

	default T setTaskName(BertTaskName taskName) {
		return set(TASK_NAME, taskName);
	}

	default T setTaskName(String taskNameStr) { return set(TASK_NAME, BertTaskName.valueOf(taskNameStr)); }
}
