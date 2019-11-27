package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.pipeline.Pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The report of tuning and it can get the pretty json.
 */
public class Report {
	/**
	 * ReportElement.
	 */
	public static class ReportElement {
		/**
		 * The Pipeline.
		 */
		private final Pipeline pipeline;

		/**
		 * Stage index, parameter's key of the tuning, parameter's value of the tuning
		 */
		private final List<Tuple3<Integer, ParamInfo, Object>> stageParamInfos;

		/**
		 * The metric.
		 */
		private final Double metric;

		public ReportElement(
			Pipeline pipeline,
			List<Tuple3<Integer, ParamInfo, Object>> stageParamInfos,
			Double metric) {
			this.pipeline = pipeline;
			this.stageParamInfos = stageParamInfos;
			this.metric = metric;
		}

		public Pipeline getPipeline() {
			return pipeline;
		}

		public List<Tuple3<Integer, ParamInfo, Object>> getStageParamInfos() {
			return stageParamInfos;
		}

		public Double getMetric() {
			return metric;
		}
	}


	private final List<ReportElement> elements;

	public Report(
		List<ReportElement> elements) {
		this.elements = elements;
	}

	private static class JsonParamInfo {
		String stage;
		String paramName;
		Object paramValue;

		JsonParamInfo(String stage, String paramName, Object paramValue) {
			this.stage = stage;
			this.paramName = paramName;
			this.paramValue = paramValue;
		}
	}

	public String toPrettyJson() {
		List<Map<String, Object>> jsonMap = new ArrayList<>();
		for (ReportElement element : elements) {
			List<JsonParamInfo> jsonParamInfos = new ArrayList<>();

			for (Tuple3<Integer, ParamInfo, Object> stageParamInfo : element.getStageParamInfos()) {
				jsonParamInfos.add(
					new JsonParamInfo(
						element
							.getPipeline()
							.get(stageParamInfo.f0)
							.getClass()
							.getSimpleName(),
						stageParamInfo.f1.getName(),
						stageParamInfo.f2
					)
				);
			}

			Map<String, Object> subModelMap = new HashMap<>();
			subModelMap.put("param", jsonParamInfos);
			subModelMap.put("metric", element.getMetric());
			jsonMap.add(subModelMap);
		}

		return JsonConverter.toPrettyJson(jsonMap);
	}
}
