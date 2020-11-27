package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
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
		private final List <Tuple3 <Integer, ParamInfo, Object>> stageParamInfos;

		/**
		 * The metric.
		 */
		private final Double metric;

		/**
		 * The reason if tuning step failed.
		 */
		private final String reason;

		public ReportElement(
			Pipeline pipeline,
			List <Tuple3 <Integer, ParamInfo, Object>> stageParamInfos,
			Double metric) {

			this(pipeline, stageParamInfos, metric, null);
		}

		public ReportElement(
			Pipeline pipeline,
			List <Tuple3 <Integer, ParamInfo, Object>> stageParamInfos,
			Double metric,
			String reason) {
			this.pipeline = pipeline;
			this.stageParamInfos = stageParamInfos;
			this.metric = metric;
			this.reason = reason;
		}

		public Pipeline getPipeline() {
			return pipeline;
		}

		public List <Tuple3 <Integer, ParamInfo, Object>> getStageParamInfos() {
			return stageParamInfos;
		}

		public Double getMetric() {
			return metric;
		}

		public String getReason() {
			return reason;
		}
	}

	private final TuningEvaluator <?> tuningEvaluator;
	private final List <ReportElement> elements;

	public Report(
		TuningEvaluator <?> tuningEvaluator,
		List <ReportElement> elements) {

		this.tuningEvaluator = tuningEvaluator;
		this.elements = new ArrayList <>();
		this.elements.addAll(elements);

		if (this.tuningEvaluator.isLargerBetter()) {
			this.elements.sort((o1, o2) -> {
				if (Double.isNaN(o1.getMetric()) && Double.isNaN(o2.getMetric())) {
					return 0;
				}

				if (Double.isNaN(o1.getMetric())) {
					return 1;
				}

				if (Double.isNaN(o2.getMetric())) {
					return -1;
				}

				return o2.getMetric().compareTo(o1.getMetric());
			});
		} else {
			this.elements.sort((o1, o2) -> {
				if (Double.isNaN(o1.getMetric()) && Double.isNaN(o2.getMetric())) {
					return 0;
				}

				if (Double.isNaN(o1.getMetric())) {
					return 1;
				}

				if (Double.isNaN(o2.getMetric())) {
					return -1;
				}

				return o1.getMetric().compareTo(o2.getMetric());
			});
		}
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
		List <Map <String, Object>> jsonMap = new ArrayList <>();
		for (ReportElement element : elements) {
			List <JsonParamInfo> jsonParamInfos = new ArrayList <>();

			for (Tuple3 <Integer, ParamInfo, Object> stageParamInfo : element.getStageParamInfos()) {
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

			Map <String, Object> subModelMap = new HashMap <>();
			subModelMap.put("param", jsonParamInfos);
			subModelMap.put(tuningEvaluator.getMetricParamInfo().getName(), element.getMetric());

			if (element.getReason() != null && !element.getReason().isEmpty()) {
				subModelMap.put("reason", element.getReason());
			}

			jsonMap.add(subModelMap);
		}

		return JsonConverter.toPrettyJson(jsonMap);
	}

	@Override
	public String toString() {
		if (elements == null || elements.isEmpty()) {
			return "";
		}

		final int indentSize = 2;

		StringBuilder stringBuilder = new StringBuilder();

		stringBuilder
			.append("Metric information:")
			.append("\n");
		stringBuilder
			.append(PrettyDisplayUtils.indentLines("Metric name: ", indentSize))
			.append(tuningEvaluator.getMetricParamInfo().getName())
			.append("\n");
		stringBuilder
			.append(PrettyDisplayUtils.indentLines("Larger is better: ", indentSize))
			.append(tuningEvaluator.isLargerBetter())
			.append("\n");

		ArrayList <String> title = new ArrayList <>();

		title.add(tuningEvaluator.getMetricParamInfo().getName());

		int index = 1;
		for (Tuple3 <Integer, ParamInfo, Object> stageParamInfo : elements.get(0).getStageParamInfos()) {
			if (index > 1) {
				title.add("stage " + index);
				title.add("param " + index);
				title.add("value " + index);
			} else {
				title.add("stage");
				title.add("param");
				title.add("value");
			}
			index++;
		}

		ArrayList <Object[]> itemsList = new ArrayList <>();

		StringBuilder failedInformationBuilder = new StringBuilder();

		for (ReportElement element : elements) {
			ArrayList <Object> item = new ArrayList <>();

			item.add(element.getMetric());

			for (Tuple3 <Integer, ParamInfo, Object> stageParamInfo : element.getStageParamInfos()) {
				item.add(
					element
						.getPipeline()
						.get(stageParamInfo.f0)
						.getClass()
						.getSimpleName()
				);
				item.add(stageParamInfo.f1.getName());
				item.add(stageParamInfo.f2);
			}

			Object[] itemArray = item.toArray();

			if (element.getReason() != null && !element.getReason().isEmpty()) {
				failedInformationBuilder
					.append("\n")
					.append("Param information:")
					.append("\n")
					.append(
						PrettyDisplayUtils.indentLines(
							PrettyDisplayUtils.displayTable(
								new Object[][] {itemArray},
								1,
								title.size(),
								null,
								title.toArray(new String[0]),
								null,
								Integer.MAX_VALUE,
								Integer.MAX_VALUE,
								true
							),
							indentSize
						)
					)
					.append("\n")
					.append("Failed reason:")
					.append("\n")
					.append(
						PrettyDisplayUtils.indentLines(
							element.getReason(), indentSize
						)
					);
			}

			itemsList.add(itemArray);
		}

		stringBuilder
			.append("Tuning information:")
			.append("\n")
			.append(
				PrettyDisplayUtils.indentLines(
					PrettyDisplayUtils.displayTable(
						itemsList.toArray(new Object[0][]),
						itemsList.size(),
						title.size(),
						null,
						title.toArray(new String[0]),
						null,
						Integer.MAX_VALUE,
						Integer.MAX_VALUE,
						true
					),
					indentSize
				)
			);

		String failedInformation = failedInformationBuilder.toString();

		if (!failedInformation.isEmpty()) {
			stringBuilder
				.append("\n")
				.append("Failed information:")
				.append(
					PrettyDisplayUtils.indentLines(
						failedInformation, indentSize
					)
				);
		}

		return stringBuilder.toString();
	}
}
