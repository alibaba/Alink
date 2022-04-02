package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.recommendation.RecommMapper;

import com.alibaba.alink.common.mapper.Mapper;

import java.util.List;

public class RecommenderUtil {
	//not load and not open.
	public static Mapper createRecommMapper(
		BaseRecommender<?> recommender, TableSchema modelSchema, TableSchema dataSchema, List <Row> data) {

		RecommMapper mapper =  new RecommMapper(
			recommender.recommKernelBuilder, recommender.recommType,
			modelSchema , dataSchema, recommender.getParams()
		);

		if(data != null) {
			mapper.loadModel(data);
		}
		return mapper;
	}
}
