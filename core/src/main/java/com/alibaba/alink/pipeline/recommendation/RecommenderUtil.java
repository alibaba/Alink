package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.pipeline.LocalPredictor;

import java.util.List;

public class RecommenderUtil {
	public static LocalPredictor createRecommLocalPredictor(
		BaseRecommender<?> recommender, TableSchema modelSchema, TableSchema dataSchema, List <Row> data) {

		RecommMapper recommMapper = new RecommMapper(
			recommender.recommKernelBuilder, recommender.recommType,
			modelSchema, dataSchema, recommender.getParams()
		);

		recommMapper.loadModel(data);

		return new LocalPredictor(recommMapper);
	}
}
