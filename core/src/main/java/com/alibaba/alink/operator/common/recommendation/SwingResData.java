package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;

import java.util.ArrayList;
import java.util.List;

public class SwingResData implements AlinkSerializable {
	private Object[] object;
	private Float[] score;
	private String itemCol;

	public SwingResData() {
	}

	public SwingResData(Object[] object, Float[] score, String itemCol) {
		this.object = object;
		this.score = score;
		this.itemCol = itemCol;
	}

	public void setObject(Object[] object) {
		this.object = object;
	}

	public void setScore(Float[] score) {
		this.score = score;
	}

	public Object[] getObject() {
		return object;
	}

	public Float[] getScore() {
		return score;
	}

	public MTable returnTopNData(int topN, TypeInformation <?> objType) {
		int thisSize = Math.min(object.length, topN);

		List <Row> rows = new ArrayList <>(thisSize);
		for (int i = 0; i < thisSize; i++) {
			rows.add(Row.of(object[i], Double.valueOf(score[i])));
		}
		return new MTable(rows, itemCol + " " + FlinkTypeConverter.getTypeString(objType) + "," + "score DOUBLE");
	}
}
