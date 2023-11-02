package com.alibaba.alink.common.insights;

import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.CsvSourceLocalOp;

class Data {

	Data() {}

	String getPath(String fileName) {
		return getClass().getClassLoader().getResource(fileName).getPath();
	}

	static LocalOperator <?> getCarSalesLocalSource() {
		String filePath = new Data().getPath("CarSales.csv");
		String schema = "year string, brand string, category string, model string, sales double";

		return new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true);
	}

	static LocalOperator <?> getCensusLocalSource() {
		String filePath = new Data().getPath("Census.csv");
		String schema
			= "Birthday string, AgeSegment string, MaritalStatus string, Sex string, AgeGroup string, CountOfPersons "
			+ "int";

		return new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true);
	}

	static LocalOperator <?> getEmissionLocalSource() {
		String filePath = new Data().getPath("Emission.csv");
		String schema
			= "Year string, State string, ProducerType string, EnergySource string, CO2_kt double, SO2_kt double, "
			+ "NOx_kt double";

		return new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true);
	}

}
