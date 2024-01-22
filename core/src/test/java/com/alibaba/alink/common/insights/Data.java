package com.alibaba.alink.common.insights;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.CsvSourceLocalOp;

public class Data {

	Data() {}

	String getPath(String fileName) {
		return getClass().getClassLoader().getResource(fileName).getPath();
	}

	public static LocalOperator <?> getCarSalesLocalSource() {
		String filePath = new Data().getPath("CarSales.csv");
		String schema = "year string, brand string, category string, model string, sales double";

		return new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true);
	}

	public static BatchOperator <?> getCarSalesBatchSource() {
		String filePath = new Data().getPath("CarSales.csv");
		String schema = "year string, brand string, category string, model string, sales double";

		return new CsvSourceBatchOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true);
	}

	public static BatchOperator <?> getCensusBatchSource() {
		String filePath = new Data().getPath("Census.csv");
		String schema
			= "Birthday string, AgeSegment string, MaritalStatus string, Sex string, AgeGroup string, CountOfPersons "
			+ "int";
		return new CsvSourceBatchOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true);
	}

	public static BatchOperator <?> getEmissionBatchSource() {
		String filePath = new Data().getPath("Emission.csv");
		String schema
			= "Year string, State string, ProducerType string, EnergySource string, CO2_kt double, SO2_kt double, "
			+ "NOx_kt double";
		return new CsvSourceBatchOp()
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

	static LocalOperator <?> getCriteo_10W_LocalSource() {
		String filePath = "https://alink-example-data.oss-cn-hangzhou-zmf.aliyuncs.com/criteo_random_10w_test_data";

		String schemaStr = "label int,nf01 int,nf02 int,nf03 int,nf04 int,nf05 int,nf06 int,nf07 int,nf08 int,nf09 "
			+ "int,nf10 int,nf11 int,nf12 int,nf13 int,cf01 string,cf02 string,cf03 string,cf04 string,cf05 string,"
			+ "cf06 string,cf07 string,cf08 string,cf09 string,cf10 string,cf11 string,cf12 string,cf13 string,cf14 "
			+ "string,cf15 string,cf16 string,cf17 string,cf18 string,cf19 string,cf20 string,cf21 string,cf22 string,"
			+ "cf23 string,cf24 string,cf25 string,cf26 string";

		return new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schemaStr);
	}

	static BatchOperator <?> getCriteo_10W_BatchSource() {
		String filePath = "https://alink-example-data.oss-cn-hangzhou-zmf.aliyuncs.com/criteo_random_10w_test_data";

		String schemaStr = "label int,nf01 int,nf02 int,nf03 int,nf04 int,nf05 int,nf06 int,nf07 int,nf08 int,nf09 "
			+ "int,nf10 int,nf11 int,nf12 int,nf13 int,cf01 string,cf02 string,cf03 string,cf04 string,cf05 string,"
			+ "cf06 string,cf07 string,cf08 string,cf09 string,cf10 string,cf11 string,cf12 string,cf13 string,cf14 "
			+ "string,cf15 string,cf16 string,cf17 string,cf18 string,cf19 string,cf20 string,cf21 string,cf22 string,"
			+ "cf23 string,cf24 string,cf25 string,cf26 string";

		return new CsvSourceBatchOp()
			.setFilePath(filePath)
			.setSchemaStr(schemaStr);
	}

	static LocalOperator <?> getCriteo_90W_LocalSource() {
		String filePath = "http://alink-example-data.oss-cn-hangzhou-zmf.aliyuncs.com/criteo_random_90w_train_data";

		String schemaStr = "label int,nf01 int,nf02 int,nf03 int,nf04 int,nf05 int,nf06 int,nf07 int,nf08 int,nf09 "
			+ "int,nf10 int,nf11 int,nf12 int,nf13 int,cf01 string,cf02 string,cf03 string,cf04 string,cf05 string,"
			+ "cf06 string,cf07 string,cf08 string,cf09 string,cf10 string,cf11 string,cf12 string,cf13 string,cf14 "
			+ "string,cf15 string,cf16 string,cf17 string,cf18 string,cf19 string,cf20 string,cf21 string,cf22 string,"
			+ "cf23 string,cf24 string,cf25 string,cf26 string";

		return new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schemaStr);
	}

	static BatchOperator <?> getCriteo_90W_BatchSource() {
		String filePath = "http://alink-example-data.oss-cn-hangzhou-zmf.aliyuncs.com/criteo_random_90w_train_data";

		String schemaStr = "label int,nf01 int,nf02 int,nf03 int,nf04 int,nf05 int,nf06 int,nf07 int,nf08 int,nf09 "
			+ "int,nf10 int,nf11 int,nf12 int,nf13 int,cf01 string,cf02 string,cf03 string,cf04 string,cf05 string,"
			+ "cf06 string,cf07 string,cf08 string,cf09 string,cf10 string,cf11 string,cf12 string,cf13 string,cf14 "
			+ "string,cf15 string,cf16 string,cf17 string,cf18 string,cf19 string,cf20 string,cf21 string,cf22 string,"
			+ "cf23 string,cf24 string,cf25 string,cf26 string";

		return new CsvSourceBatchOp()
			.setFilePath(filePath)
			.setSchemaStr(schemaStr);
	}

}
