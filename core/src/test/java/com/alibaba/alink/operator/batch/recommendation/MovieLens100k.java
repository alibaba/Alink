package com.alibaba.alink.operator.batch.recommendation;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;

public class MovieLens100k {
    private static final String PATH = "http://alink-testdata.oss-cn-hangzhou-zmf.aliyuncs.com/csv/ml-100k/";
    private static final String RATING_FILE = "u.data";
    private static final String USER_FILE = "u.user";
    private static final String ITEM_FILE = "u.item";
    private static final String RATING_TRAIN_FILE = "u2.base";
    private static final String RATING_TEST_FILE = "u2.test";

    private static final String RATING_SCHEMA_STRING = "user_id long, item_id long, rating float, ts long";

    private static final String USER_SCHEMA_STRING =
            "user_id long, age int, gender string, occupation string, zip_code string";

    private static final String ITEM_SCHEMA_STRING = "item_id long, title string, "
            + "release_date string, video_release_date string, imdb_url string, "
            + "unknown int, action int, adventure int, animation int, "
            + "children int, comedy int, crime int, documentary int, drama int, "
            + "fantasy int, film_noir int, horror int, musical int, mystery int, "
            + "romance int, sci_fi int, thriller int, war int, western int";

    static BatchOperator getUserInfo() {
        return new CsvSourceBatchOp()
                .setFilePath(PATH + USER_FILE)
                .setFieldDelimiter("|")
                .setSchemaStr(USER_SCHEMA_STRING);
    }

    static BatchOperator getItemInfo() {
        return new CsvSourceBatchOp()
                .setFilePath(PATH + ITEM_FILE)
                .setFieldDelimiter("|")
                .setSchemaStr(ITEM_SCHEMA_STRING);
    }

    static BatchOperator getRatingsData() {
        return new CsvSourceBatchOp()
                .setFilePath(PATH + RATING_FILE)
                .setFieldDelimiter("\t")
                .setSchemaStr(RATING_SCHEMA_STRING);
    }

    static BatchOperator getRatingsTrainData() {
        return new CsvSourceBatchOp()
                .setFilePath(PATH + RATING_TRAIN_FILE)
                .setFieldDelimiter("\t")
                .setSchemaStr(RATING_SCHEMA_STRING);
    }

    static BatchOperator getRatingsTestData() {
        return new CsvSourceBatchOp()
                .setFilePath(PATH + RATING_TEST_FILE)
                .setFieldDelimiter("\t")
                .setSchemaStr(RATING_SCHEMA_STRING);
    }
}
