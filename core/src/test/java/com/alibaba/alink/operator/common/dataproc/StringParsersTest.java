package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class StringParsersTest {
    @Test
    public void testJsonParser() throws Exception {
        String jsonStr = "{\n" +
            "  \"media_name\": \"Titanic\",\n" +
            "  \"title\": \"Titanic\",\n" +
            "  \"compare_point\": 0.0001,\n" +
            "  \"spider_point\": 0.0000,\n" +
            "  \"search_point\": 0.6,\n" +
            "  \"collection_id\": 123456,\n" +
            "  \"media_id\": 3214\n" +
            "}";

        String schemaStr
            = "media_name string, title string, compare_point double, spider_point double, search_point double, "
            + "collection_id bigint, media_id bigint";

        TableSchema schema = CsvUtil.schemaStr2Schema(schemaStr);
        StringParsers.JsonParser parser = new StringParsers.JsonParser(schema.getFieldNames(), schema.getFieldTypes());
        Tuple2<Boolean, Row> parsed = parser.parse(jsonStr);
        Assert.assertTrue(parsed.f0);
        Assert.assertEquals(parsed.f1.getArity(), 7);
    }

    @Test
    public void testKvParser() throws Exception {
        String kvStr = "f1=1,f2=2.0,f3=false,f4=val,f5=2018-09-10,f6=14:22:20,f7=2018-09-10 14:22:20";
        String schemaStr = "f1 bigint, f2 double, f3 boolean, f4 string, f5 date, f6 time, f7 timestamp";

        TableSchema schema = CsvUtil.schemaStr2Schema(schemaStr);
        StringParsers.KvParser parser = new StringParsers.KvParser(schema.getFieldNames(), schema.getFieldTypes(), ",", "=");
        Tuple2<Boolean, Row> parsed = parser.parse(kvStr);
        Assert.assertTrue(parsed.f0);
        Assert.assertEquals(parsed.f1.getArity(), 7);
    }

    @Test
    public void testCsvParser() throws Exception {
        StringParsers.CsvParser parser = new StringParsers.CsvParser(
            new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING}, "____", '"');
        Assert.assertEquals(parser.parse("hello_____world____").f1.getField(0), "hello");
        Assert.assertEquals(parser.parse("hello_____world____").f1.getField(1), "_world");
        Assert.assertEquals(parser.parse("hello_____world____").f1.getField(2), null);
        Assert.assertEquals(parser.parse("\"hello_____world____\"").f1.getField(0), "hello_____world____");
        Assert.assertEquals(parser.parse("\"hello_____world____\"").f1.getField(1), null);
        Assert.assertEquals(parser.parse("\"hello_____world____\"").f1.getField(2), null);
    }

}