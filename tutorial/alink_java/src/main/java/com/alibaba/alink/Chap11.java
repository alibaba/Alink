package com.alibaba.alink;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.StratifiedSampleBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.JoinBatchOp;
import com.alibaba.alink.operator.batch.sql.LeftOuterJoinBatchOp;
import com.alibaba.alink.operator.batch.statistics.CorrelationBatchOp;
import com.alibaba.alink.params.shared.tree.HasIndividualTreeType.TreeType;
import com.alibaba.alink.pipeline.classification.DecisionTreeClassifier;
import com.alibaba.alink.pipeline.classification.GbdtClassifier;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.classification.RandomForestClassifier;
import com.alibaba.alink.pipeline.dataproc.Imputer;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;

public class Chap11 {

    static final String DATA_DIR = Utils.ROOT_DIR + "tmall" + File.separator;

    static final String ORIGIN_FILE = "action_log.csv";

    static final String FEATURE_LABEL_FILE = "feature_label.ak";
    static final String TRAIN_FILE = "train.ak";
    static final String TEST_FILE = "test.ak";
    static final String TRAIN_SAMPLE_FILE = "train_sample.ak";

    static final String LABEL_COL_NAME = "label";
    static final String PREDICTION_COL_NAME = "pred";
    static final String PRED_DETAIL_COL_NAME = "predInfo";

    public static void main(String[] args) throws Exception {

        BatchOperator.setParallelism(1);

        c_1();

        c_3();

        c_4();

        c_7();

        c_8();

    }

    static CsvSourceBatchOp getSource() {
        return new CsvSourceBatchOp()
            .setFilePath(DATA_DIR + ORIGIN_FILE)
            .setSchemaStr("user_id long, brand_id long, type int, ts timestamp")
            .setFieldDelimiter("\t");
    }

    static void c_1() throws Exception {
        BatchOperator<?> source = getSource();

        source.lazyPrint(10, "origin file");

        source.lazyPrintStatistics("stat of origin file");

        source.link(
            new CorrelationBatchOp()
                .setSelectedCols("user_id", "brand_id", "type")
                .lazyPrintCorrelation()
        );

        source.select("min(ts) AS min_ts, max(ts) AS max_ts").lazyPrint(-1);

        source.groupBy("type", "type, COUNT(*) AS cnt").lazyPrint(-1);

        BatchOperator.execute();
    }

    static void c_3() throws Exception {
        BatchOperator<?> source = getSource();

        BatchOperator t1 = source.filter("ts < CAST('2014-07-16 00:00:00' AS TIMESTAMP)");
        BatchOperator t2 = source.filter("ts >= CAST('2014-07-16 00:00:00' AS TIMESTAMP)");

        t1.lazyPrint(3, "[ ts < '2014-07-16 00:00:00' ]")
            .lazyPrintStatistics();

        t2.lazyPrint(3, "[ ts >= '2014-07-16 00:00:00' ]")
            .lazyPrintStatistics();

        BatchOperator.execute();

        String clausePreProc = "user_id, brand_id, type, ts, past_days,"
            + "case when type=0 then 1 else 0 end AS is_click,"
            + "case when type=1 then 1 else 0 end AS is_buy,"
            + "case when type=2 then 1 else 0 end AS is_collect,"
            + "case when type=3 then 1 else 0 end AS is_cart,"
            + "case when type=0 and past_days<=30 then 1 else 0 end AS is_click_1m,"
            + "case when type=1 and past_days<=30 then 1 else 0 end AS is_buy_1m,"
            + "case when type=2 and past_days<=30 then 1 else 0 end AS is_collect_1m,"
            + "case when type=3 and past_days<=30 then 1 else 0 end AS is_cart_1m,"
            + "case when type=0 and past_days<=60 then 1 else 0 end AS is_click_2m,"
            + "case when type=1 and past_days<=60 then 1 else 0 end AS is_buy_2m,"
            + "case when type=2 and past_days<=60 then 1 else 0 end AS is_collect_2m,"
            + "case when type=3 and past_days<=60 then 1 else 0 end AS is_cart_2m,"
            + "case when type=0 and past_days<=90 then 1 else 0 end AS is_click_3m,"
            + "case when type=1 and past_days<=90 then 1 else 0 end AS is_buy_3m,"
            + "case when type=2 and past_days<=90 then 1 else 0 end AS is_collect_3m,"
            + "case when type=3 and past_days<=90 then 1 else 0 end AS is_cart_3m,"
            + "case when type=0 and past_days>30 and past_days<=60 then 1 else 0 end AS is_click_m2nd,"
            + "case when type=1 and past_days>30 and past_days<=60 then 1 else 0 end AS is_buy_m2nd,"
            + "case when type=2 and past_days>30 and past_days<=60 then 1 else 0 end AS is_collect_m2nd,"
            + "case when type=3 and past_days>30 and past_days<=60 then 1 else 0 end AS is_cart_m2nd,"
            + "case when type=0 and past_days>60 and past_days<=90 then 1 else 0 end AS is_click_m3th,"
            + "case when type=1 and past_days>60 and past_days<=90 then 1 else 0 end AS is_buy_m3th,"
            + "case when type=2 and past_days>60 and past_days<=90 then 1 else 0 end AS is_collect_m3th,"
            + "case when type=3 and past_days>60 and past_days<=90 then 1 else 0 end AS is_cart_m3th,"
            + "case when type=0 and past_days<=3 then 1 else 0 end AS is_click_3d,"
            + "case when type=1 and past_days<=3 then 1 else 0 end AS is_buy_3d,"
            + "case when type=2 and past_days<=3 then 1 else 0 end AS is_collect_3d,"
            + "case when type=3 and past_days<=3 then 1 else 0 end AS is_cart_3d,"
            + "case when type=0 and past_days>3 and past_days<=6 then 1 else 0 end AS is_click_3d2nd,"
            + "case when type=1 and past_days>3 and past_days<=6 then 1 else 0 end AS is_buy_3d2nd,"
            + "case when type=2 and past_days>3 and past_days<=6 then 1 else 0 end AS is_collect_3d2nd,"
            + "case when type=3 and past_days>3 and past_days<=6 then 1 else 0 end AS is_cart_3d2nd,"
            + "case when type=0 and past_days>6 and past_days<=9 then 1 else 0 end AS is_click_3d3th,"
            + "case when type=1 and past_days>6 and past_days<=9 then 1 else 0 end AS is_buy_3d3th,"
            + "case when type=2 and past_days>6 and past_days<=9 then 1 else 0 end AS is_collect_3d3th,"
            + "case when type=3 and past_days>6 and past_days<=9 then 1 else 0 end AS is_cart_3d3th,"
            + "case when type=0 and past_days<=7 then 1 else 0 end AS is_click_1w,"
            + "case when type=1 and past_days<=7 then 1 else 0 end AS is_buy_1w,"
            + "case when type=2 and past_days<=7 then 1 else 0 end AS is_collect_1w,"
            + "case when type=3 and past_days<=7 then 1 else 0 end AS is_cart_1w,"
            + "case when type=0 and past_days>7 and past_days<=14 then 1 else 0 end AS is_click_w2nd,"
            + "case when type=1 and past_days>7 and past_days<=14 then 1 else 0 end AS is_buy_w2nd,"
            + "case when type=2 and past_days>7 and past_days<=14 then 1 else 0 end AS is_collect_w2nd,"
            + "case when type=3 and past_days>7 and past_days<=14 then 1 else 0 end AS is_cart_w2nd,"
            + "case when type=0 and past_days>14 and past_days<=21 then 1 else 0 end AS is_click_w3th,"
            + "case when type=1 and past_days>14 and past_days<=21 then 1 else 0 end AS is_buy_w3th,"
            + "case when type=2 and past_days>14 and past_days<=21 then 1 else 0 end AS is_collect_w3th,"
            + "case when type=3 and past_days>14 and past_days<=21 then 1 else 0 end AS is_cart_w3th";

        BatchOperator t1_preproc = t1
            .select("user_id, brand_id, type, ts, "
                + "TIMESTAMPDIFF(DAY, ts, TIMESTAMP '2014-07-16 00:00:00') AS past_days")
            .select(clausePreProc);

        String clauseUserBrand = "user_id, brand_id, SUM(is_click) as cnt_click, SUM(is_buy) as cnt_buy, "
            + "SUM(is_collect) as cnt_collect, SUM(is_cart) as cnt_cart, "
            + "SUM(is_click_1m) as cnt_click_1m, SUM(is_buy_1m) as cnt_buy_1m, "
            + "SUM(is_collect_1m) as cnt_collect_1m, SUM(is_cart_1m) as cnt_cart_1m, "
            + "SUM(is_click_2m) as cnt_click_2m, SUM(is_buy_2m) as cnt_buy_2m, "
            + "SUM(is_collect_2m) as cnt_collect_2m, SUM(is_cart_2m) as cnt_cart_2m, "
            + "SUM(is_click_3m) as cnt_click_3m, SUM(is_buy_3m) as cnt_buy_3m, "
            + "SUM(is_collect_3m) as cnt_collect_3m, SUM(is_cart_3m) as cnt_cart_3m, "
            + "SUM(is_click_m2nd) as cnt_click_m2nd, SUM(is_buy_m2nd) as cnt_buy_m2nd, "
            + "SUM(is_collect_m2nd) as cnt_collect_m2nd, SUM(is_cart_m2nd) as cnt_cart_m2nd, "
            + "SUM(is_click_m3th) as cnt_click_m3th, SUM(is_buy_m3th) as cnt_buy_m3th, "
            + "SUM(is_collect_m3th) as cnt_collect_m3th, SUM(is_cart_m3th) as cnt_cart_m3th, "
            + "SUM(is_click_3d) as cnt_click_3d, SUM(is_buy_3d) as cnt_buy_3d, "
            + "SUM(is_collect_3d) as cnt_collect_3d, SUM(is_cart_3d) as cnt_cart_3d, "
            + "SUM(is_click_3d2nd) as cnt_click_3d2nd, SUM(is_buy_3d2nd) as cnt_buy_3d2nd, "
            + "SUM(is_collect_3d2nd) as cnt_collect_3d2nd, SUM(is_cart_3d2nd) as cnt_cart_3d2nd, "
            + "SUM(is_click_3d3th) as cnt_click_3d3th, SUM(is_buy_3d3th) as cnt_buy_3d3th, "
            + "SUM(is_collect_3d3th) as cnt_collect_3d3th, SUM(is_cart_3d3th) as cnt_cart_3d3th, "
            + "SUM(is_click_1w) as cnt_click_1w, SUM(is_buy_1w) as cnt_buy_1w, "
            + "SUM(is_collect_1w) as cnt_collect_1w, SUM(is_cart_1w) as cnt_cart_1w, "
            + "SUM(is_click_w2nd) as cnt_click_w2nd, SUM(is_buy_w2nd) as cnt_buy_w2nd, "
            + "SUM(is_collect_w2nd) as cnt_collect_w2nd, SUM(is_cart_w2nd) as cnt_cart_w2nd, "
            + "SUM(is_click_w3th) as cnt_click_w3th, SUM(is_buy_w3th) as cnt_buy_w3th, "
            + "SUM(is_collect_w3th) as cnt_collect_w3th, SUM(is_cart_w3th) as cnt_cart_w3th";

        BatchOperator t1_userbrand = t1_preproc.groupBy("user_id, brand_id", clauseUserBrand);

        String clauseUserBrand_Rate = "user_id,brand_id,"
            + "cnt_click,cnt_buy,cnt_collect,cnt_cart,"
            + "cnt_click_1m,cnt_buy_1m,cnt_collect_1m,cnt_cart_1m,"
            + "cnt_click_2m,cnt_buy_2m,cnt_collect_2m,cnt_cart_2m,"
            + "cnt_click_3m,cnt_buy_3m,cnt_collect_3m,cnt_cart_3m,"
            + "cnt_click_m2nd,cnt_buy_m2nd,cnt_collect_m2nd,cnt_cart_m2nd,"
            + "cnt_click_m3th,cnt_buy_m3th,cnt_collect_m3th,cnt_cart_m3th,"
            + "cnt_click_3d,cnt_buy_3d,cnt_collect_3d,cnt_cart_3d,"
            + "cnt_click_3d2nd,cnt_buy_3d2nd,cnt_collect_3d2nd,cnt_cart_3d2nd,"
            + "cnt_click_3d3th,cnt_buy_3d3th,cnt_collect_3d3th,cnt_cart_3d3th,"
            + "cnt_click_1w,cnt_buy_1w,cnt_collect_1w,cnt_cart_1w,"
            + "cnt_click_w2nd,cnt_buy_w2nd,cnt_collect_w2nd,cnt_cart_w2nd,"
            + "cnt_click_w3th,cnt_buy_w3th,cnt_collect_w3th,cnt_cart_w3th,"
            + "case when cnt_buy>cnt_click then 1.0 when cnt_buy=0 then 0.0 else cnt_buy*1.0/cnt_click end AS "
            + "rt_click2buy,"
            + "case when cnt_buy>cnt_collect then 1.0 when cnt_buy=0 then 0.0 else cnt_buy*1.0/cnt_collect end AS "
            + "rt_collect2buy,"
            + "case when cnt_buy>cnt_cart then 1.0 when cnt_buy=0 then 0.0 else cnt_buy*1.0/cnt_cart end AS "
            + "rt_cart2buy,"
            + "case when cnt_buy_3d>cnt_click_3d then 1.0 when cnt_buy_3d=0 then 0.0 else cnt_buy_3d*1.0/cnt_click_3d"
            + " end AS rt_click2buy_3d,"
            + "case when cnt_buy_3d>cnt_collect_3d then 1.0 when cnt_buy_3d=0 then 0.0 else "
            + "cnt_buy_3d*1.0/cnt_collect_3d end AS rt_collect2buy_3d,"
            + "case when cnt_buy_3d>cnt_cart_3d then 1.0 when cnt_buy_3d=0 then 0.0 else cnt_buy_3d*1.0/cnt_cart_3d "
            + "end AS rt_cart2buy_3d,"
            + "case when cnt_buy_1w>cnt_click_1w then 1.0 when cnt_buy_1w=0 then 0.0 else cnt_buy_1w*1.0/cnt_click_1w"
            + " end AS rt_click2buy_1w,"
            + "case when cnt_buy_1w>cnt_collect_1w then 1.0 when cnt_buy_1w=0 then 0.0 else "
            + "cnt_buy_1w*1.0/cnt_collect_1w end AS rt_collect2buy_1w,"
            + "case when cnt_buy_1w>cnt_cart_1w then 1.0 when cnt_buy_1w=0 then 0.0 else cnt_buy_1w*1.0/cnt_cart_1w "
            + "end AS rt_cart2buy_1w,"
            + "case when cnt_buy_1m>cnt_click_1m then 1.0 when cnt_buy_1m=0 then 0.0 else cnt_buy_1m*1.0/cnt_click_1m"
            + " end AS rt_click2buy_1m,"
            + "case when cnt_buy_1m>cnt_collect_1m then 1.0 when cnt_buy_1m=0 then 0.0 else "
            + "cnt_buy_1m*1.0/cnt_collect_1m end AS rt_collect2buy_1m,"
            + "case when cnt_buy_1m>cnt_cart_1m then 1.0 when cnt_buy_1m=0 then 0.0 else cnt_buy_1m*1.0/cnt_cart_1m "
            + "end AS rt_cart2buy_1m,"
            + "case when cnt_click_3d=0 then 0.0 when cnt_click_3d>=120.0*cnt_click_3d2nd then 120.0 else "
            + "cnt_click_3d*1.0/cnt_click_3d2nd end AS rt_click_3d,"
            + "case when cnt_buy_3d=0 then 0.0 when cnt_buy_3d>=10.0*cnt_buy_3d2nd then 10.0 else "
            + "cnt_buy_3d*1.0/cnt_buy_3d2nd end AS rt_buy_3d,"
            + "case when cnt_collect_3d=0 then 0.0 when cnt_collect_3d>=10.0*cnt_collect_3d2nd then 10.0 else "
            + "cnt_collect_3d*1.0/cnt_collect_3d2nd end AS rt_collect_3d,"
            + "case when cnt_cart_3d=0 then 0.0 when cnt_cart_3d>=20.0*cnt_cart_3d2nd then 20.0 else "
            + "cnt_cart_3d*1.0/cnt_cart_3d2nd end AS rt_cart_3d,"
            + "case when cnt_click_1w=0 then 0.0 when cnt_click_1w>=300.0*cnt_click_w2nd then 300.0 else "
            + "cnt_click_1w*1.0/cnt_click_w2nd end AS rt_click_1w,"
            + "case when cnt_buy_1w=0 then 0.0 when cnt_buy_1w>=15.0*cnt_buy_w2nd then 15.0 else "
            + "cnt_buy_1w*1.0/cnt_buy_w2nd end AS rt_buy_1w,"
            + "case when cnt_collect_1w=0 then 0.0 when cnt_collect_1w>=15.0*cnt_collect_w2nd then 15.0 else "
            + "cnt_collect_1w*1.0/cnt_collect_w2nd end AS rt_collect_1w,"
            + "case when cnt_cart_1w=0 then 0.0 when cnt_cart_1w>=40.0*cnt_cart_w2nd then 40.0 else "
            + "cnt_cart_1w*1.0/cnt_cart_w2nd end AS rt_cart_1w,"
            + "case when cnt_click_1m=0 then 0.0 when cnt_click_1m>=500.0*cnt_click_m2nd then 500.0 else "
            + "cnt_click_1m*1.0/cnt_click_m2nd end AS rt_click_1m,"
            + "case when cnt_buy_1m=0 then 0.0 when cnt_buy_1m>=30.0*cnt_buy_m2nd then 30.0 else "
            + "cnt_buy_1m*1.0/cnt_buy_m2nd end AS rt_buy_1m,"
            + "case when cnt_collect_1m=0 then 0.0 when cnt_collect_1m>=30.0*cnt_collect_m2nd then 30.0 else "
            + "cnt_collect_1m*1.0/cnt_collect_m2nd end AS rt_collect_1m,"
            + "case when cnt_cart_1m=0 then 0.0 when cnt_cart_1m>=50.0*cnt_cart_m2nd then 50.0 else "
            + "cnt_cart_1m*1.0/cnt_cart_m2nd end AS rt_cart_1m";

        t1_userbrand = t1_userbrand.select(clauseUserBrand_Rate);

        String clauseUser = "user_id, "
            + "SUM(is_click) as user_cnt_click, SUM(is_buy) as user_cnt_buy, "
            + "SUM(is_collect) as user_cnt_collect, SUM(is_cart) as user_cnt_cart, "
            + "SUM(is_click_1m) as user_cnt_click_1m, SUM(is_buy_1m) as user_cnt_buy_1m, "
            + "SUM(is_collect_1m) as user_cnt_collect_1m, SUM(is_cart_1m) as user_cnt_cart_1m, "
            + "SUM(is_click_2m) as user_cnt_click_2m, SUM(is_buy_2m) as user_cnt_buy_2m, "
            + "SUM(is_collect_2m) as user_cnt_collect_2m, SUM(is_cart_2m) as user_cnt_cart_2m, "
            + "SUM(is_click_3m) as user_cnt_click_3m, SUM(is_buy_3m) as user_cnt_buy_3m, "
            + "SUM(is_collect_3m) as user_cnt_collect_3m, SUM(is_cart_3m) as user_cnt_cart_3m, "
            + "SUM(is_click_m2nd) as user_cnt_click_m2nd, SUM(is_buy_m2nd) as user_cnt_buy_m2nd, "
            + "SUM(is_collect_m2nd) as user_cnt_collect_m2nd, SUM(is_cart_m2nd) as user_cnt_cart_m2nd, "
            + "SUM(is_click_m3th) as user_cnt_click_m3th, SUM(is_buy_m3th) as user_cnt_buy_m3th, "
            + "SUM(is_collect_m3th) as user_cnt_collect_m3th, SUM(is_cart_m3th) as user_cnt_cart_m3th, "
            + "SUM(is_click_3d) as user_cnt_click_3d, SUM(is_buy_3d) as user_cnt_buy_3d, "
            + "SUM(is_collect_3d) as user_cnt_collect_3d, SUM(is_cart_3d) as user_cnt_cart_3d, "
            + "SUM(is_click_3d2nd) as user_cnt_click_3d2nd, SUM(is_buy_3d2nd) as user_cnt_buy_3d2nd, "
            + "SUM(is_collect_3d2nd) as user_cnt_collect_3d2nd, SUM(is_cart_3d2nd) as user_cnt_cart_3d2nd, "
            + "SUM(is_click_3d3th) as user_cnt_click_3d3th, SUM(is_buy_3d3th) as user_cnt_buy_3d3th, "
            + "SUM(is_collect_3d3th) as user_cnt_collect_3d3th, SUM(is_cart_3d3th) as user_cnt_cart_3d3th, "
            + "SUM(is_click_1w) as user_cnt_click_1w, SUM(is_buy_1w) as user_cnt_buy_1w, "
            + "SUM(is_collect_1w) as user_cnt_collect_1w, SUM(is_cart_1w) as user_cnt_cart_1w, "
            + "SUM(is_click_w2nd) as user_cnt_click_w2nd, SUM(is_buy_w2nd) as user_cnt_buy_w2nd, "
            + "SUM(is_collect_w2nd) as user_cnt_collect_w2nd, SUM(is_cart_w2nd) as user_cnt_cart_w2nd, "
            + "SUM(is_click_w3th) as user_cnt_click_w3th, SUM(is_buy_w3th) as user_cnt_buy_w3th, "
            + "SUM(is_collect_w3th) as user_cnt_collect_w3th, SUM(is_cart_w3th) as user_cnt_cart_w3th";

        BatchOperator t1_user = t1_preproc.groupBy("user_id", clauseUser);

        String clauseUser_Rate = "user_id AS user_id4join,"
            + "user_cnt_click,user_cnt_buy,user_cnt_collect,user_cnt_cart,"
            + "user_cnt_click_1m,user_cnt_buy_1m,user_cnt_collect_1m,user_cnt_cart_1m,"
            + "user_cnt_click_2m,user_cnt_buy_2m,user_cnt_collect_2m,user_cnt_cart_2m,"
            + "user_cnt_click_3m,user_cnt_buy_3m,user_cnt_collect_3m,user_cnt_cart_3m,"
            + "user_cnt_click_m2nd,user_cnt_buy_m2nd,user_cnt_collect_m2nd,user_cnt_cart_m2nd,"
            + "user_cnt_click_m3th,user_cnt_buy_m3th,user_cnt_collect_m3th,user_cnt_cart_m3th,"
            + "user_cnt_click_3d,user_cnt_buy_3d,user_cnt_collect_3d,user_cnt_cart_3d,"
            + "user_cnt_click_3d2nd,user_cnt_buy_3d2nd,user_cnt_collect_3d2nd,user_cnt_cart_3d2nd,"
            + "user_cnt_click_3d3th,user_cnt_buy_3d3th,user_cnt_collect_3d3th,user_cnt_cart_3d3th,"
            + "user_cnt_click_1w,user_cnt_buy_1w,user_cnt_collect_1w,user_cnt_cart_1w,"
            + "user_cnt_click_w2nd,user_cnt_buy_w2nd,user_cnt_collect_w2nd,user_cnt_cart_w2nd,"
            + "user_cnt_click_w3th,user_cnt_buy_w3th,user_cnt_collect_w3th,user_cnt_cart_w3th,"
            + "case when user_cnt_buy>user_cnt_click then 1.0 when user_cnt_buy=0 then 0.0 else "
            + "user_cnt_buy*1.0/user_cnt_click end AS user_rt_click2buy,"
            + "case when user_cnt_buy>user_cnt_collect then 1.0 when user_cnt_buy=0 then 0.0 else "
            + "user_cnt_buy*1.0/user_cnt_collect end AS user_rt_collect2buy,"
            + "case when user_cnt_buy>user_cnt_cart then 1.0 when user_cnt_buy=0 then 0.0 else "
            + "user_cnt_buy*1.0/user_cnt_cart end AS user_rt_cart2buy,"
            + "case when user_cnt_buy_3d>user_cnt_click_3d then 1.0 when user_cnt_buy_3d=0 then 0.0 else "
            + "user_cnt_buy_3d*1.0/user_cnt_click_3d end AS user_rt_click2buy_3d,"
            + "case when user_cnt_buy_3d>user_cnt_collect_3d then 1.0 when user_cnt_buy_3d=0 then 0.0 else "
            + "user_cnt_buy_3d*1.0/user_cnt_collect_3d end AS user_rt_collect2buy_3d,"
            + "case when user_cnt_buy_3d>user_cnt_cart_3d then 1.0 when user_cnt_buy_3d=0 then 0.0 else "
            + "user_cnt_buy_3d*1.0/user_cnt_cart_3d end AS user_rt_cart2buy_3d,"
            + "case when user_cnt_buy_1w>user_cnt_click_1w then 1.0 when user_cnt_buy_1w=0 then 0.0 else "
            + "user_cnt_buy_1w*1.0/user_cnt_click_1w end AS user_rt_click2buy_1w,"
            + "case when user_cnt_buy_1w>user_cnt_collect_1w then 1.0 when user_cnt_buy_1w=0 then 0.0 else "
            + "user_cnt_buy_1w*1.0/user_cnt_collect_1w end AS user_rt_collect2buy_1w,"
            + "case when user_cnt_buy_1w>user_cnt_cart_1w then 1.0 when user_cnt_buy_1w=0 then 0.0 else "
            + "user_cnt_buy_1w*1.0/user_cnt_cart_1w end AS user_rt_cart2buy_1w,"
            + "case when user_cnt_buy_1m>user_cnt_click_1m then 1.0 when user_cnt_buy_1m=0 then 0.0 else "
            + "user_cnt_buy_1m*1.0/user_cnt_click_1m end AS user_rt_click2buy_1m,"
            + "case when user_cnt_buy_1m>user_cnt_collect_1m then 1.0 when user_cnt_buy_1m=0 then 0.0 else "
            + "user_cnt_buy_1m*1.0/user_cnt_collect_1m end AS user_rt_collect2buy_1m,"
            + "case when user_cnt_buy_1m>user_cnt_cart_1m then 1.0 when user_cnt_buy_1m=0 then 0.0 else "
            + "user_cnt_buy_1m*1.0/user_cnt_cart_1m end AS user_rt_cart2buy_1m";

        t1_user = t1_user.select(clauseUser_Rate);

        String clauseBrand = "brand_id, "
            + "SUM(is_click) as brand_cnt_click, SUM(is_buy) as brand_cnt_buy, "
            + "SUM(is_collect) as brand_cnt_collect, SUM(is_cart) as brand_cnt_cart, "
            + "SUM(is_click_1m) as brand_cnt_click_1m, SUM(is_buy_1m) as brand_cnt_buy_1m, "
            + "SUM(is_collect_1m) as brand_cnt_collect_1m, SUM(is_cart_1m) as brand_cnt_cart_1m, "
            + "SUM(is_click_2m) as brand_cnt_click_2m, SUM(is_buy_2m) as brand_cnt_buy_2m, "
            + "SUM(is_collect_2m) as brand_cnt_collect_2m, SUM(is_cart_2m) as brand_cnt_cart_2m, "
            + "SUM(is_click_3m) as brand_cnt_click_3m, SUM(is_buy_3m) as brand_cnt_buy_3m, "
            + "SUM(is_collect_3m) as brand_cnt_collect_3m, SUM(is_cart_3m) as brand_cnt_cart_3m, "
            + "SUM(is_click_m2nd) as brand_cnt_click_m2nd, SUM(is_buy_m2nd) as brand_cnt_buy_m2nd, "
            + "SUM(is_collect_m2nd) as brand_cnt_collect_m2nd, SUM(is_cart_m2nd) as brand_cnt_cart_m2nd, "
            + "SUM(is_click_m3th) as brand_cnt_click_m3th, SUM(is_buy_m3th) as brand_cnt_buy_m3th, "
            + "SUM(is_collect_m3th) as brand_cnt_collect_m3th, SUM(is_cart_m3th) as brand_cnt_cart_m3th, "
            + "SUM(is_click_3d) as brand_cnt_click_3d, SUM(is_buy_3d) as brand_cnt_buy_3d, "
            + "SUM(is_collect_3d) as brand_cnt_collect_3d, SUM(is_cart_3d) as brand_cnt_cart_3d, "
            + "SUM(is_click_3d2nd) as brand_cnt_click_3d2nd, SUM(is_buy_3d2nd) as brand_cnt_buy_3d2nd, "
            + "SUM(is_collect_3d2nd) as brand_cnt_collect_3d2nd, SUM(is_cart_3d2nd) as brand_cnt_cart_3d2nd, "
            + "SUM(is_click_3d3th) as brand_cnt_click_3d3th, SUM(is_buy_3d3th) as brand_cnt_buy_3d3th, "
            + "SUM(is_collect_3d3th) as brand_cnt_collect_3d3th, SUM(is_cart_3d3th) as brand_cnt_cart_3d3th, "
            + "SUM(is_click_1w) as brand_cnt_click_1w, SUM(is_buy_1w) as brand_cnt_buy_1w, "
            + "SUM(is_collect_1w) as brand_cnt_collect_1w, SUM(is_cart_1w) as brand_cnt_cart_1w, "
            + "SUM(is_click_w2nd) as brand_cnt_click_w2nd, SUM(is_buy_w2nd) as brand_cnt_buy_w2nd, "
            + "SUM(is_collect_w2nd) as brand_cnt_collect_w2nd, SUM(is_cart_w2nd) as brand_cnt_cart_w2nd, "
            + "SUM(is_click_w3th) as brand_cnt_click_w3th, SUM(is_buy_w3th) as brand_cnt_buy_w3th, "
            + "SUM(is_collect_w3th) as brand_cnt_collect_w3th, SUM(is_cart_w3th) as brand_cnt_cart_w3th";

        BatchOperator t1_brand = t1_preproc.groupBy("brand_id", clauseBrand);

        String clauseBrand_Rate = "brand_id AS brand_id4join,"
            + "brand_cnt_click,brand_cnt_buy,brand_cnt_collect,brand_cnt_cart,"
            + "brand_cnt_click_1m,brand_cnt_buy_1m,brand_cnt_collect_1m,brand_cnt_cart_1m,"
            + "brand_cnt_click_2m,brand_cnt_buy_2m,brand_cnt_collect_2m,brand_cnt_cart_2m,"
            + "brand_cnt_click_3m,brand_cnt_buy_3m,brand_cnt_collect_3m,brand_cnt_cart_3m,"
            + "brand_cnt_click_m2nd,brand_cnt_buy_m2nd,brand_cnt_collect_m2nd,brand_cnt_cart_m2nd,"
            + "brand_cnt_click_m3th,brand_cnt_buy_m3th,brand_cnt_collect_m3th,brand_cnt_cart_m3th,"
            + "brand_cnt_click_3d,brand_cnt_buy_3d,brand_cnt_collect_3d,brand_cnt_cart_3d,"
            + "brand_cnt_click_3d2nd,brand_cnt_buy_3d2nd,brand_cnt_collect_3d2nd,brand_cnt_cart_3d2nd,"
            + "brand_cnt_click_3d3th,brand_cnt_buy_3d3th,brand_cnt_collect_3d3th,brand_cnt_cart_3d3th,"
            + "brand_cnt_click_1w,brand_cnt_buy_1w,brand_cnt_collect_1w,brand_cnt_cart_1w,"
            + "brand_cnt_click_w2nd,brand_cnt_buy_w2nd,brand_cnt_collect_w2nd,brand_cnt_cart_w2nd,"
            + "brand_cnt_click_w3th,brand_cnt_buy_w3th,brand_cnt_collect_w3th,brand_cnt_cart_w3th,"
            + "case when brand_cnt_buy>brand_cnt_click then 1.0 when brand_cnt_buy=0 then 0.0 else "
            + "brand_cnt_buy*1.0/brand_cnt_click end AS brand_rt_click2buy,"
            + "case when brand_cnt_buy>brand_cnt_collect then 1.0 when brand_cnt_buy=0 then 0.0 else "
            + "brand_cnt_buy*1.0/brand_cnt_collect end AS brand_rt_collect2buy,"
            + "case when brand_cnt_buy>brand_cnt_cart then 1.0 when brand_cnt_buy=0 then 0.0 else "
            + "brand_cnt_buy*1.0/brand_cnt_cart end AS brand_rt_cart2buy,"
            + "case when brand_cnt_buy_3d>brand_cnt_click_3d then 1.0 when brand_cnt_buy_3d=0 then 0.0 else "
            + "brand_cnt_buy_3d*1.0/brand_cnt_click_3d end AS brand_rt_click2buy_3d,"
            + "case when brand_cnt_buy_3d>brand_cnt_collect_3d then 1.0 when brand_cnt_buy_3d=0 then 0.0 else "
            + "brand_cnt_buy_3d*1.0/brand_cnt_collect_3d end AS brand_rt_collect2buy_3d,"
            + "case when brand_cnt_buy_3d>brand_cnt_cart_3d then 1.0 when brand_cnt_buy_3d=0 then 0.0 else "
            + "brand_cnt_buy_3d*1.0/brand_cnt_cart_3d end AS brand_rt_cart2buy_3d,"
            + "case when brand_cnt_buy_1w>brand_cnt_click_1w then 1.0 when brand_cnt_buy_1w=0 then 0.0 else "
            + "brand_cnt_buy_1w*1.0/brand_cnt_click_1w end AS brand_rt_click2buy_1w,"
            + "case when brand_cnt_buy_1w>brand_cnt_collect_1w then 1.0 when brand_cnt_buy_1w=0 then 0.0 else "
            + "brand_cnt_buy_1w*1.0/brand_cnt_collect_1w end AS brand_rt_collect2buy_1w,"
            + "case when brand_cnt_buy_1w>brand_cnt_cart_1w then 1.0 when brand_cnt_buy_1w=0 then 0.0 else "
            + "brand_cnt_buy_1w*1.0/brand_cnt_cart_1w end AS brand_cart2buy_1w,"
            + "case when brand_cnt_buy_1m>brand_cnt_click_1m then 1.0 when brand_cnt_buy_1m=0 then 0.0 else "
            + "brand_cnt_buy_1m*1.0/brand_cnt_click_1m end AS brand_rt_click2buy_1m,"
            + "case when brand_cnt_buy_1m>brand_cnt_collect_1m then 1.0 when brand_cnt_buy_1m=0 then 0.0 else "
            + "brand_cnt_buy_1m*1.0/brand_cnt_collect_1m end AS brand_rt_collect2buy_1m,"
            + "case when brand_cnt_buy_1m>brand_cnt_cart_1m then 1.0 when brand_cnt_buy_1m=0 then 0.0 else "
            + "brand_cnt_buy_1m*1.0/brand_cnt_cart_1m end AS brand_rt_cart2buy_1m";

        t1_brand = t1_brand.select(clauseBrand_Rate);

        BatchOperator t1_join = new JoinBatchOp()
            .setSelectClause("*")
            .setJoinPredicate("user_id=user_id4join")
            .linkFrom(t1_userbrand, t1_user);

        t1_join = new JoinBatchOp()
            .setSelectClause("*")
            .setJoinPredicate("brand_id=brand_id4join")
            .linkFrom(t1_join, t1_brand);

        BatchOperator t2_label = t2
            .filter("type=1")
            .select("user_id AS user_id4label, brand_id AS brand_id4label, 1 as label")
            .distinct();

        BatchOperator feature_label = new LeftOuterJoinBatchOp()
            .setSelectClause("*")
            .setJoinPredicate("user_id = user_id4label AND brand_id = brand_id4label")
            .linkFrom(t1_join, t2_label);

        Imputer imputer = new Imputer()
            .setStrategy("value")
            .setFillValue("0")
            .setSelectedCols("label");

        feature_label = imputer.fit(feature_label).transform(feature_label);

        System.out.println(feature_label.getSchema());

        String[] featureColNames =
            ArrayUtils.removeElements(
                feature_label.getColNames(),
                new String[]{
                    "user_id", "brand_id",
                    "user_id4join", "brand_id4join",
                    "user_id4label", "brand_id4label",
                    LABEL_COL_NAME
                }
            );

        StringBuilder sbd = new StringBuilder();
        for (String name : featureColNames) {
            sbd.append("CAST(").append(name).append(" AS DOUBLE) AS ").append(name).append(", ");
        }
        sbd.append(LABEL_COL_NAME);

        feature_label
            .select(sbd.toString())
            .link(
                new AkSinkBatchOp()
                    .setFilePath(DATA_DIR + FEATURE_LABEL_FILE)
                    .setOverwriteSink(true)
            );

        BatchOperator.execute();
    }

    static void c_4() throws Exception {

        AkSourceBatchOp all_data = new AkSourceBatchOp().setFilePath(DATA_DIR + FEATURE_LABEL_FILE);

        all_data
            .lazyPrintStatistics()
            .groupBy("label", "label, COUNT(*) AS cnt")
            .print();

        Utils.splitTrainTestIfNotExist(all_data, DATA_DIR + TRAIN_FILE, DATA_DIR + TEST_FILE, 0.8);

        AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
        AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

        String[] featureColNames = ArrayUtils.removeElement(train_data.getColNames(), LABEL_COL_NAME);

        new LogisticRegression()
            .setFeatureCols(featureColNames)
            .setLabelCol(LABEL_COL_NAME)
            .setPredictionCol(PREDICTION_COL_NAME)
            .setPredictionDetailCol(PRED_DETAIL_COL_NAME)
            .fit(train_data)
            .transform(test_data)
            .link(
                new EvalBinaryClassBatchOp()
                    .setLabelCol(LABEL_COL_NAME)
                    .setPredictionDetailCol(PRED_DETAIL_COL_NAME)
                    .lazyPrintMetrics("LogisticRegression")
            );
        BatchOperator.execute();

        if (!new File(DATA_DIR + TRAIN_SAMPLE_FILE).exists()) {
            train_data
                .link(
                    new StratifiedSampleBatchOp()
                        .setStrataRatios("0:0.05,1:1.0")
                        .setStrataCol(LABEL_COL_NAME)
                )
                .link(
                    new AkSinkBatchOp()
                        .setFilePath(DATA_DIR + TRAIN_SAMPLE_FILE)
                );
            BatchOperator.execute();
        }

        AkSourceBatchOp train_sample =
            new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_SAMPLE_FILE);

        new LogisticRegression()
            .setFeatureCols(featureColNames)
            .setLabelCol(LABEL_COL_NAME)
            .setPredictionCol(PREDICTION_COL_NAME)
            .setPredictionDetailCol(PRED_DETAIL_COL_NAME)
            .fit(train_sample)
            .transform(test_data)
            .link(
                new EvalBinaryClassBatchOp()
                    .setLabelCol(LABEL_COL_NAME)
                    .setPredictionDetailCol(PRED_DETAIL_COL_NAME)
                    .lazyPrintMetrics("LogisticRegression with Stratified Sample")
            );
        BatchOperator.execute();

    }

    static void c_5() throws Exception {
        AkSourceBatchOp train_sample = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_SAMPLE_FILE);
        AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

        String[] featureColNames = ArrayUtils.removeElement(test_data.getColNames(), LABEL_COL_NAME);

        for (TreeType treeType : new TreeType[]{TreeType.GINI,
            TreeType.INFOGAIN, TreeType.INFOGAINRATIO}) {

            new DecisionTreeClassifier()
                .setTreeType(treeType)
                .setFeatureCols(featureColNames)
                .setLabelCol(LABEL_COL_NAME)
                .setPredictionCol(PREDICTION_COL_NAME)
                .setPredictionDetailCol(PRED_DETAIL_COL_NAME)
                .fit(train_sample)
                .transform(test_data)
                .link(
                    new EvalBinaryClassBatchOp()
                        .setPositiveLabelValueString("1")
                        .setLabelCol(LABEL_COL_NAME)
                        .setPredictionDetailCol(PRED_DETAIL_COL_NAME)
                        .lazyPrintMetrics(treeType.toString())
                );
        }

        BatchOperator.execute();
    }

    static void c_7() throws Exception {
        AkSourceBatchOp train_sample = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_SAMPLE_FILE);
        AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

        String[] featureColNames = ArrayUtils.removeElement(test_data.getColNames(), LABEL_COL_NAME);

        new RandomForestClassifier()
            .setNumTrees(20)
            .setMaxDepth(4)
            .setMaxBins(512)
            .setFeatureCols(featureColNames)
            .setLabelCol(LABEL_COL_NAME)
            .setPredictionCol(PREDICTION_COL_NAME)
            .setPredictionDetailCol(PRED_DETAIL_COL_NAME)
            .fit(train_sample)
            .transform(test_data)
            .link(
                new EvalBinaryClassBatchOp()
                    .setLabelCol(LABEL_COL_NAME)
                    .setPredictionDetailCol(PRED_DETAIL_COL_NAME)
                    .lazyPrintMetrics("RandomForest with Stratified Sample")
            );

        BatchOperator.execute();
    }

    static void c_8() throws Exception {
        AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);
        AkSourceBatchOp train_sample = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_SAMPLE_FILE);

        String[] featureColNames = ArrayUtils.removeElement(test_data.getColNames(), LABEL_COL_NAME);

        new GbdtClassifier()
            .setNumTrees(100)
            .setMaxDepth(5)
            .setMaxBins(256)
            .setFeatureCols(featureColNames)
            .setLabelCol(LABEL_COL_NAME)
            .setPredictionCol(PREDICTION_COL_NAME)
            .setPredictionDetailCol(PRED_DETAIL_COL_NAME)
            .fit(train_sample)
            .transform(test_data)
            .link(
                new EvalBinaryClassBatchOp()
                    .setLabelCol(LABEL_COL_NAME)
                    .setPredictionDetailCol(PRED_DETAIL_COL_NAME)
                    .lazyPrintMetrics("GBDT with Stratified Sample")
            );

        BatchOperator.execute();
    }

}
