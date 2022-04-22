# Bert文本分类器 (BertTextClassifier)
Java 类名：com.alibaba.alink.pipeline.classification.BertTextClassifier

Python 类名：BertTextClassifier


## 功能介绍

Bert 文本分类器。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| textCol | 文本列 | 文本列 | String | ✓ |  |  |
| batchSize | 数据批大小 | 数据批大小 | Integer |  |  | 32 |
| bertModelName | BERT模型名字 | BERT模型名字： Base-Chinese,Base-Multilingual-Cased,Base-Uncased,Base-Cased | String |  |  | "Base-Chinese" |
| checkpointFilePath | 保存 checkpoint 的路径 | 用于保存中间结果的路径，将作为 TensorFlow 中 `Estimator` 的 `model_dir` 传入，需要为所有 worker 都能访问到的目录 | String |  |  | null |
| customConfigJson | 自定义参数 | 对应 https://github.com/alibaba/EasyTransfer/blob/master/easytransfer/app_zoo/app_config.py 中的config_json | String |  |  |  |
| inferBatchSize | 推理数据批大小 | 推理数据批大小 | Integer |  |  | 256 |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  |  | 4 |
| learningRate | 学习率 | 学习率 | Double |  |  | 0.001 |
| maxSeqLength | 句子截断长度 | 句子截断长度 | Integer |  |  | 128 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| numEpochs | epoch 数 | epoch 数 | Double |  |  | 0.01 |
| numFineTunedLayers | 微调层数 | 微调层数 | Integer |  |  | 1 |
| numPSs | PS 角色数 | PS 角色的数量。值未设置时，如果 Worker 角色数也未设置，则为作业总并发度的 1/4（需要取整），否则为总并发度减去 Worker 角色数。 | Integer |  |  | null |
| numWorkers | Worker 角色数 | Worker 角色的数量。值未设置时，如果 PS 角色数也未设置，则为作业总并发度的 3/4（需要取整），否则为总并发度减去 PS 角色数。 | Integer |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| pythonEnv | Python 环境路径 | Python 环境路径，一般情况下不需要填写。如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；如果是目录，那么只能使用本地路径，即 file://。 | String |  |  | "" |
| removeCheckpointBeforeTraining | 是否在训练前移除 checkpoint 相关文件 | 是否在训练前移除 checkpoint 相关文件用于重新训练，只会删除必要的文件 | Boolean |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
url = "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv"
schemaStr = "label bigint, review string"
data = CsvSourceBatchOp() \
    .setFilePath(url) \
    .setSchemaStr(schemaStr) \
    .setIgnoreFirstLine(True)
data = data.where("review is not null")
data = ShuffleBatchOp().linkFrom(data)

classifier = BertTextClassifier() \
    .setTextCol("review") \
    .setLabelCol("label") \
    .setNumEpochs(0.01) \
    .setNumFineTunedLayers(1) \
    .setMaxSeqLength(128) \
    .setBertModelName("Base-Chinese") \
    .setPredictionCol("pred") \
    .setPredictionDetailCol("pred_detail")
model = classifier.fit(data)
predict = model.transform(data.firstN(300))
predict.print()
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ShuffleBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.classification.BertClassificationModel;
import com.alibaba.alink.pipeline.classification.BertTextClassifier;
import org.junit.Test;

public class BertTextClassifierTest {
	@Test
	public void test() throws Exception {
		String url = "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv";
		String schemaStr = "label bigint, review string";
		BatchOperator <?> data = new CsvSourceBatchOp()
			.setFilePath(url)
			.setSchemaStr(schemaStr)
			.setIgnoreFirstLine(true);
		data = data.where("review is not null");
		data = new ShuffleBatchOp().linkFrom(data);

		BertTextClassifier classifier = new BertTextClassifier()
			.setTextCol("review")
			.setLabelCol("label")
			.setNumEpochs(0.01)
			.setNumFineTunedLayers(1)
			.setMaxSeqLength(128)
			.setBertModelName("Base-Chinese")
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");
		BertClassificationModel model = classifier.fit(data);
		BatchOperator <?> predict = model.transform(data.firstN(300));
		predict.print();
	}
}
```

### 运行结果

label|review|pred|pred_detail
-----|------|----|-----------
1|知道网线接口在哪儿吗？比高家庄的地道口还隐蔽。在床头柜后面！想不道吧？看你怎么用。1：自带4米以上网线；2：自带小板凳：3：房间的垃圾桶可翻过来坐......|1|{"0":0.07187604904174805,"1":0.928123950958252}
1|1.酒店承诺接机，飞机本来应该在晚上10：00到，但是因为稍晚再加取行李，10：30分才出来，酒店的服务生已经在出口举牌等了，虽然只有我一个人，但仍很热心，态度很好的送我回了酒店。很感谢！（酒店在市中心，机场离市区蛮远，酒店的接机真的很方便。）2.酒店楼很高（比想象的），价格合理，很干净不知道官方是怎么评级的，个人觉得是不错的3星补充点评2008年6月23日：:-)已经1个多月了，想起来还是觉得酒店不错看来以后如果我机会再去HHHT，肯定还会住这里的|1|{"0":0.03521829843521118,"1":0.9647817015647888}
1|超赞！虽然窗正对中环，但一点也不觉得吵。房间很干净、整齐，给人很舒服的感觉，服务也很好，价格也不贵，非常满意！|1|{"0":0.162497878074646,"1":0.837502121925354}
1|吸取网友们的经验，我们预定的是度假区高层海景大床房，于6月21-23日入住。很高兴亚太的服务还不错，完全按照我们的要求安排了房间。服务员在走廊里遇到客人也会问好；晚上在酒店吃烧烤，服务也不错。总的来说，酒店的环境和海滨浴场都不错，泳池造型很美，水也清澈，晚上水池的灯光也很漂亮，可能是淡季的原因，海滨浴场人很少，特别安静，适合度假。美中不足是房间的设施比较陈旧，电视太小，风扇和空调都有噪音，电话机也很老旧，拖鞋看起来不太干净，好在我们自带了拖鞋。再有就是早餐要提出批评，虽然种类很多，但味道实在不敢恭维，作为一个老五星酒店，房间设施陈旧可以理解，更新起来成本过高，但早餐质量应该尽快改进，这样才能与星级相配。另外，北京的五星酒店都会在入住的第一天送水果，甚至蜈支洲岛上的观海木楼都送了水果，可惜亚太却没有，这也是对客人表示欢迎的一种服务吧，建议亚太考虑一下。总的来说，酒店还是很舒服，给我们留下了比较好的印象。|1|{"0":0.04930460453033447,"1":0.9506953954696655}
1|携程这次不错，订的行政房给我免费升级到了8号楼的山景房。对于酒店，总结一下：超5星的环境4星的房间3星的设施和服务酒店分栋依山而建，酒店里有专门的散步路线图，要把整个酒店的山景从容走完至少1小时，期间山水桥路，鸟叫虫鸣，空气清新，心旷神怡！8号楼是酒店最好的一栋楼了（价格最贵），进入房间，房间宽大，落地窗外就是山景翠色。但其大床一看就知不会很舒服，床很低，床垫质量不好，床上用品虽然干净但明显成色已经旧了灰扑扑的颜色，摸上去硬邦邦的，家具电器以及卫生间用品都很一般。68一位早餐品种还是比较丰富，吃饱没问题，精致美味就谈不上了。各处的服务员态度还是很好但服务意识和服务质量应提高，酒店有宽阔的停车场，不过无论住店客人均收费。宾馆反馈2008年7月10日：您好!感谢您对红珠山宾馆提出的宝贵意见,我宾馆现已经将布草全部更换,您的意见我们已经交给相关部门进行处理,我们诚恳地期待您的下次光临!|1|{"0":0.0854041576385498,"1":0.9145958423614502}
...|...|...  |...
0|我这次是第5次住在长春的雁鸣湖大酒店。昨晚夜里停电。深夜我睡着了。我的钱包被内贼进入我的房间，偷了我近1000元和4张信用卡。。。我的证件和外币，数码相机等都在房间的保险箱里，原封不动。我打了好几个小时的长途电话来处理我的信用卡的冻结。我报案了，这个4星酒店的保安摄像探头竟然坏了，没有修理！保安还查房卡入门时间，就是没有其他人在深夜进入我的房间。难道内贼不会用其他高明的方式进入吗？我的羽绒服也被这个内贼放在地上！我醒来时没有多想！近中午时我才发觉钱包少了现金和信用卡！还有，这家酒店的态度很差！没有同情心！我之前授权的2000元，我打了国际电话，银行说两天前我入酒店的2000元授权了，可是酒店的财务不领情，说中国银行没有授权。我又打了国际电话，我的银行说通过了！这家4星级的酒店不负责，认为不可能发生，我报案了，我下次再也不住这个1星不到的服务态度，很可耻！我还要把这个事件说给那些想定这个酒店的住客。酒店为何停电，摄像头坏得也太凑巧了来让大家知道这种内贼行为是要强力打击的。好了，不说了！！！千元丢了小事。酒店的处理态度我很反感！我强力告诉大家和提醒其他人不要到该酒店！|1|{"0":0.025804638862609863,"1":0.9741953611373901}
1|该酒店对去溧阳公务或旅游的人都很适合，自助早餐很丰富，酒店内部环境和服务很好。唯一的不足是酒店大门口在晚上时太乱，各种车辆和人在门口挤成一团。补充点评2008年5月9日：房间淋浴水压不稳，一会热、一会冷，很不好调整。宾馆反馈2008年5月13日：非常感谢您选择入住金陵溧阳宾馆。您给予我们的肯定与赞赏让我们倍受鼓舞，也使我们更加自信地去做好每一天的服务工作。正是有许多像您一样的宾客给予我们不断的鼓励和赞赏，酒店的服务品质才能得以不断提升。对于酒店大门口的秩序和房间淋浴水的问题我们已做出了相应的措施。再次向您表示我们最衷心的感谢！我们期待您的再次光临！|1|{"0":0.09373688697814941,"1":0.9062631130218506}
1|房间设备太破,连喷头都是不好用,空调几乎感觉不到,虽然我开了最大另外就是设备维修不及时,洗澡用品感觉都是廉价货,味道很奇怪的洗头液等等...总体感觉服务还可以,设备招待所水平...|1|{"0":0.15379667282104492,"1":0.8462033271789551}
1|在桐乡是不错的选择，大堂很大。打车方便。|1|{"0":0.14478212594985962,"1":0.8552178740501404}
1|商务大床房，房间很大，床有2M宽，整体感觉经济实惠不错!|1|{"0":0.0821835994720459,"1":0.9178164005279541}
