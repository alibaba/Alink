## Description
Latent Dirichlet Allocation (LDA), a topic model designed for text documents.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
### Code
```python
data = np.array(["a b b c c c c c c e e f f f g h k k k", \
"a b b b d e e e h h k", \
"a b b b b c f f f f g g g g g g g g g i j j", \
"a a b d d d g g g g g i i j j j k k k k k k k k k", \
"a a a b c d d d d d d d d d e e e g g j k k k", \
"a a a a b b d d d e e e e f f f f f g h i j j j j", \
"a a b d d d g g g g g i i j j k k k k k k k k k", \
"a b c d d d d d d d d d e e f g g j k k k", \
"a a a a b b b b d d d e e e e f f g h h h", \
"a a b b b b b b b b c c e e e g g i i j j j j j j j k k", \
"a b c d d d d d d d d d f f g g j j j k k k", \
"a a a a b e e e e f f f f f g h h h j"])

df = pd.DataFrame({"doc" : data})
inOp = dataframeToOperator(df, schemaStr="doc string",op_type="batch")



ldaTrain = LdaTrainBatchOp()\
            .setSelectedCol("doc")\
            .setTopicNum(6)\
            .setMethod("online")\
            .setSubsamplingRate(1.0)\
            .setOptimizeDocConcentration(True)\
            .setNumIter(20)

ldaPredict = LdaPredictStreamOp().setPredictionCol("pred").setSelectedCol("doc")

model = ldaTrain.linkFrom(inOp)

ldaPredict.linkFrom(model, inOp).print()

StreamOperator.execute()
```

## Results
### Model

model_id|model_info
--------|----------
0|{"logPerplexity":"3.7090449161397796","betaArray":"[0.16666666666666666,0.16666666666666666,0.16666666666666666,0.16666666666666666,0.16666666666666666,0.16666666666666666]","logLikelihood":"-964.3516781963427","method":"\"online\"","alphaArray":"[0.13821318741806757,0.14883947846014303,0.11751772860080838,0.11649338902896737,0.1503735753641805,0.12383960905322638]","topicNum":"6","vocabularySize":"11"}
1048576|{"m":6,"n":11,"data":[6125.275647735944,5541.830400832857,5277.404107556518,5575.307666756267,5738.822977932333,5664.141524765102,5183.8663148472615,6286.886714218059,5159.4834022615505,5965.45851687814,5785.616901302167,5558.164928383525,5290.881194601821,5849.766053667748,5595.238710003511,5709.172846472106,5367.427910628795,6967.997740551021,5688.8764262580735,4955.8174077887725,4940.593716098454,5435.785995518678,6359.043301395186,4992.933732368455,5164.467086144761,6624.6072909374125,6911.005911971013,6239.327690548231,5908.580210537792,6090.679944041717,4491.439930702308,5785.921888708801,4648.954813378507,5714.129075228494,6200.167117921488,5223.186458407328,5560.911614536643,5141.113565996373,6043.809469077941,7092.299303765094,6408.739229185271,5851.449695701356,4518.178684615466,5946.483529384942,5633.526524470202,5538.4345859137275,5983.901197676244,5587.210556929512,6050.024468817716,4965.114090486532,4634.277477990217,5692.989466800378,5462.485467579785,4841.301836486494,5117.962076960599,4980.381226902301,5186.706443620538,6608.121037167229,5926.302505211329,6106.240714316094,5474.117007346719,4977.005342253029,5871.2842682743185,4842.798396244806,4810.0086663355705,5468.469136036559]}
2097152|{"f0":"d","f1":0.36772478012531734,"f2":0}
3145728|{"f0":"k","f1":0.36772478012531734,"f2":1}
4194304|{"f0":"f","f1":0.4855078157817008,"f2":7}
5242880|{"f0":"c","f1":0.6190392084062235,"f2":8}
6291456|{"f0":"h","f1":0.7731898882334817,"f2":9}
7340032|{"f0":"i","f1":0.7731898882334817,"f2":10}
8388608|{"f0":"g","f1":0.08004270767353636,"f2":2}
9437184|{"f0":"b","f1":0.0,"f2":3}
10485760|{"f0":"a","f1":0.0,"f2":4}
11534336|{"f0":"e","f1":0.36772478012531734,"f2":5}
12582912|{"f0":"j","f1":0.26236426446749106,"f2":6}


### Prediction

| id | libsvm | pred |
|----|--------|------|
| 0 | a b b c c c c c c e e f f f g h k k k| 0 |
| 1 | a b b b d e e e h h k| 4 |
| 2 | a b b b b c f f f f g g g g g g g g g i j j| 5 |
| 3 | a a b d d d g g g g g i i j j j k k k k k k k k k| 1 |
| 4 | a a a b c d d d d d d d d d e e e g g j k k k| 1 |
| 5 | a a a a b b d d d e e e e f f f f f g h i j j j j| 2 |
| 6 | a a b d d d g g g g g i i j j k k k k k k k k k| 1 |
| 7 | a b c d d d d d d d d d e e f g g j k k k| 0 |
| 8 | a a a a b b b b d d d e e e e f f g h h h| 4 |
| 9 | a a b b b b b b b b c c e e e g g i i j j j j j j j k k| 4 |
| 10 | a b c d d d d d d d d d f f g g j j j k k k| 0 |
| 11 | a a a a b e e e e f f f f f g h h h j| 1 |
