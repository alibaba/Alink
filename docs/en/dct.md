## Description
Discrete Cosine Transform(DCT) transforms a real-valued sequence in the time domain into another real-valued sequence
 with same length in the frequency domain.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| inverse | If true, perform inverse transformation(e.g. inverse DCT/inverse FFT). Otherwise perform (forward) transformation. Default: false  | Boolean |  | false |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCol | Name of the output column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |

## Script Example
#### Code
```python
data = np.array([
    ["-0.6264538 0.1836433"],
    ["-0.8356286 1.5952808"],
    ["0.3295078 -0.8204684"],
    ["0.4874291 0.7383247"],
    ["0.5757814 -0.3053884"],
    ["1.5117812 0.3898432"],
    ["-0.6212406 -2.2146999"],
    ["11.1249309 9.9550664"],
    ["9.9838097 10.9438362"],
    ["10.8212212 10.5939013"],
    ["10.9189774 10.7821363"],
    ["10.0745650 8.0106483"],
    ["10.6198257 9.9438713"],
    ["9.8442045 8.5292476"],
    ["9.5218499 10.4179416"],
])

df_data = pd.DataFrame({
    "features": data[:, 0],
})

data = dataframeToOperator(df_data, schemaStr='features string', op_type="batch")

dct = DCT() \
    .setSelectedCol("features") \
    .setOutputCol("result")

dct.transform(data).print()
```

#### Results
```
                 features                                    result
0    -0.6264538 0.1836433  -0.31311430733060563 -0.5728251528295567
1    -0.8356286 1.5952808    0.5371552219632794 -1.7189125211901217
2    0.3295078 -0.8204684   -0.34716156955541605 0.8131559692231375
3     0.4874291 0.7383247    0.866738824045179 -0.17740998012986753
4    0.5757814 -0.3053884    0.19119672388537412 0.6230811409567939
5     1.5117812 0.3898432     1.3446515085097996 0.7933299678708727
6   -0.6212406 -2.2146999      -2.005312758591568 1.126745876574769
7    11.1249309 9.9550664     14.905809038224113 0.8272191210194105
8    9.9838097 10.9438362    14.798080330160849 -0.6788412482687869
9   10.8212212 10.5939013     15.142778339690611 0.1607394427886475
10  10.9189774 10.7821363    15.345004656570287 0.09676126975502636
11   10.0745650 8.0106483     12.788176963635138 1.4594094943741613
12   10.6198257 9.9438713      14.54072959496546 0.4779719400128842
13    9.8442045 8.5292476     12.991992573716212 0.9298149409580412
14   9.5218499 10.4179416    14.099561785095878 -0.6336325176349823
```
