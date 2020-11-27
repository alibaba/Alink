## Description
Calculate the calc between texts in pair.
 We support different calc methods, by choosing the parameter "method", you can choose
 which method you use.
 LEVENSHTEIN: the minimum number of single-character edits (insertions, deletions or substitutions)
 required to change one word into the other.
 LEVENSHTEIN_SIM: calc = 1.0 - Normalized Distance.
 LCS: the longest subsequence common to the two inputs.
 LCS_SIM: Similarity = Distance / max(Left Length, Right Length)
 COSINE: a measure of calc between two non-zero vectors of an inner product
 space that measures the cosine of the angle between them.
 SSK: maps strings to a feature vector indexed by all k tuples of characters, and
 get the dot product.
 SIMHASH_HAMMING: Hash the inputs to BIT_LENGTH size, and calculate the hamming distance.
 SIMHASH_HAMMING_SIM: Similarity = 1.0 - distance / BIT_LENGTH.
 MINHASH_SIM: MinHashSim = P(hmin(A) = hmin(B)) = Count(I(hmin(A) = hmin(B))) / k.
 JACCARD_SIM: JaccardSim = |A ∩ B| / |A ∪ B| = |A ∩ B| / (|A| + |B| - |A ∩ B|)

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| lambda | punish factor. | Double |  | 0.5 |
| metric | Method to calculate calc or distance. | String |  | "LEVENSHTEIN_SIM" |
| windowSize | window size | Integer |  | 2 |
| numBucket | the number of bucket | Integer |  | 10 |
| numHashTables | The number of hash tables | Integer |  | 10 |
| seed | seed | Long |  | 0 |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| outputCol | Name of the output column | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
#### Code
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, "a b c d e", "a a b c e"],
    [1, "a a c e d w", "a a b b e d"],
    [2, "c d e f a", "b b c e f a"],
    [3, "b d e f h", "d d e a c"],
    [4, "a c e d m", "a e e f b c"]
])
df = pd.DataFrame({"id": data[:, 0], "text1": data[:, 1], "text2": data[:, 2]})
inOp1 = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='batch')
inOp2 = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='stream')

op = TextSimilarityPairwiseBatchOp().setSelectedCols(["text1", "text2"]).setMetric("LEVENSHTEIN").setOutputCol("output")
op.linkFrom(inOp1).print()

op = TextSimilarityPairwiseStreamOp().setSelectedCols(["text1", "text2"]).setMetric("COSINE").setOutputCol("output")
op.linkFrom(inOp2).print()
StreamOperator.execute()
```
#### Results
```
id|text1|text2|output
---|-----|-----|------
0|a b c d e|a a b c e|2.0
1|a a c e d w|a a b b e d|3.0
2|c d e f a|b b c e f a|3.0
3|b d e f h|d d e a c|3.0
4|a c e d m|a e e f b c|4.0
```


