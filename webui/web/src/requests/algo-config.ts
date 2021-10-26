export const algoData = [
  {
    "isDir": true,
    "name": "批组件",
    "children": [
      {
        "isDir": true,
        "name": "数据导入",
        "children": [
          {
            "name": "AkSourceBatchOp",
            "description": "AkSourceBatchOp",
            "id": "com.alibaba.alink.operator.batch.source.AkSourceBatchOp",
            "className": "com.alibaba.alink.operator.batch.source.AkSourceBatchOp",
            "params": [
              "filePath"
            ],
            "numInPorts": 0,
            "numOutPorts": 1
          },
          {
            "name": "CatalogSourceBatchOp",
            "description": "CatalogSourceBatchOp",
            "id": "com.alibaba.alink.operator.batch.source.CatalogSourceBatchOp",
            "className": "com.alibaba.alink.operator.batch.source.CatalogSourceBatchOp",
            "params": [
              "catalogObject"
            ],
            "numInPorts": 0,
            "numOutPorts": 1
          },
          {
            "name": "CsvSourceBatchOp",
            "description": "CsvSourceBatchOp",
            "id": "com.alibaba.alink.operator.batch.source.CsvSourceBatchOp",
            "className": "com.alibaba.alink.operator.batch.source.CsvSourceBatchOp",
            "params": [
              "filePath",
              "schemaStr",
              "fieldDelimiter",
              "quoteChar",
              "skipBlankLine",
              "rowDelimiter",
              "ignoreFirstLine",
              "lenient"
            ],
            "numInPorts": 0,
            "numOutPorts": 1
          },
          {
            "name": "DataSetWrapperBatchOp",
            "description": "DataSetWrapperBatchOp",
            "id": "com.alibaba.alink.operator.batch.source.DataSetWrapperBatchOp",
            "className": "com.alibaba.alink.operator.batch.source.DataSetWrapperBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LibSvmSourceBatchOp",
            "description": "LibSvmSourceBatchOp",
            "id": "com.alibaba.alink.operator.batch.source.LibSvmSourceBatchOp",
            "className": "com.alibaba.alink.operator.batch.source.LibSvmSourceBatchOp",
            "params": [
              "filePath",
              "startIndex"
            ],
            "numInPorts": 0,
            "numOutPorts": 1
          },
          {
            "name": "MemSourceBatchOp",
            "description": "MemSourceBatchOp",
            "id": "com.alibaba.alink.operator.batch.source.MemSourceBatchOp",
            "className": "com.alibaba.alink.operator.batch.source.MemSourceBatchOp",
            "params": [],
            "numInPorts": 0,
            "numOutPorts": 1
          },
          {
            "name": "NumSeqSourceBatchOp",
            "description": "NumSeqSourceBatchOp",
            "id": "com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp",
            "className": "com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp",
            "params": [
              "from",
              "to",
              "outputCol"
            ],
            "numInPorts": 0,
            "numOutPorts": 1
          },
          {
            "name": "RandomTableSourceBatchOp",
            "description": "RandomTableSourceBatchOp",
            "id": "com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp",
            "className": "com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp",
            "params": [
              "idCol",
              "outputColConfs",
              "outputCols",
              "numCols",
              "numRows"
            ],
            "numInPorts": 0,
            "numOutPorts": 1
          },
          {
            "name": "RandomVectorSourceBatchOp",
            "description": "RandomVectorSourceBatchOp",
            "id": "com.alibaba.alink.operator.batch.source.RandomVectorSourceBatchOp",
            "className": "com.alibaba.alink.operator.batch.source.RandomVectorSourceBatchOp",
            "params": [
              "idCol",
              "outputCol",
              "numRows",
              "size",
              "sparsity"
            ],
            "numInPorts": 0,
            "numOutPorts": 1
          },
          {
            "name": "TableSourceBatchOp",
            "description": "TableSourceBatchOp",
            "id": "com.alibaba.alink.operator.batch.source.TableSourceBatchOp",
            "className": "com.alibaba.alink.operator.batch.source.TableSourceBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TextSourceBatchOp",
            "description": "TextSourceBatchOp",
            "id": "com.alibaba.alink.operator.batch.source.TextSourceBatchOp",
            "className": "com.alibaba.alink.operator.batch.source.TextSourceBatchOp",
            "params": [
              "filePath",
              "ignoreFirstLine",
              "textCol"
            ],
            "numInPorts": 0,
            "numOutPorts": 1
          },
          {
            "name": "TsvSourceBatchOp",
            "description": "TsvSourceBatchOp",
            "id": "com.alibaba.alink.operator.batch.source.TsvSourceBatchOp",
            "className": "com.alibaba.alink.operator.batch.source.TsvSourceBatchOp",
            "params": [
              "filePath",
              "schemaStr",
              "skipBlankLine",
              "ignoreFirstLine"
            ],
            "numInPorts": 0,
            "numOutPorts": 1
          }
        ],
        "id": "batch.source"
      },
      {
        "isDir": true,
        "name": "数据导出",
        "children": [
          {
            "name": "AkSinkBatchOp",
            "description": "AkSinkBatchOp",
            "id": "com.alibaba.alink.operator.batch.sink.AkSinkBatchOp",
            "className": "com.alibaba.alink.operator.batch.sink.AkSinkBatchOp",
            "params": [
              "filePath",
              "overwriteSink",
              "numFiles"
            ],
            "numInPorts": 1,
            "numOutPorts": 0
          },
          {
            "name": "AppendModelStreamFileSinkBatchOp",
            "description": "AppendModelStreamFileSinkBatchOp",
            "id": "com.alibaba.alink.operator.batch.sink.AppendModelStreamFileSinkBatchOp",
            "className": "com.alibaba.alink.operator.batch.sink.AppendModelStreamFileSinkBatchOp",
            "params": [
              "numKeepModel",
              "modelTime",
              "numFiles",
              "filePath"
            ],
            "numInPorts": 1,
            "numOutPorts": 0
          },
          {
            "name": "CatalogSinkBatchOp",
            "description": "CatalogSinkBatchOp",
            "id": "com.alibaba.alink.operator.batch.sink.CatalogSinkBatchOp",
            "className": "com.alibaba.alink.operator.batch.sink.CatalogSinkBatchOp",
            "params": [
              "catalogObject"
            ],
            "numInPorts": 1,
            "numOutPorts": 0
          },
          {
            "name": "CsvSinkBatchOp",
            "description": "CsvSinkBatchOp",
            "id": "com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp",
            "className": "com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp",
            "params": [
              "filePath",
              "fieldDelimiter",
              "rowDelimiter",
              "quoteChar",
              "overwriteSink",
              "numFiles"
            ],
            "numInPorts": 1,
            "numOutPorts": 0
          },
          {
            "name": "LibSvmSinkBatchOp",
            "description": "LibSvmSinkBatchOp",
            "id": "com.alibaba.alink.operator.batch.sink.LibSvmSinkBatchOp",
            "className": "com.alibaba.alink.operator.batch.sink.LibSvmSinkBatchOp",
            "params": [
              "filePath",
              "overwriteSink",
              "vectorCol",
              "labelCol",
              "startIndex"
            ],
            "numInPorts": 1,
            "numOutPorts": 0
          },
          {
            "name": "TextSinkBatchOp",
            "description": "TextSinkBatchOp",
            "id": "com.alibaba.alink.operator.batch.sink.TextSinkBatchOp",
            "className": "com.alibaba.alink.operator.batch.sink.TextSinkBatchOp",
            "params": [
              "filePath",
              "overwriteSink",
              "numFiles"
            ],
            "numInPorts": 1,
            "numOutPorts": 0
          },
          {
            "name": "TsvSinkBatchOp",
            "description": "TsvSinkBatchOp",
            "id": "com.alibaba.alink.operator.batch.sink.TsvSinkBatchOp",
            "className": "com.alibaba.alink.operator.batch.sink.TsvSinkBatchOp",
            "params": [
              "filePath",
              "overwriteSink",
              "numFiles"
            ],
            "numInPorts": 1,
            "numOutPorts": 0
          }
        ],
        "id": "batch.sink"
      },
      {
        "isDir": true,
        "name": "数据处理",
        "children": [
          {
            "name": "AggLookupBatchOp",
            "description": "AggLookupBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.AggLookupBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.AggLookupBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "reservedCols",
              "delimiter",
              "clause",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AppendIdBatchOp",
            "description": "AppendIdBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp",
            "params": [
              "appendType",
              "idCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FirstNBatchOp",
            "description": "FirstNBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.FirstNBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.FirstNBatchOp",
            "params": [
              "size"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FlattenMTableBatchOp",
            "description": "FlattenMTableBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.FlattenMTableBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.FlattenMTableBatchOp",
            "params": [
              "selectedCol",
              "schemaStr",
              "reservedCols",
              "handleInvalidMethod"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GroupDataBatchOp",
            "description": "GroupDataBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.GroupDataBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.GroupDataBatchOp",
            "params": [
              "groupCols",
              "selectedCols",
              "outputCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "HugeLookupBatchOp",
            "description": "HugeLookupBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.HugeLookupBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.HugeLookupBatchOp",
            "params": [
              "mapKeyCols",
              "mapValueCols",
              "modelStreamUpdateMethod",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "HugeMultiStringIndexerPredictBatchOp",
            "description": "HugeMultiStringIndexerPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.HugeMultiStringIndexerPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.HugeMultiStringIndexerPredictBatchOp",
            "params": [
              "selectedCols",
              "reservedCols",
              "outputCols",
              "handleInvalid"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ImputerModelInfoBatchOp",
            "description": "ImputerModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.ImputerModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.ImputerModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ImputerPredictBatchOp",
            "description": "ImputerPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.ImputerPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.ImputerPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "outputCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ImputerTrainBatchOp",
            "description": "ImputerTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.ImputerTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.ImputerTrainBatchOp",
            "params": [
              "strategy",
              "selectedCols",
              "fillValue"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "IndexToStringPredictBatchOp",
            "description": "IndexToStringPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.IndexToStringPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.IndexToStringPredictBatchOp",
            "params": [
              "modelName",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCol",
              "reservedCols",
              "outputCol",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "JsonValueBatchOp",
            "description": "JsonValueBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.JsonValueBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.JsonValueBatchOp",
            "params": [
              "jsonPath",
              "skipFailed",
              "selectedCol",
              "reservedCols",
              "outputCols",
              "outputColTypes",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LookupBatchOp",
            "description": "LookupBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.LookupBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.LookupBatchOp",
            "params": [
              "mapKeyCols",
              "mapValueCols",
              "modelStreamUpdateMethod",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MaxAbsScalerModelInfoBatchOp",
            "description": "MaxAbsScalerModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.MaxAbsScalerModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.MaxAbsScalerModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MaxAbsScalerPredictBatchOp",
            "description": "MaxAbsScalerPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.MaxAbsScalerPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.MaxAbsScalerPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "outputCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MaxAbsScalerTrainBatchOp",
            "description": "MaxAbsScalerTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.MaxAbsScalerTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.MaxAbsScalerTrainBatchOp",
            "params": [
              "selectedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MinMaxScalerModelInfoBatchOp",
            "description": "MinMaxScalerModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.MinMaxScalerModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.MinMaxScalerModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MinMaxScalerPredictBatchOp",
            "description": "MinMaxScalerPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.MinMaxScalerPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.MinMaxScalerPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "outputCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MinMaxScalerTrainBatchOp",
            "description": "MinMaxScalerTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.MinMaxScalerTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.MinMaxScalerTrainBatchOp",
            "params": [
              "selectedCols",
              "min",
              "max"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MultiStringIndexerPredictBatchOp",
            "description": "MultiStringIndexerPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "handleInvalid",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MultiStringIndexerTrainBatchOp",
            "description": "MultiStringIndexerTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp",
            "params": [
              "selectedCols",
              "stringOrderType"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SampleBatchOp",
            "description": "SampleBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.SampleBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.SampleBatchOp",
            "params": [
              "ratio",
              "withReplacement"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SampleWithSizeBatchOp",
            "description": "SampleWithSizeBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.SampleWithSizeBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.SampleWithSizeBatchOp",
            "params": [
              "size",
              "withReplacement"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ShuffleBatchOp",
            "description": "ShuffleBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.ShuffleBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.ShuffleBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SplitBatchOp",
            "description": "SplitBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.SplitBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.SplitBatchOp",
            "params": [
              "fraction",
              "randomSeed"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StandardScalerModelInfoBatchOp",
            "description": "StandardScalerModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.StandardScalerModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.StandardScalerModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StandardScalerPredictBatchOp",
            "description": "StandardScalerPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.StandardScalerPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.StandardScalerPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "outputCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StandardScalerTrainBatchOp",
            "description": "StandardScalerTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.StandardScalerTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.StandardScalerTrainBatchOp",
            "params": [
              "selectedCols",
              "withMean",
              "withStd"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StratifiedSampleBatchOp",
            "description": "StratifiedSampleBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.StratifiedSampleBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.StratifiedSampleBatchOp",
            "params": [
              "strataCol",
              "strataRatio",
              "strataRatios",
              "withReplacement"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StratifiedSampleWithSizeBatchOp",
            "description": "StratifiedSampleWithSizeBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.StratifiedSampleWithSizeBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.StratifiedSampleWithSizeBatchOp",
            "params": [
              "strataCol",
              "strataSize",
              "strataSizes",
              "withReplacement"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StringIndexerPredictBatchOp",
            "description": "StringIndexerPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.StringIndexerPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.StringIndexerPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCol",
              "reservedCols",
              "handleInvalid",
              "outputCol",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StringIndexerTrainBatchOp",
            "description": "StringIndexerTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.StringIndexerTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.StringIndexerTrainBatchOp",
            "params": [
              "modelName",
              "selectedCol",
              "stringOrderType"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TypeConvertBatchOp",
            "description": "TypeConvertBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.TypeConvertBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.TypeConvertBatchOp",
            "params": [
              "selectedCols",
              "targetType"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "WeightSampleBatchOp",
            "description": "WeightSampleBatchOp",
            "id": "com.alibaba.alink.operator.batch.dataproc.WeightSampleBatchOp",
            "className": "com.alibaba.alink.operator.batch.dataproc.WeightSampleBatchOp",
            "params": [
              "weightCol",
              "ratio",
              "withReplacement"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "isDir": true,
            "name": "数据格式转换",
            "children": [
              {
                "name": "ColumnsToCsvBatchOp",
                "description": "ColumnsToCsvBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.ColumnsToCsvBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.ColumnsToCsvBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar",
                  "selectedCols"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "ColumnsToJsonBatchOp",
                "description": "ColumnsToJsonBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.ColumnsToJsonBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.ColumnsToJsonBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "jsonCol",
                  "selectedCols"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "ColumnsToKvBatchOp",
                "description": "ColumnsToKvBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.ColumnsToKvBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.ColumnsToKvBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter",
                  "selectedCols"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "ColumnsToTripleBatchOp",
                "description": "ColumnsToTripleBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.ColumnsToTripleBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.ColumnsToTripleBatchOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnValueSchemaStr",
                  "reservedCols",
                  "selectedCols"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "ColumnsToVectorBatchOp",
                "description": "ColumnsToVectorBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.ColumnsToVectorBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.ColumnsToVectorBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "vectorCol",
                  "vectorSize",
                  "selectedCols"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "CsvToColumnsBatchOp",
                "description": "CsvToColumnsBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.CsvToColumnsBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.CsvToColumnsBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "schemaStr",
                  "csvCol",
                  "csvFieldDelimiter",
                  "quoteChar"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "CsvToJsonBatchOp",
                "description": "CsvToJsonBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.CsvToJsonBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.CsvToJsonBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "jsonCol",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "CsvToKvBatchOp",
                "description": "CsvToKvBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.CsvToKvBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.CsvToKvBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "CsvToTripleBatchOp",
                "description": "CsvToTripleBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.CsvToTripleBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.CsvToTripleBatchOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnValueSchemaStr",
                  "reservedCols",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "CsvToVectorBatchOp",
                "description": "CsvToVectorBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.CsvToVectorBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.CsvToVectorBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "vectorCol",
                  "vectorSize",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "JsonToColumnsBatchOp",
                "description": "JsonToColumnsBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.JsonToColumnsBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.JsonToColumnsBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "schemaStr",
                  "jsonCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "JsonToCsvBatchOp",
                "description": "JsonToCsvBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.JsonToCsvBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.JsonToCsvBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar",
                  "jsonCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "JsonToKvBatchOp",
                "description": "JsonToKvBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.JsonToKvBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.JsonToKvBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter",
                  "jsonCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "JsonToTripleBatchOp",
                "description": "JsonToTripleBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.JsonToTripleBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.JsonToTripleBatchOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnValueSchemaStr",
                  "reservedCols",
                  "jsonCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "JsonToVectorBatchOp",
                "description": "JsonToVectorBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.JsonToVectorBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.JsonToVectorBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "vectorCol",
                  "vectorSize",
                  "jsonCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "KvToColumnsBatchOp",
                "description": "KvToColumnsBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.KvToColumnsBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.KvToColumnsBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "schemaStr",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "KvToCsvBatchOp",
                "description": "KvToCsvBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.KvToCsvBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.KvToCsvBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "KvToJsonBatchOp",
                "description": "KvToJsonBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.KvToJsonBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.KvToJsonBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "jsonCol",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "KvToTripleBatchOp",
                "description": "KvToTripleBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.KvToTripleBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.KvToTripleBatchOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnValueSchemaStr",
                  "reservedCols",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "KvToVectorBatchOp",
                "description": "KvToVectorBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.KvToVectorBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.KvToVectorBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "vectorCol",
                  "vectorSize",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "TripleToColumnsBatchOp",
                "description": "TripleToColumnsBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.TripleToColumnsBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.TripleToColumnsBatchOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnCol",
                  "tripleValueCol",
                  "schemaStr",
                  "tripleRowCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "TripleToCsvBatchOp",
                "description": "TripleToCsvBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.TripleToCsvBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.TripleToCsvBatchOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnCol",
                  "tripleValueCol",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar",
                  "tripleRowCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "TripleToJsonBatchOp",
                "description": "TripleToJsonBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.TripleToJsonBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.TripleToJsonBatchOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnCol",
                  "tripleValueCol",
                  "jsonCol",
                  "tripleRowCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "TripleToKvBatchOp",
                "description": "TripleToKvBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.TripleToKvBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.TripleToKvBatchOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnCol",
                  "tripleValueCol",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter",
                  "tripleRowCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "TripleToVectorBatchOp",
                "description": "TripleToVectorBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.TripleToVectorBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.TripleToVectorBatchOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnCol",
                  "tripleValueCol",
                  "vectorCol",
                  "vectorSize",
                  "tripleRowCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorToColumnsBatchOp",
                "description": "VectorToColumnsBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.VectorToColumnsBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.VectorToColumnsBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "schemaStr",
                  "vectorCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorToCsvBatchOp",
                "description": "VectorToCsvBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.VectorToCsvBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.VectorToCsvBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar",
                  "vectorCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorToJsonBatchOp",
                "description": "VectorToJsonBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.VectorToJsonBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.VectorToJsonBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "jsonCol",
                  "vectorCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorToKvBatchOp",
                "description": "VectorToKvBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.VectorToKvBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.VectorToKvBatchOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter",
                  "vectorCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorToTripleBatchOp",
                "description": "VectorToTripleBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.format.VectorToTripleBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.format.VectorToTripleBatchOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnValueSchemaStr",
                  "reservedCols",
                  "vectorCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              }
            ],
            "id": "batch.dataproc.format"
          },
          {
            "isDir": true,
            "name": "张量",
            "children": [
              {
                "name": "TensorToVectorBatchOp",
                "description": "TensorToVectorBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.tensor.TensorToVectorBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.tensor.TensorToVectorBatchOp",
                "params": [
                  "convertMethod",
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorToTensorBatchOp",
                "description": "VectorToTensorBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.tensor.VectorToTensorBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.tensor.VectorToTensorBatchOp",
                "params": [
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              }
            ],
            "id": "batch.dataproc.tensor"
          },
          {
            "isDir": true,
            "name": "向量",
            "children": [
              {
                "name": "VectorAssemblerBatchOp",
                "description": "VectorAssemblerBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp",
                "params": [
                  "handleInvalidMethod",
                  "selectedCols",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorElementwiseProductBatchOp",
                "description": "VectorElementwiseProductBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorElementwiseProductBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorElementwiseProductBatchOp",
                "params": [
                  "scalingVector",
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorImputerModelInfoBatchOp",
                "description": "VectorImputerModelInfoBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorImputerModelInfoBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorImputerModelInfoBatchOp",
                "params": [],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorImputerPredictBatchOp",
                "description": "VectorImputerPredictBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorImputerPredictBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorImputerPredictBatchOp",
                "params": [
                  "modelStreamFilePath",
                  "modelStreamScanInterval",
                  "modelStreamStartTime",
                  "outputCol",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorImputerTrainBatchOp",
                "description": "VectorImputerTrainBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorImputerTrainBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorImputerTrainBatchOp",
                "params": [
                  "strategy",
                  "selectedCol",
                  "fillValue"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorInteractionBatchOp",
                "description": "VectorInteractionBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorInteractionBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorInteractionBatchOp",
                "params": [
                  "selectedCols",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorMaxAbsScalerModelInfoBatchOp",
                "description": "VectorMaxAbsScalerModelInfoBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorMaxAbsScalerModelInfoBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorMaxAbsScalerModelInfoBatchOp",
                "params": [],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorMaxAbsScalerPredictBatchOp",
                "description": "VectorMaxAbsScalerPredictBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorMaxAbsScalerPredictBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorMaxAbsScalerPredictBatchOp",
                "params": [
                  "modelStreamFilePath",
                  "modelStreamScanInterval",
                  "modelStreamStartTime",
                  "outputCol",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorMaxAbsScalerTrainBatchOp",
                "description": "VectorMaxAbsScalerTrainBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorMaxAbsScalerTrainBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorMaxAbsScalerTrainBatchOp",
                "params": [
                  "selectedCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorMinMaxScalerModelInfoBatchOp",
                "description": "VectorMinMaxScalerModelInfoBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorMinMaxScalerModelInfoBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorMinMaxScalerModelInfoBatchOp",
                "params": [],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorMinMaxScalerPredictBatchOp",
                "description": "VectorMinMaxScalerPredictBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorMinMaxScalerPredictBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorMinMaxScalerPredictBatchOp",
                "params": [
                  "modelStreamFilePath",
                  "modelStreamScanInterval",
                  "modelStreamStartTime",
                  "outputCol",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorMinMaxScalerTrainBatchOp",
                "description": "VectorMinMaxScalerTrainBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorMinMaxScalerTrainBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorMinMaxScalerTrainBatchOp",
                "params": [
                  "selectedCol",
                  "min",
                  "max"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorNormalizeBatchOp",
                "description": "VectorNormalizeBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorNormalizeBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorNormalizeBatchOp",
                "params": [
                  "p",
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorPolynomialExpandBatchOp",
                "description": "VectorPolynomialExpandBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorPolynomialExpandBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorPolynomialExpandBatchOp",
                "params": [
                  "degree",
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorSizeHintBatchOp",
                "description": "VectorSizeHintBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorSizeHintBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorSizeHintBatchOp",
                "params": [
                  "size",
                  "handleInvalidMethod",
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorSliceBatchOp",
                "description": "VectorSliceBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorSliceBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorSliceBatchOp",
                "params": [
                  "indices",
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorStandardScalerModelInfoBatchOp",
                "description": "VectorStandardScalerModelInfoBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerModelInfoBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerModelInfoBatchOp",
                "params": [],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorStandardScalerPredictBatchOp",
                "description": "VectorStandardScalerPredictBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerPredictBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerPredictBatchOp",
                "params": [
                  "outputCol",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorStandardScalerTrainBatchOp",
                "description": "VectorStandardScalerTrainBatchOp",
                "id": "com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerTrainBatchOp",
                "className": "com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerTrainBatchOp",
                "params": [
                  "selectedCol",
                  "withMean",
                  "withStd"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              }
            ],
            "id": "batch.dataproc.vector"
          }
        ],
        "id": "batch.dataproc"
      },
      {
        "isDir": true,
        "name": "SQL",
        "children": [
          {
            "name": "AsBatchOp",
            "description": "AsBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.AsBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.AsBatchOp",
            "params": [
              "clause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DistinctBatchOp",
            "description": "DistinctBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.DistinctBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.DistinctBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FilterBatchOp",
            "description": "FilterBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.FilterBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.FilterBatchOp",
            "params": [
              "clause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FullOuterJoinBatchOp",
            "description": "FullOuterJoinBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.FullOuterJoinBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.FullOuterJoinBatchOp",
            "params": [
              "joinPredicate",
              "selectClause",
              "type"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GroupByBatchOp",
            "description": "GroupByBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.GroupByBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.GroupByBatchOp",
            "params": [
              "groupByPredicate",
              "selectClause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "IntersectAllBatchOp",
            "description": "IntersectAllBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.IntersectAllBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.IntersectAllBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "IntersectBatchOp",
            "description": "IntersectBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.IntersectBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.IntersectBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "JoinBatchOp",
            "description": "JoinBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.JoinBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.JoinBatchOp",
            "params": [
              "joinPredicate",
              "selectClause",
              "type"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LeftOuterJoinBatchOp",
            "description": "LeftOuterJoinBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.LeftOuterJoinBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.LeftOuterJoinBatchOp",
            "params": [
              "joinPredicate",
              "selectClause",
              "type"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MinusAllBatchOp",
            "description": "MinusAllBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.MinusAllBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.MinusAllBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MinusBatchOp",
            "description": "MinusBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.MinusBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.MinusBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "OrderByBatchOp",
            "description": "OrderByBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.OrderByBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.OrderByBatchOp",
            "params": [
              "fetch",
              "limit",
              "offset",
              "order",
              "clause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RightOuterJoinBatchOp",
            "description": "RightOuterJoinBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.RightOuterJoinBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.RightOuterJoinBatchOp",
            "params": [
              "joinPredicate",
              "selectClause",
              "type"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SelectBatchOp",
            "description": "SelectBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.SelectBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.SelectBatchOp",
            "params": [
              "clause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UnionAllBatchOp",
            "description": "UnionAllBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.UnionAllBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.UnionAllBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UnionBatchOp",
            "description": "UnionBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.UnionBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.UnionBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "WhereBatchOp",
            "description": "WhereBatchOp",
            "id": "com.alibaba.alink.operator.batch.sql.WhereBatchOp",
            "className": "com.alibaba.alink.operator.batch.sql.WhereBatchOp",
            "params": [
              "clause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.sql"
      },
      {
        "isDir": true,
        "name": "特征工程",
        "children": [
          {
            "name": "BinarizerBatchOp",
            "description": "BinarizerBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.BinarizerBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.BinarizerBatchOp",
            "params": [
              "threshold",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BucketizerBatchOp",
            "description": "BucketizerBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.BucketizerBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.BucketizerBatchOp",
            "params": [
              "cutsArray",
              "cutsArrayStr",
              "leftOpen",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "handleInvalid",
              "encode",
              "dropLast",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ChiSqSelectorBatchOp",
            "description": "ChiSqSelectorBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.ChiSqSelectorBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.ChiSqSelectorBatchOp",
            "params": [
              "selectorType",
              "numTopFeatures",
              "percentile",
              "fpr",
              "fdr",
              "fwe",
              "selectedCols",
              "labelCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ChisqSelectorModelInfoBatchOp",
            "description": "ChisqSelectorModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.ChisqSelectorModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.ChisqSelectorModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CrossFeaturePredictBatchOp",
            "description": "CrossFeaturePredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.CrossFeaturePredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.CrossFeaturePredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "outputCol",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CrossFeatureTrainBatchOp",
            "description": "CrossFeatureTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.CrossFeatureTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.CrossFeatureTrainBatchOp",
            "params": [
              "selectedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DCTBatchOp",
            "description": "DCTBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.DCTBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.DCTBatchOp",
            "params": [
              "inverse",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "EqualWidthDiscretizerModelInfoBatchOp",
            "description": "EqualWidthDiscretizerModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "EqualWidthDiscretizerPredictBatchOp",
            "description": "EqualWidthDiscretizerPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "handleInvalid",
              "encode",
              "dropLast",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "EqualWidthDiscretizerTrainBatchOp",
            "description": "EqualWidthDiscretizerTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerTrainBatchOp",
            "params": [
              "selectedCols",
              "numBuckets",
              "numBucketsArray",
              "leftOpen"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FeatureHasherBatchOp",
            "description": "FeatureHasherBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.FeatureHasherBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.FeatureHasherBatchOp",
            "params": [
              "selectedCols",
              "outputCol",
              "reservedCols",
              "numFeatures",
              "categoricalCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "HashCrossFeatureBatchOp",
            "description": "HashCrossFeatureBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.HashCrossFeatureBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.HashCrossFeatureBatchOp",
            "params": [
              "selectedCols",
              "numFeatures",
              "outputCol",
              "reservedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MultiHotModelInfoBatchOp",
            "description": "MultiHotModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.MultiHotModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.MultiHotModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MultiHotPredictBatchOp",
            "description": "MultiHotPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.MultiHotPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.MultiHotPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "handleInvalid",
              "encode",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MultiHotTrainBatchOp",
            "description": "MultiHotTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.MultiHotTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.MultiHotTrainBatchOp",
            "params": [
              "selectedCols",
              "delimiter",
              "discreteThresholds",
              "discreteThresholdsArray"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "OneHotModelInfoBatchOp",
            "description": "OneHotModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.OneHotModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.OneHotModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "OneHotPredictBatchOp",
            "description": "OneHotPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.OneHotPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.OneHotPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "handleInvalid",
              "encode",
              "dropLast",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "OneHotTrainBatchOp",
            "description": "OneHotTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.OneHotTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.OneHotTrainBatchOp",
            "params": [
              "selectedCols",
              "discreteThresholds",
              "discreteThresholdsArray"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "OverWindowBatchOp",
            "description": "OverWindowBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.OverWindowBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.OverWindowBatchOp",
            "params": [
              "partitionCols",
              "orderBy",
              "clause",
              "reservedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "PcaModelInfoBatchOp",
            "description": "PcaModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.PcaModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.PcaModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "PcaPredictBatchOp",
            "description": "PcaPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.PcaPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.PcaPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "reservedCols",
              "predictionCol",
              "vectorCol",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "PcaTrainBatchOp",
            "description": "PcaTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.PcaTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.PcaTrainBatchOp",
            "params": [
              "selectedCols",
              "vectorCol",
              "k",
              "calculationType"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "QuantileDiscretizerModelInfoBatchOp",
            "description": "QuantileDiscretizerModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.QuantileDiscretizerModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.QuantileDiscretizerModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "QuantileDiscretizerPredictBatchOp",
            "description": "QuantileDiscretizerPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.QuantileDiscretizerPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.QuantileDiscretizerPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "handleInvalid",
              "encode",
              "dropLast",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "QuantileDiscretizerTrainBatchOp",
            "description": "QuantileDiscretizerTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp",
            "params": [
              "selectedCols",
              "numBuckets",
              "numBucketsArray",
              "leftOpen"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TreeModelEncoderBatchOp",
            "description": "TreeModelEncoderBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.TreeModelEncoderBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.TreeModelEncoderBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "reservedCols",
              "predictionCol",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "VectorChiSqSelectorBatchOp",
            "description": "VectorChiSqSelectorBatchOp",
            "id": "com.alibaba.alink.operator.batch.feature.VectorChiSqSelectorBatchOp",
            "className": "com.alibaba.alink.operator.batch.feature.VectorChiSqSelectorBatchOp",
            "params": [
              "selectorType",
              "numTopFeatures",
              "percentile",
              "fpr",
              "fdr",
              "fwe",
              "selectedCol",
              "labelCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.feature"
      },
      {
        "isDir": true,
        "name": "文本",
        "children": [
          {
            "name": "BertTextEmbeddingBatchOp",
            "description": "BertTextEmbeddingBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.BertTextEmbeddingBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.BertTextEmbeddingBatchOp",
            "params": [
              "selectedCol",
              "outputCol",
              "maxSeqLength",
              "doLowerCase",
              "layer",
              "bertModelName",
              "modelPath",
              "reservedCols",
              "intraOpParallelism"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DocCountVectorizerPredictBatchOp",
            "description": "DocCountVectorizerPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.DocCountVectorizerPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.DocCountVectorizerPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DocCountVectorizerTrainBatchOp",
            "description": "DocCountVectorizerTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.DocCountVectorizerTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.DocCountVectorizerTrainBatchOp",
            "params": [
              "maxDF",
              "selectedCol",
              "minDF",
              "featureType",
              "vocabSize",
              "minTF"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DocHashCountVectorizerPredictBatchOp",
            "description": "DocHashCountVectorizerPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.DocHashCountVectorizerPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.DocHashCountVectorizerPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "numThreads",
              "selectedCol",
              "outputCol",
              "reservedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DocHashCountVectorizerTrainBatchOp",
            "description": "DocHashCountVectorizerTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.DocHashCountVectorizerTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.DocHashCountVectorizerTrainBatchOp",
            "params": [
              "selectedCol",
              "numFeatures",
              "minDF",
              "featureType",
              "minTF"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DocWordCountBatchOp",
            "description": "DocWordCountBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.DocWordCountBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.DocWordCountBatchOp",
            "params": [
              "docIdCol",
              "contentCol",
              "wordDelimiter"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KeywordsExtractionBatchOp",
            "description": "KeywordsExtractionBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.KeywordsExtractionBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.KeywordsExtractionBatchOp",
            "params": [
              "selectedCol",
              "topN",
              "windowSize",
              "dampingFactor",
              "maxIter",
              "outputCol",
              "epsilon",
              "method"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "NGramBatchOp",
            "description": "NGramBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.NGramBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.NGramBatchOp",
            "params": [
              "n",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RegexTokenizerBatchOp",
            "description": "RegexTokenizerBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.RegexTokenizerBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.RegexTokenizerBatchOp",
            "params": [
              "pattern",
              "gaps",
              "minTokenLength",
              "toLowerCase",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SegmentBatchOp",
            "description": "SegmentBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.SegmentBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.SegmentBatchOp",
            "params": [
              "userDefinedDict",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StopWordsRemoverBatchOp",
            "description": "StopWordsRemoverBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.StopWordsRemoverBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.StopWordsRemoverBatchOp",
            "params": [
              "caseSensitive",
              "stopWords",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TfidfBatchOp",
            "description": "TfidfBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.TfidfBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.TfidfBatchOp",
            "params": [
              "wordCol",
              "countCol",
              "docIdCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TokenizerBatchOp",
            "description": "TokenizerBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.TokenizerBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.TokenizerBatchOp",
            "params": [
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "Word2VecPredictBatchOp",
            "description": "Word2VecPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.Word2VecPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.Word2VecPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCol",
              "reservedCols",
              "outputCol",
              "wordDelimiter",
              "predMethod",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "Word2VecTrainBatchOp",
            "description": "Word2VecTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.Word2VecTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.Word2VecTrainBatchOp",
            "params": [
              "numIter",
              "selectedCol",
              "vectorSize",
              "alpha",
              "wordDelimiter",
              "minCount",
              "randomWindow",
              "window"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "WordCountBatchOp",
            "description": "WordCountBatchOp",
            "id": "com.alibaba.alink.operator.batch.nlp.WordCountBatchOp",
            "className": "com.alibaba.alink.operator.batch.nlp.WordCountBatchOp",
            "params": [
              "selectedCol",
              "wordDelimiter"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.nlp"
      },
      {
        "isDir": true,
        "name": "统计分析",
        "children": [
          {
            "name": "ChiSquareTestBatchOp",
            "description": "ChiSquareTestBatchOp",
            "id": "com.alibaba.alink.operator.batch.statistics.ChiSquareTestBatchOp",
            "className": "com.alibaba.alink.operator.batch.statistics.ChiSquareTestBatchOp",
            "params": [
              "labelCol",
              "selectedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CorrelationBatchOp",
            "description": "CorrelationBatchOp",
            "id": "com.alibaba.alink.operator.batch.statistics.CorrelationBatchOp",
            "className": "com.alibaba.alink.operator.batch.statistics.CorrelationBatchOp",
            "params": [
              "selectedCols",
              "method"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SummarizerBatchOp",
            "description": "SummarizerBatchOp",
            "id": "com.alibaba.alink.operator.batch.statistics.SummarizerBatchOp",
            "className": "com.alibaba.alink.operator.batch.statistics.SummarizerBatchOp",
            "params": [
              "selectedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "VectorChiSquareTestBatchOp",
            "description": "VectorChiSquareTestBatchOp",
            "id": "com.alibaba.alink.operator.batch.statistics.VectorChiSquareTestBatchOp",
            "className": "com.alibaba.alink.operator.batch.statistics.VectorChiSquareTestBatchOp",
            "params": [
              "labelCol",
              "selectedCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "VectorCorrelationBatchOp",
            "description": "VectorCorrelationBatchOp",
            "id": "com.alibaba.alink.operator.batch.statistics.VectorCorrelationBatchOp",
            "className": "com.alibaba.alink.operator.batch.statistics.VectorCorrelationBatchOp",
            "params": [
              "selectedCol",
              "method"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "VectorSummarizerBatchOp",
            "description": "VectorSummarizerBatchOp",
            "id": "com.alibaba.alink.operator.batch.statistics.VectorSummarizerBatchOp",
            "className": "com.alibaba.alink.operator.batch.statistics.VectorSummarizerBatchOp",
            "params": [
              "selectedCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.statistics"
      },
      {
        "isDir": true,
        "name": "分类",
        "children": [
          {
            "name": "BertTextClassifierPredictBatchOp",
            "description": "BertTextClassifierPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.BertTextClassifierPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.BertTextClassifierPredictBatchOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "predictionDetailCol",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BertTextClassifierTrainBatchOp",
            "description": "BertTextClassifierTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.BertTextClassifierTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.BertTextClassifierTrainBatchOp",
            "params": [
              "taskName",
              "taskType",
              "selectedCols",
              "bertModelName",
              "modelPath",
              "numEpochs",
              "textCol",
              "textPairCol",
              "labelCol",
              "maxSeqLength",
              "numFineTunedLayers",
              "customConfigJson",
              "learningRate",
              "batchSize",
              "pythonEnv",
              "intraOpParallelism",
              "numWorkers",
              "numPSs",
              "checkpointFilePath",
              "removeCheckpointBeforeTraining"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BertTextPairClassifierPredictBatchOp",
            "description": "BertTextPairClassifierPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.BertTextPairClassifierPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.BertTextPairClassifierPredictBatchOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "predictionDetailCol",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BertTextPairClassifierTrainBatchOp",
            "description": "BertTextPairClassifierTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.BertTextPairClassifierTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.BertTextPairClassifierTrainBatchOp",
            "params": [
              "taskName",
              "taskType",
              "selectedCols",
              "bertModelName",
              "modelPath",
              "numEpochs",
              "textCol",
              "textPairCol",
              "labelCol",
              "maxSeqLength",
              "numFineTunedLayers",
              "customConfigJson",
              "learningRate",
              "batchSize",
              "pythonEnv",
              "intraOpParallelism",
              "numWorkers",
              "numPSs",
              "checkpointFilePath",
              "removeCheckpointBeforeTraining"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "C45ModelInfoBatchOp",
            "description": "C45ModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.C45ModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.C45ModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "C45PredictBatchOp",
            "description": "C45PredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.C45PredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.C45PredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "C45TrainBatchOp",
            "description": "C45TrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.C45TrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.C45TrainBatchOp",
            "params": [
              "maxDepth",
              "minSamplesPerLeaf",
              "createTreeMode",
              "maxBins",
              "maxMemoryInMB",
              "featureCols",
              "labelCol",
              "categoricalCols",
              "weightCol",
              "maxLeaves",
              "minSampleRatioPerChild",
              "minInfoGain"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CartModelInfoBatchOp",
            "description": "CartModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.CartModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.CartModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CartPredictBatchOp",
            "description": "CartPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.CartPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.CartPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CartTrainBatchOp",
            "description": "CartTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.CartTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.CartTrainBatchOp",
            "params": [
              "maxDepth",
              "minSamplesPerLeaf",
              "createTreeMode",
              "maxBins",
              "maxMemoryInMB",
              "featureCols",
              "labelCol",
              "categoricalCols",
              "weightCol",
              "maxLeaves",
              "minSampleRatioPerChild",
              "minInfoGain"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DecisionTreeModelInfoBatchOp",
            "description": "DecisionTreeModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.DecisionTreeModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.DecisionTreeModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DecisionTreePredictBatchOp",
            "description": "DecisionTreePredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.DecisionTreePredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.DecisionTreePredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DecisionTreeTrainBatchOp",
            "description": "DecisionTreeTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.DecisionTreeTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.DecisionTreeTrainBatchOp",
            "params": [
              "treeType",
              "maxDepth",
              "minSamplesPerLeaf",
              "createTreeMode",
              "maxBins",
              "maxMemoryInMB",
              "featureCols",
              "labelCol",
              "categoricalCols",
              "weightCol",
              "maxLeaves",
              "minSampleRatioPerChild",
              "minInfoGain"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmClassifierModelInfoBatchOp",
            "description": "FmClassifierModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.FmClassifierModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.FmClassifierModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmClassifierPredictBatchOp",
            "description": "FmClassifierPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.FmClassifierPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.FmClassifierPredictBatchOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmClassifierTrainBatchOp",
            "description": "FmClassifierTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.FmClassifierTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.FmClassifierTrainBatchOp",
            "params": [
              "minibatchSize",
              "labelCol",
              "vectorCol",
              "weightCol",
              "epsilon",
              "featureCols",
              "withIntercept",
              "withLinearItem",
              "numFactor",
              "lambda0",
              "lambda1",
              "lambda2",
              "numEpochs",
              "learnRate",
              "initStdev"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GbdtModelInfoBatchOp",
            "description": "GbdtModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.GbdtModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.GbdtModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GbdtPredictBatchOp",
            "description": "GbdtPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.GbdtPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.GbdtPredictBatchOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GbdtTrainBatchOp",
            "description": "GbdtTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.GbdtTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.GbdtTrainBatchOp",
            "params": [
              "learningRate",
              "minSumHessianPerLeaf",
              "lambda",
              "gamma",
              "criteriaType",
              "algoType",
              "useMissing",
              "useOneHot",
              "useEpsilonApproQuantile",
              "sketchEps",
              "sketchRatio",
              "vectorCol",
              "numTrees",
              "minSamplesPerLeaf",
              "maxDepth",
              "subsamplingRatio",
              "featureSubsamplingRatio",
              "maxBins",
              "newtonStep",
              "featureImportanceType",
              "featureCols",
              "labelCol",
              "categoricalCols",
              "weightCol",
              "maxLeaves",
              "minSampleRatioPerChild",
              "minInfoGain"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "Id3ModelInfoBatchOp",
            "description": "Id3ModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.Id3ModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.Id3ModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "Id3PredictBatchOp",
            "description": "Id3PredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.Id3PredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.Id3PredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "Id3TrainBatchOp",
            "description": "Id3TrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.Id3TrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.Id3TrainBatchOp",
            "params": [
              "maxDepth",
              "minSamplesPerLeaf",
              "createTreeMode",
              "maxBins",
              "maxMemoryInMB",
              "featureCols",
              "labelCol",
              "categoricalCols",
              "weightCol",
              "maxLeaves",
              "minSampleRatioPerChild",
              "minInfoGain"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KerasSequentialClassifierPredictBatchOp",
            "description": "KerasSequentialClassifierPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.KerasSequentialClassifierPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.KerasSequentialClassifierPredictBatchOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "predictionDetailCol",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KerasSequentialClassifierTrainBatchOp",
            "description": "KerasSequentialClassifierTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.KerasSequentialClassifierTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.KerasSequentialClassifierTrainBatchOp",
            "params": [
              "tensorCol",
              "labelCol",
              "layers",
              "optimizer",
              "learningRate",
              "numEpochs",
              "batchSize",
              "checkpointFilePath",
              "pythonEnv",
              "intraOpParallelism",
              "numWorkers",
              "numPSs",
              "removeCheckpointBeforeTraining"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KnnPredictBatchOp",
            "description": "KnnPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.KnnPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.KnnPredictBatchOp",
            "params": [
              "k",
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KnnTrainBatchOp",
            "description": "KnnTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.KnnTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.KnnTrainBatchOp",
            "params": [
              "labelCol",
              "featureCols",
              "vectorCol",
              "reservedCols",
              "distanceType"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LinearSvmModelInfoBatchOp",
            "description": "LinearSvmModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.LinearSvmModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.LinearSvmModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LinearSvmPredictBatchOp",
            "description": "LinearSvmPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.LinearSvmPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.LinearSvmPredictBatchOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LinearSvmTrainBatchOp",
            "description": "LinearSvmTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.LinearSvmTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.LinearSvmTrainBatchOp",
            "params": [
              "optimMethod",
              "l1",
              "l2",
              "withIntercept",
              "maxIter",
              "epsilon",
              "featureCols",
              "labelCol",
              "weightCol",
              "vectorCol",
              "standardization"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LogisticRegressionModelInfoBatchOp",
            "description": "LogisticRegressionModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.LogisticRegressionModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.LogisticRegressionModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LogisticRegressionPredictBatchOp",
            "description": "LogisticRegressionPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.LogisticRegressionPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.LogisticRegressionPredictBatchOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LogisticRegressionTrainBatchOp",
            "description": "LogisticRegressionTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp",
            "params": [
              "optimMethod",
              "l1",
              "l2",
              "withIntercept",
              "maxIter",
              "epsilon",
              "featureCols",
              "labelCol",
              "weightCol",
              "vectorCol",
              "standardization"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MultilayerPerceptronPredictBatchOp",
            "description": "MultilayerPerceptronPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.MultilayerPerceptronPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.MultilayerPerceptronPredictBatchOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MultilayerPerceptronTrainBatchOp",
            "description": "MultilayerPerceptronTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.MultilayerPerceptronTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.MultilayerPerceptronTrainBatchOp",
            "params": [
              "layers",
              "blockSize",
              "initialWeights",
              "vectorCol",
              "featureCols",
              "labelCol",
              "maxIter",
              "epsilon",
              "l1",
              "l2"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "NaiveBayesModelInfoBatchOp",
            "description": "NaiveBayesModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.NaiveBayesModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.NaiveBayesModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "NaiveBayesPredictBatchOp",
            "description": "NaiveBayesPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.NaiveBayesPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.NaiveBayesPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "NaiveBayesTextModelInfoBatchOp",
            "description": "NaiveBayesTextModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.NaiveBayesTextModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.NaiveBayesTextModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "NaiveBayesTextPredictBatchOp",
            "description": "NaiveBayesTextPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.NaiveBayesTextPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.NaiveBayesTextPredictBatchOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "NaiveBayesTextTrainBatchOp",
            "description": "NaiveBayesTextTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.NaiveBayesTextTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.NaiveBayesTextTrainBatchOp",
            "params": [
              "modelType",
              "labelCol",
              "weightCol",
              "vectorCol",
              "smoothing"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "NaiveBayesTrainBatchOp",
            "description": "NaiveBayesTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.NaiveBayesTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.NaiveBayesTrainBatchOp",
            "params": [
              "smoothing",
              "categoricalCols",
              "featureCols",
              "labelCol",
              "weightCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RandomForestModelInfoBatchOp",
            "description": "RandomForestModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.RandomForestModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.RandomForestModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RandomForestPredictBatchOp",
            "description": "RandomForestPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.RandomForestPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.RandomForestPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RandomForestTrainBatchOp",
            "description": "RandomForestTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp",
            "params": [
              "featureSubsamplingRatio",
              "numSubsetFeatures",
              "numTrees",
              "numTreesOfGini",
              "numTreesOfInfoGain",
              "numTreesOfInfoGainRatio",
              "subsamplingRatio",
              "maxDepth",
              "minSamplesPerLeaf",
              "createTreeMode",
              "maxBins",
              "maxMemoryInMB",
              "featureCols",
              "labelCol",
              "categoricalCols",
              "weightCol",
              "maxLeaves",
              "minSampleRatioPerChild",
              "minInfoGain"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SoftmaxModelInfoBatchOp",
            "description": "SoftmaxModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.SoftmaxModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.SoftmaxModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SoftmaxPredictBatchOp",
            "description": "SoftmaxPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.SoftmaxPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.SoftmaxPredictBatchOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SoftmaxTrainBatchOp",
            "description": "SoftmaxTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.SoftmaxTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.SoftmaxTrainBatchOp",
            "params": [
              "optimMethod",
              "l1",
              "l2",
              "withIntercept",
              "maxIter",
              "epsilon",
              "featureCols",
              "labelCol",
              "weightCol",
              "vectorCol",
              "standardization"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TFTableModelClassifierPredictBatchOp",
            "description": "TFTableModelClassifierPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.classification.TFTableModelClassifierPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.classification.TFTableModelClassifierPredictBatchOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "predictionDetailCol",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.classification"
      },
      {
        "isDir": true,
        "name": "回归",
        "children": [
          {
            "name": "AftSurvivalRegModelInfoBatchOp",
            "description": "AftSurvivalRegModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.AftSurvivalRegModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.AftSurvivalRegModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AftSurvivalRegPredictBatchOp",
            "description": "AftSurvivalRegPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.AftSurvivalRegPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.AftSurvivalRegPredictBatchOp",
            "params": [
              "quantileProbabilities",
              "vectorCol",
              "predictionDetailCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AftSurvivalRegTrainBatchOp",
            "description": "AftSurvivalRegTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.AftSurvivalRegTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.AftSurvivalRegTrainBatchOp",
            "params": [
              "censorCol",
              "maxIter",
              "epsilon",
              "withIntercept",
              "labelCol",
              "vectorCol",
              "featureCols",
              "l1",
              "l2"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BertTextPairRegressorPredictBatchOp",
            "description": "BertTextPairRegressorPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.BertTextPairRegressorPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.BertTextPairRegressorPredictBatchOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BertTextPairRegressorTrainBatchOp",
            "description": "BertTextPairRegressorTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.BertTextPairRegressorTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.BertTextPairRegressorTrainBatchOp",
            "params": [
              "taskName",
              "taskType",
              "selectedCols",
              "bertModelName",
              "modelPath",
              "numEpochs",
              "textCol",
              "textPairCol",
              "labelCol",
              "maxSeqLength",
              "numFineTunedLayers",
              "customConfigJson",
              "learningRate",
              "batchSize",
              "pythonEnv",
              "intraOpParallelism",
              "numWorkers",
              "numPSs",
              "checkpointFilePath",
              "removeCheckpointBeforeTraining"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BertTextRegressorPredictBatchOp",
            "description": "BertTextRegressorPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.BertTextRegressorPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.BertTextRegressorPredictBatchOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BertTextRegressorTrainBatchOp",
            "description": "BertTextRegressorTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.BertTextRegressorTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.BertTextRegressorTrainBatchOp",
            "params": [
              "taskName",
              "taskType",
              "selectedCols",
              "bertModelName",
              "modelPath",
              "numEpochs",
              "textCol",
              "textPairCol",
              "labelCol",
              "maxSeqLength",
              "numFineTunedLayers",
              "customConfigJson",
              "learningRate",
              "batchSize",
              "pythonEnv",
              "intraOpParallelism",
              "numWorkers",
              "numPSs",
              "checkpointFilePath",
              "removeCheckpointBeforeTraining"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CartRegModelInfoBatchOp",
            "description": "CartRegModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.CartRegModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.CartRegModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CartRegPredictBatchOp",
            "description": "CartRegPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.CartRegPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.CartRegPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CartRegTrainBatchOp",
            "description": "CartRegTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.CartRegTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.CartRegTrainBatchOp",
            "params": [
              "maxDepth",
              "minSamplesPerLeaf",
              "createTreeMode",
              "maxBins",
              "maxMemoryInMB",
              "featureCols",
              "labelCol",
              "categoricalCols",
              "weightCol",
              "maxLeaves",
              "minSampleRatioPerChild",
              "minInfoGain"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DecisionTreeRegModelInfoBatchOp",
            "description": "DecisionTreeRegModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.DecisionTreeRegModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.DecisionTreeRegModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DecisionTreeRegPredictBatchOp",
            "description": "DecisionTreeRegPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.DecisionTreeRegPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.DecisionTreeRegPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DecisionTreeRegTrainBatchOp",
            "description": "DecisionTreeRegTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.DecisionTreeRegTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.DecisionTreeRegTrainBatchOp",
            "params": [
              "maxDepth",
              "minSamplesPerLeaf",
              "createTreeMode",
              "maxBins",
              "maxMemoryInMB",
              "featureCols",
              "labelCol",
              "categoricalCols",
              "weightCol",
              "maxLeaves",
              "minSampleRatioPerChild",
              "minInfoGain"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmRegressorModelInfoBatchOp",
            "description": "FmRegressorModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.FmRegressorModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.FmRegressorModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmRegressorPredictBatchOp",
            "description": "FmRegressorPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.FmRegressorPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.FmRegressorPredictBatchOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmRegressorTrainBatchOp",
            "description": "FmRegressorTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.FmRegressorTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.FmRegressorTrainBatchOp",
            "params": [
              "minibatchSize",
              "labelCol",
              "vectorCol",
              "weightCol",
              "epsilon",
              "featureCols",
              "withIntercept",
              "withLinearItem",
              "numFactor",
              "lambda0",
              "lambda1",
              "lambda2",
              "numEpochs",
              "learnRate",
              "initStdev"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GbdtRegModelInfoBatchOp",
            "description": "GbdtRegModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.GbdtRegModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.GbdtRegModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GbdtRegPredictBatchOp",
            "description": "GbdtRegPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.GbdtRegPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.GbdtRegPredictBatchOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GbdtRegTrainBatchOp",
            "description": "GbdtRegTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.GbdtRegTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.GbdtRegTrainBatchOp",
            "params": [
              "algoType",
              "useMissing",
              "useOneHot",
              "useEpsilonApproQuantile",
              "sketchEps",
              "sketchRatio",
              "learningRate",
              "minSumHessianPerLeaf",
              "lambda",
              "gamma",
              "criteriaType",
              "vectorCol",
              "numTrees",
              "minSamplesPerLeaf",
              "maxDepth",
              "subsamplingRatio",
              "featureSubsamplingRatio",
              "maxBins",
              "newtonStep",
              "featureImportanceType",
              "featureCols",
              "labelCol",
              "categoricalCols",
              "weightCol",
              "maxLeaves",
              "minSampleRatioPerChild",
              "minInfoGain"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GlmEvaluationBatchOp",
            "description": "GlmEvaluationBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.GlmEvaluationBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.GlmEvaluationBatchOp",
            "params": [
              "family",
              "variancePower",
              "link",
              "linkPower",
              "offsetCol",
              "fitIntercept",
              "regParam",
              "epsilon",
              "weightCol",
              "maxIter",
              "featureCols",
              "labelCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GlmModelInfoBatchOp",
            "description": "GlmModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.GlmModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.GlmModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GlmPredictBatchOp",
            "description": "GlmPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.GlmPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.GlmPredictBatchOp",
            "params": [
              "linkPredResultCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GlmTrainBatchOp",
            "description": "GlmTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.GlmTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.GlmTrainBatchOp",
            "params": [
              "family",
              "variancePower",
              "link",
              "linkPower",
              "offsetCol",
              "fitIntercept",
              "regParam",
              "epsilon",
              "weightCol",
              "maxIter",
              "featureCols",
              "labelCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "IsotonicRegPredictBatchOp",
            "description": "IsotonicRegPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.IsotonicRegPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.IsotonicRegPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "IsotonicRegTrainBatchOp",
            "description": "IsotonicRegTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.IsotonicRegTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.IsotonicRegTrainBatchOp",
            "params": [
              "featureCol",
              "isotonic",
              "featureIndex",
              "labelCol",
              "weightCol",
              "vectorCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KerasSequentialRegressorPredictBatchOp",
            "description": "KerasSequentialRegressorPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.KerasSequentialRegressorPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.KerasSequentialRegressorPredictBatchOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KerasSequentialRegressorTrainBatchOp",
            "description": "KerasSequentialRegressorTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.KerasSequentialRegressorTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.KerasSequentialRegressorTrainBatchOp",
            "params": [
              "tensorCol",
              "labelCol",
              "layers",
              "optimizer",
              "learningRate",
              "numEpochs",
              "batchSize",
              "checkpointFilePath",
              "pythonEnv",
              "intraOpParallelism",
              "numWorkers",
              "numPSs",
              "removeCheckpointBeforeTraining"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LassoRegModelInfoBatchOp",
            "description": "LassoRegModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.LassoRegModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.LassoRegModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LassoRegPredictBatchOp",
            "description": "LassoRegPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.LassoRegPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.LassoRegPredictBatchOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LassoRegTrainBatchOp",
            "description": "LassoRegTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.LassoRegTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.LassoRegTrainBatchOp",
            "params": [
              "optimMethod",
              "lambda",
              "withIntercept",
              "maxIter",
              "epsilon",
              "featureCols",
              "labelCol",
              "weightCol",
              "vectorCol",
              "standardization"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LinearRegModelInfoBatchOp",
            "description": "LinearRegModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.LinearRegModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.LinearRegModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LinearRegPredictBatchOp",
            "description": "LinearRegPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.LinearRegPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.LinearRegPredictBatchOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LinearRegTrainBatchOp",
            "description": "LinearRegTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp",
            "params": [
              "optimMethod",
              "l1",
              "l2",
              "withIntercept",
              "maxIter",
              "epsilon",
              "featureCols",
              "labelCol",
              "weightCol",
              "vectorCol",
              "standardization"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RandomForestRegModelInfoBatchOp",
            "description": "RandomForestRegModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.RandomForestRegModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.RandomForestRegModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RandomForestRegPredictBatchOp",
            "description": "RandomForestRegPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.RandomForestRegPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.RandomForestRegPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RandomForestRegTrainBatchOp",
            "description": "RandomForestRegTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.RandomForestRegTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.RandomForestRegTrainBatchOp",
            "params": [
              "numSubsetFeatures",
              "numTrees",
              "subsamplingRatio",
              "seed",
              "maxDepth",
              "minSamplesPerLeaf",
              "createTreeMode",
              "maxBins",
              "maxMemoryInMB",
              "featureCols",
              "labelCol",
              "categoricalCols",
              "weightCol",
              "maxLeaves",
              "minSampleRatioPerChild",
              "minInfoGain"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RidgeRegModelInfoBatchOp",
            "description": "RidgeRegModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.RidgeRegModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.RidgeRegModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RidgeRegPredictBatchOp",
            "description": "RidgeRegPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.RidgeRegPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.RidgeRegPredictBatchOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RidgeRegTrainBatchOp",
            "description": "RidgeRegTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.RidgeRegTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.RidgeRegTrainBatchOp",
            "params": [
              "optimMethod",
              "lambda",
              "withIntercept",
              "maxIter",
              "epsilon",
              "featureCols",
              "labelCol",
              "weightCol",
              "vectorCol",
              "standardization"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TFTableModelRegressorPredictBatchOp",
            "description": "TFTableModelRegressorPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.regression.TFTableModelRegressorPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.regression.TFTableModelRegressorPredictBatchOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.regression"
      },
      {
        "isDir": true,
        "name": "聚类",
        "children": [
          {
            "name": "BisectingKMeansModelInfoBatchOp",
            "description": "BisectingKMeansModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.BisectingKMeansModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.BisectingKMeansModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BisectingKMeansPredictBatchOp",
            "description": "BisectingKMeansPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.BisectingKMeansPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.BisectingKMeansPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BisectingKMeansTrainBatchOp",
            "description": "BisectingKMeansTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.BisectingKMeansTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.BisectingKMeansTrainBatchOp",
            "params": [
              "minDivisibleClusterSize",
              "k",
              "distanceType",
              "vectorCol",
              "maxIter",
              "randomSeed"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GeoKMeansPredictBatchOp",
            "description": "GeoKMeansPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.GeoKMeansPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.GeoKMeansPredictBatchOp",
            "params": [
              "predictionDistanceCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GeoKMeansTrainBatchOp",
            "description": "GeoKMeansTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.GeoKMeansTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.GeoKMeansTrainBatchOp",
            "params": [
              "maxIter",
              "initMode",
              "initSteps",
              "latitudeCol",
              "longitudeCol",
              "k",
              "epsilon",
              "randomSeed"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GmmModelInfoBatchOp",
            "description": "GmmModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.GmmModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.GmmModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GmmPredictBatchOp",
            "description": "GmmPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.GmmPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.GmmPredictBatchOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GmmTrainBatchOp",
            "description": "GmmTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.GmmTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.GmmTrainBatchOp",
            "params": [
              "vectorCol",
              "k",
              "maxIter",
              "epsilon",
              "randomSeed"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KMeansModelInfoBatchOp",
            "description": "KMeansModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.KMeansModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.KMeansModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KMeansPredictBatchOp",
            "description": "KMeansPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.KMeansPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.KMeansPredictBatchOp",
            "params": [
              "predictionDistanceCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KMeansTrainBatchOp",
            "description": "KMeansTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp",
            "params": [
              "distanceType",
              "vectorCol",
              "maxIter",
              "initMode",
              "initSteps",
              "k",
              "epsilon",
              "randomSeed"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LdaModelInfoBatchOp",
            "description": "LdaModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.LdaModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.LdaModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LdaPredictBatchOp",
            "description": "LdaPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.LdaPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.LdaPredictBatchOp",
            "params": [
              "selectedCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LdaTrainBatchOp",
            "description": "LdaTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.clustering.LdaTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.clustering.LdaTrainBatchOp",
            "params": [
              "topicNum",
              "alpha",
              "beta",
              "method",
              "onlineLearningOffset",
              "learningDecay",
              "subsamplingRate",
              "optimizeDocConcentration",
              "numIter",
              "vocabSize",
              "selectedCol",
              "randomSeed"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.clustering"
      },
      {
        "isDir": true,
        "name": "关联规则",
        "children": [
          {
            "name": "FpGrowthBatchOp",
            "description": "FpGrowthBatchOp",
            "id": "com.alibaba.alink.operator.batch.associationrule.FpGrowthBatchOp",
            "className": "com.alibaba.alink.operator.batch.associationrule.FpGrowthBatchOp",
            "params": [
              "itemsCol",
              "minSupportCount",
              "minSupportPercent",
              "minConfidence",
              "maxPatternLength",
              "maxConsequentLength",
              "minLift"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "PrefixSpanBatchOp",
            "description": "PrefixSpanBatchOp",
            "id": "com.alibaba.alink.operator.batch.associationrule.PrefixSpanBatchOp",
            "className": "com.alibaba.alink.operator.batch.associationrule.PrefixSpanBatchOp",
            "params": [
              "itemsCol",
              "minSupportCount",
              "minSupportPercent",
              "minConfidence",
              "maxPatternLength"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.associationrule"
      },
      {
        "isDir": true,
        "name": "推荐",
        "children": [
          {
            "name": "AlsImplicitTrainBatchOp",
            "description": "AlsImplicitTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.AlsImplicitTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.AlsImplicitTrainBatchOp",
            "params": [
              "alpha",
              "rank",
              "lambda",
              "nonnegative",
              "numBlocks",
              "userCol",
              "itemCol",
              "rateCol",
              "numIter"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AlsItemsPerUserRecommBatchOp",
            "description": "AlsItemsPerUserRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.AlsItemsPerUserRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.AlsItemsPerUserRecommBatchOp",
            "params": [
              "userCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AlsModelInfoBatchOp",
            "description": "AlsModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.AlsModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.AlsModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AlsRateRecommBatchOp",
            "description": "AlsRateRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.AlsRateRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.AlsRateRecommBatchOp",
            "params": [
              "userCol",
              "itemCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AlsSimilarItemsRecommBatchOp",
            "description": "AlsSimilarItemsRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.AlsSimilarItemsRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.AlsSimilarItemsRecommBatchOp",
            "params": [
              "itemCol",
              "k",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AlsSimilarUsersRecommBatchOp",
            "description": "AlsSimilarUsersRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.AlsSimilarUsersRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.AlsSimilarUsersRecommBatchOp",
            "params": [
              "userCol",
              "k",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AlsTrainBatchOp",
            "description": "AlsTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.AlsTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.AlsTrainBatchOp",
            "params": [
              "rank",
              "lambda",
              "nonnegative",
              "numBlocks",
              "userCol",
              "itemCol",
              "rateCol",
              "numIter"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AlsUsersPerItemRecommBatchOp",
            "description": "AlsUsersPerItemRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.AlsUsersPerItemRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.AlsUsersPerItemRecommBatchOp",
            "params": [
              "itemCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FlattenKObjectBatchOp",
            "description": "FlattenKObjectBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.FlattenKObjectBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.FlattenKObjectBatchOp",
            "params": [
              "selectedCol",
              "outputCols",
              "outputColTypes",
              "reservedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmItemsPerUserRecommBatchOp",
            "description": "FmItemsPerUserRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.FmItemsPerUserRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.FmItemsPerUserRecommBatchOp",
            "params": [
              "userCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmRateRecommBatchOp",
            "description": "FmRateRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.FmRateRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.FmRateRecommBatchOp",
            "params": [
              "userCol",
              "itemCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmRecommBinaryImplicitTrainBatchOp",
            "description": "FmRecommBinaryImplicitTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.FmRecommBinaryImplicitTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.FmRecommBinaryImplicitTrainBatchOp",
            "params": [
              "userFeatureCols",
              "userCategoricalFeatureCols",
              "itemFeatureCols",
              "itemCategoricalFeatureCols",
              "rateCol",
              "userCol",
              "itemCol",
              "withIntercept",
              "withLinearItem",
              "numFactor",
              "lambda0",
              "lambda1",
              "lambda2",
              "numEpochs",
              "learnRate",
              "initStdev"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmRecommTrainBatchOp",
            "description": "FmRecommTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.FmRecommTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.FmRecommTrainBatchOp",
            "params": [
              "userFeatureCols",
              "userCategoricalFeatureCols",
              "itemFeatureCols",
              "itemCategoricalFeatureCols",
              "rateCol",
              "userCol",
              "itemCol",
              "withIntercept",
              "withLinearItem",
              "numFactor",
              "lambda0",
              "lambda1",
              "lambda2",
              "numEpochs",
              "learnRate",
              "initStdev"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmUsersPerItemRecommBatchOp",
            "description": "FmUsersPerItemRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.FmUsersPerItemRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.FmUsersPerItemRecommBatchOp",
            "params": [
              "itemCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ItemCfItemsPerUserRecommBatchOp",
            "description": "ItemCfItemsPerUserRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.ItemCfItemsPerUserRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.ItemCfItemsPerUserRecommBatchOp",
            "params": [
              "userCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ItemCfModelInfoBatchOp",
            "description": "ItemCfModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.ItemCfModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.ItemCfModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ItemCfRateRecommBatchOp",
            "description": "ItemCfRateRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.ItemCfRateRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.ItemCfRateRecommBatchOp",
            "params": [
              "userCol",
              "itemCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ItemCfSimilarItemsRecommBatchOp",
            "description": "ItemCfSimilarItemsRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.ItemCfSimilarItemsRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.ItemCfSimilarItemsRecommBatchOp",
            "params": [
              "itemCol",
              "k",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ItemCfTrainBatchOp",
            "description": "ItemCfTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.ItemCfTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.ItemCfTrainBatchOp",
            "params": [
              "maxNeighborNumber",
              "userCol",
              "itemCol",
              "similarityType",
              "rateCol",
              "similarityThreshold"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ItemCfUsersPerItemRecommBatchOp",
            "description": "ItemCfUsersPerItemRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.ItemCfUsersPerItemRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.ItemCfUsersPerItemRecommBatchOp",
            "params": [
              "itemCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LeaveKObjectOutBatchOp",
            "description": "LeaveKObjectOutBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.LeaveKObjectOutBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.LeaveKObjectOutBatchOp",
            "params": [
              "groupCol",
              "objectCol",
              "fraction",
              "k",
              "outputCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LeaveTopKObjectOutBatchOp",
            "description": "LeaveTopKObjectOutBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.LeaveTopKObjectOutBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.LeaveTopKObjectOutBatchOp",
            "params": [
              "rateThreshold",
              "rateCol",
              "groupCol",
              "objectCol",
              "fraction",
              "k",
              "outputCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "NegativeItemSamplingBatchOp",
            "description": "NegativeItemSamplingBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.NegativeItemSamplingBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.NegativeItemSamplingBatchOp",
            "params": [
              "samplingFactor"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SwingRecommBatchOp",
            "description": "SwingRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.SwingRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.SwingRecommBatchOp",
            "params": [
              "itemCol",
              "k",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SwingTrainBatchOp",
            "description": "SwingTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.SwingTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.SwingTrainBatchOp",
            "params": [
              "userAlpha",
              "userBeta",
              "resultNormalize",
              "maxItemNumber",
              "minUserItems",
              "maxUserItems",
              "alpha",
              "rateCol",
              "userCol",
              "itemCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UserCfItemsPerUserRecommBatchOp",
            "description": "UserCfItemsPerUserRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.UserCfItemsPerUserRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.UserCfItemsPerUserRecommBatchOp",
            "params": [
              "userCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UserCfModelInfoBatchOp",
            "description": "UserCfModelInfoBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.UserCfModelInfoBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.UserCfModelInfoBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UserCfRateRecommBatchOp",
            "description": "UserCfRateRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.UserCfRateRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.UserCfRateRecommBatchOp",
            "params": [
              "userCol",
              "itemCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UserCfSimilarUsersRecommBatchOp",
            "description": "UserCfSimilarUsersRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.UserCfSimilarUsersRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.UserCfSimilarUsersRecommBatchOp",
            "params": [
              "userCol",
              "k",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UserCfTrainBatchOp",
            "description": "UserCfTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.UserCfTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.UserCfTrainBatchOp",
            "params": [
              "similarityThreshold",
              "k",
              "userCol",
              "itemCol",
              "similarityType",
              "rateCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UserCfUsersPerItemRecommBatchOp",
            "description": "UserCfUsersPerItemRecommBatchOp",
            "id": "com.alibaba.alink.operator.batch.recommendation.UserCfUsersPerItemRecommBatchOp",
            "className": "com.alibaba.alink.operator.batch.recommendation.UserCfUsersPerItemRecommBatchOp",
            "params": [
              "itemCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.recommendation"
      },
      {
        "isDir": true,
        "name": "图",
        "children": [
          {
            "name": "LineBatchOp",
            "description": "LineBatchOp",
            "id": "com.alibaba.alink.operator.batch.graph.LineBatchOp",
            "className": "com.alibaba.alink.operator.batch.graph.LineBatchOp",
            "params": [
              "order",
              "rho",
              "sampleRatioPerPartition",
              "minRhoRate",
              "sourceCol",
              "targetCol",
              "isToUndigraph",
              "vectorSize",
              "weightCol",
              "maxIter",
              "negative",
              "numThreads",
              "batchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MetaPathWalkBatchOp",
            "description": "MetaPathWalkBatchOp",
            "id": "com.alibaba.alink.operator.batch.graph.MetaPathWalkBatchOp",
            "className": "com.alibaba.alink.operator.batch.graph.MetaPathWalkBatchOp",
            "params": [
              "metaPath",
              "typeCol",
              "vertexCol",
              "samplingMethod",
              "sourceCol",
              "targetCol",
              "delimiter",
              "weightCol",
              "walkLength",
              "walkNum",
              "isToUndigraph"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "Node2VecWalkBatchOp",
            "description": "Node2VecWalkBatchOp",
            "id": "com.alibaba.alink.operator.batch.graph.Node2VecWalkBatchOp",
            "className": "com.alibaba.alink.operator.batch.graph.Node2VecWalkBatchOp",
            "params": [
              "p",
              "q",
              "samplingMethod",
              "sourceCol",
              "targetCol",
              "delimiter",
              "weightCol",
              "walkLength",
              "walkNum",
              "isToUndigraph"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RandomWalkBatchOp",
            "description": "RandomWalkBatchOp",
            "id": "com.alibaba.alink.operator.batch.graph.RandomWalkBatchOp",
            "className": "com.alibaba.alink.operator.batch.graph.RandomWalkBatchOp",
            "params": [
              "isWeightedSampling",
              "samplingMethod",
              "sourceCol",
              "targetCol",
              "delimiter",
              "weightCol",
              "walkLength",
              "walkNum",
              "isToUndigraph"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.graph"
      },
      {
        "isDir": true,
        "name": "评估",
        "children": [
          {
            "name": "EvalBinaryClassBatchOp",
            "description": "EvalBinaryClassBatchOp",
            "id": "com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp",
            "className": "com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp",
            "params": [
              "predictionDetailCol",
              "labelCol",
              "positiveLabelValueString"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "EvalClusterBatchOp",
            "description": "EvalClusterBatchOp",
            "id": "com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp",
            "className": "com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp",
            "params": [
              "labelCol",
              "vectorCol",
              "predictionCol",
              "distanceType"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "EvalMultiClassBatchOp",
            "description": "EvalMultiClassBatchOp",
            "id": "com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp",
            "className": "com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp",
            "params": [
              "predictionCol",
              "labelCol",
              "predictionDetailCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "EvalMultiLabelBatchOp",
            "description": "EvalMultiLabelBatchOp",
            "id": "com.alibaba.alink.operator.batch.evaluation.EvalMultiLabelBatchOp",
            "className": "com.alibaba.alink.operator.batch.evaluation.EvalMultiLabelBatchOp",
            "params": [
              "labelCol",
              "predictionRankingInfo",
              "labelRankingInfo",
              "predictionCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "EvalRankingBatchOp",
            "description": "EvalRankingBatchOp",
            "id": "com.alibaba.alink.operator.batch.evaluation.EvalRankingBatchOp",
            "className": "com.alibaba.alink.operator.batch.evaluation.EvalRankingBatchOp",
            "params": [
              "labelCol",
              "predictionRankingInfo",
              "labelRankingInfo",
              "predictionCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "EvalRegressionBatchOp",
            "description": "EvalRegressionBatchOp",
            "id": "com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp",
            "className": "com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp",
            "params": [
              "labelCol",
              "predictionCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "EvalTimeSeriesBatchOp",
            "description": "EvalTimeSeriesBatchOp",
            "id": "com.alibaba.alink.operator.batch.evaluation.EvalTimeSeriesBatchOp",
            "className": "com.alibaba.alink.operator.batch.evaluation.EvalTimeSeriesBatchOp",
            "params": [
              "labelCol",
              "predictionCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.evaluation"
      },
      {
        "isDir": true,
        "name": "超大规模算法",
        "children": [
          {
            "name": "HugeDeepWalkTrainBatchOp",
            "description": "HugeDeepWalkTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.huge.HugeDeepWalkTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.huge.HugeDeepWalkTrainBatchOp",
            "params": [
              "numCheckpoint",
              "sourceCol",
              "targetCol",
              "weightCol",
              "walkNum",
              "walkLength",
              "isToUndigraph",
              "numIter",
              "vectorSize",
              "alpha",
              "minCount",
              "negative",
              "randomWindow",
              "window",
              "batchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "HugeLabeledWord2VecTrainBatchOp",
            "description": "HugeLabeledWord2VecTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.huge.HugeLabeledWord2VecTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.huge.HugeLabeledWord2VecTrainBatchOp",
            "params": [
              "numCheckpoint",
              "vertexCol",
              "typeCol",
              "numIter",
              "selectedCol",
              "vectorSize",
              "alpha",
              "wordDelimiter",
              "minCount",
              "negative",
              "randomWindow",
              "window",
              "batchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "HugeMetaPath2VecTrainBatchOp",
            "description": "HugeMetaPath2VecTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.huge.HugeMetaPath2VecTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.huge.HugeMetaPath2VecTrainBatchOp",
            "params": [
              "numCheckpoint",
              "sourceCol",
              "targetCol",
              "weightCol",
              "walkNum",
              "walkLength",
              "isToUndigraph",
              "metaPath",
              "vertexCol",
              "typeCol",
              "mode",
              "numIter",
              "vectorSize",
              "alpha",
              "minCount",
              "negative",
              "randomWindow",
              "window",
              "batchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "HugeNode2VecTrainBatchOp",
            "description": "HugeNode2VecTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.huge.HugeNode2VecTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.huge.HugeNode2VecTrainBatchOp",
            "params": [
              "numCheckpoint",
              "sourceCol",
              "targetCol",
              "weightCol",
              "p",
              "q",
              "walkNum",
              "walkLength",
              "isToUndigraph",
              "numIter",
              "vectorSize",
              "alpha",
              "wordDelimiter",
              "minCount",
              "negative",
              "randomWindow",
              "window",
              "batchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "HugeWord2VecTrainBatchOp",
            "description": "HugeWord2VecTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.huge.HugeWord2VecTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.huge.HugeWord2VecTrainBatchOp",
            "params": [
              "numCheckpoint",
              "metapathMode",
              "numIter",
              "selectedCol",
              "vectorSize",
              "alpha",
              "wordDelimiter",
              "minCount",
              "negative",
              "randomWindow",
              "window",
              "batchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.huge"
      },
      {
        "isDir": true,
        "name": "异常检测",
        "children": [
          {
            "name": "IsolationForestsPredictBatchOp",
            "description": "IsolationForestsPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.outlier.IsolationForestsPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.outlier.IsolationForestsPredictBatchOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "IsolationForestsTrainBatchOp",
            "description": "IsolationForestsTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.outlier.IsolationForestsTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.outlier.IsolationForestsTrainBatchOp",
            "params": [
              "featureCols",
              "maxLeaves",
              "minSampleRatioPerChild",
              "maxDepth",
              "minSamplesPerLeaf",
              "numTrees",
              "subsamplingRatio",
              "seed"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SosBatchOp",
            "description": "SosBatchOp",
            "id": "com.alibaba.alink.operator.batch.outlier.SosBatchOp",
            "className": "com.alibaba.alink.operator.batch.outlier.SosBatchOp",
            "params": [
              "perplexity",
              "vectorCol",
              "predictionCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.outlier"
      },
      {
        "isDir": true,
        "name": "相似度",
        "children": [
          {
            "name": "StringApproxNearestNeighborPredictBatchOp",
            "description": "StringApproxNearestNeighborPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.StringApproxNearestNeighborPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.StringApproxNearestNeighborPredictBatchOp",
            "params": [
              "radius",
              "topN",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StringApproxNearestNeighborTrainBatchOp",
            "description": "StringApproxNearestNeighborTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.StringApproxNearestNeighborTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.StringApproxNearestNeighborTrainBatchOp",
            "params": [
              "metric",
              "trainType",
              "idCol",
              "selectedCol",
              "numBucket",
              "numHashTables",
              "seed"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StringNearestNeighborPredictBatchOp",
            "description": "StringNearestNeighborPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.StringNearestNeighborPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.StringNearestNeighborPredictBatchOp",
            "params": [
              "radius",
              "topN",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StringNearestNeighborTrainBatchOp",
            "description": "StringNearestNeighborTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.StringNearestNeighborTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.StringNearestNeighborTrainBatchOp",
            "params": [
              "metric",
              "trainType",
              "lambda",
              "idCol",
              "selectedCol",
              "windowSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StringSimilarityPairwiseBatchOp",
            "description": "StringSimilarityPairwiseBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.StringSimilarityPairwiseBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.StringSimilarityPairwiseBatchOp",
            "params": [
              "lambda",
              "metric",
              "windowSize",
              "numBucket",
              "numHashTables",
              "seed",
              "selectedCols",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TextApproxNearestNeighborPredictBatchOp",
            "description": "TextApproxNearestNeighborPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.TextApproxNearestNeighborPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.TextApproxNearestNeighborPredictBatchOp",
            "params": [
              "radius",
              "topN",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TextApproxNearestNeighborTrainBatchOp",
            "description": "TextApproxNearestNeighborTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.TextApproxNearestNeighborTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.TextApproxNearestNeighborTrainBatchOp",
            "params": [
              "metric",
              "trainType",
              "idCol",
              "selectedCol",
              "numBucket",
              "numHashTables",
              "seed"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TextNearestNeighborPredictBatchOp",
            "description": "TextNearestNeighborPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.TextNearestNeighborPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.TextNearestNeighborPredictBatchOp",
            "params": [
              "radius",
              "topN",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TextNearestNeighborTrainBatchOp",
            "description": "TextNearestNeighborTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.TextNearestNeighborTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.TextNearestNeighborTrainBatchOp",
            "params": [
              "metric",
              "trainType",
              "lambda",
              "idCol",
              "selectedCol",
              "windowSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TextSimilarityPairwiseBatchOp",
            "description": "TextSimilarityPairwiseBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.TextSimilarityPairwiseBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.TextSimilarityPairwiseBatchOp",
            "params": [
              "lambda",
              "metric",
              "windowSize",
              "numBucket",
              "numHashTables",
              "seed",
              "selectedCols",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "VectorApproxNearestNeighborPredictBatchOp",
            "description": "VectorApproxNearestNeighborPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.VectorApproxNearestNeighborPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.VectorApproxNearestNeighborPredictBatchOp",
            "params": [
              "radius",
              "topN",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "VectorApproxNearestNeighborTrainBatchOp",
            "description": "VectorApproxNearestNeighborTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.VectorApproxNearestNeighborTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.VectorApproxNearestNeighborTrainBatchOp",
            "params": [
              "metric",
              "solver",
              "trainType",
              "idCol",
              "selectedCol",
              "numHashTables",
              "numProjectionsPerTable",
              "seed",
              "projectionWidth"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "VectorNearestNeighborPredictBatchOp",
            "description": "VectorNearestNeighborPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborPredictBatchOp",
            "params": [
              "radius",
              "topN",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "VectorNearestNeighborTrainBatchOp",
            "description": "VectorNearestNeighborTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborTrainBatchOp",
            "params": [
              "trainType",
              "metric",
              "idCol",
              "selectedCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.similarity"
      },
      {
        "isDir": true,
        "name": "工具类",
        "children": [
          {
            "name": "FlatModelMapBatchOp",
            "description": "FlatModelMapBatchOp",
            "id": "com.alibaba.alink.operator.batch.utils.FlatModelMapBatchOp",
            "className": "com.alibaba.alink.operator.batch.utils.FlatModelMapBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MTableSerializeBatchOp",
            "description": "MTableSerializeBatchOp",
            "id": "com.alibaba.alink.operator.batch.utils.MTableSerializeBatchOp",
            "className": "com.alibaba.alink.operator.batch.utils.MTableSerializeBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "PrintBatchOp",
            "description": "PrintBatchOp",
            "id": "com.alibaba.alink.operator.batch.utils.PrintBatchOp",
            "className": "com.alibaba.alink.operator.batch.utils.PrintBatchOp",
            "params": [],
            "numInPorts": 1,
            "numOutPorts": 0
          },
          {
            "name": "TensorSerializeBatchOp",
            "description": "TensorSerializeBatchOp",
            "id": "com.alibaba.alink.operator.batch.utils.TensorSerializeBatchOp",
            "className": "com.alibaba.alink.operator.batch.utils.TensorSerializeBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UDFBatchOp",
            "description": "UDFBatchOp",
            "id": "com.alibaba.alink.operator.batch.utils.UDFBatchOp",
            "className": "com.alibaba.alink.operator.batch.utils.UDFBatchOp",
            "params": [
              "funcName",
              "selectedCols",
              "outputCol",
              "reservedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UDTFBatchOp",
            "description": "UDTFBatchOp",
            "id": "com.alibaba.alink.operator.batch.utils.UDTFBatchOp",
            "className": "com.alibaba.alink.operator.batch.utils.UDTFBatchOp",
            "params": [
              "funcName",
              "selectedCols",
              "outputCols",
              "reservedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "VectorSerializeBatchOp",
            "description": "VectorSerializeBatchOp",
            "id": "com.alibaba.alink.operator.batch.utils.VectorSerializeBatchOp",
            "className": "com.alibaba.alink.operator.batch.utils.VectorSerializeBatchOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.utils"
      },
      {
        "isDir": true,
        "name": "audio",
        "children": [
          {
            "name": "ExtractMfccFeatureBatchOp",
            "description": "ExtractMfccFeatureBatchOp",
            "id": "com.alibaba.alink.operator.batch.audio.ExtractMfccFeatureBatchOp",
            "className": "com.alibaba.alink.operator.batch.audio.ExtractMfccFeatureBatchOp",
            "params": [
              "windowSecond",
              "hopSecond",
              "nMfcc",
              "sampleRate",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ReadAudioToTensorBatchOp",
            "description": "ReadAudioToTensorBatchOp",
            "id": "com.alibaba.alink.operator.batch.audio.ReadAudioToTensorBatchOp",
            "className": "com.alibaba.alink.operator.batch.audio.ReadAudioToTensorBatchOp",
            "params": [
              "durationTime",
              "startTime",
              "sampleRate",
              "relativeFilePathCol",
              "rootFilePath",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.audio"
      },
      {
        "isDir": true,
        "name": "image",
        "children": [
          {
            "name": "ReadImageToTensorBatchOp",
            "description": "ReadImageToTensorBatchOp",
            "id": "com.alibaba.alink.operator.batch.image.ReadImageToTensorBatchOp",
            "className": "com.alibaba.alink.operator.batch.image.ReadImageToTensorBatchOp",
            "params": [
              "imageWidth",
              "imageHeight",
              "rootFilePath",
              "outputCol",
              "relativeFilePathCol",
              "reservedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "WriteTensorToImageBatchOp",
            "description": "WriteTensorToImageBatchOp",
            "id": "com.alibaba.alink.operator.batch.image.WriteTensorToImageBatchOp",
            "className": "com.alibaba.alink.operator.batch.image.WriteTensorToImageBatchOp",
            "params": [
              "rootFilePath",
              "tensorCol",
              "relativeFilePathCol",
              "reservedCols",
              "imageType"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.image"
      },
      {
        "isDir": true,
        "name": "tensorflow",
        "children": [
          {
            "name": "TF2TableModelTrainBatchOp",
            "description": "TF2TableModelTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.tensorflow.TF2TableModelTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.tensorflow.TF2TableModelTrainBatchOp",
            "params": [
              "numPSs",
              "numWorkers",
              "selectedCols",
              "pythonEnv",
              "intraOpParallelism",
              "userFiles",
              "mainScriptFile",
              "userParams"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TFSavedModelPredictBatchOp",
            "description": "TFSavedModelPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.tensorflow.TFSavedModelPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.tensorflow.TFSavedModelPredictBatchOp",
            "params": [
              "modelPath",
              "reservedCols",
              "graphDefTag",
              "signatureDefKey",
              "inputSignatureDefs",
              "outputSignatureDefs",
              "selectedCols",
              "outputSchemaStr"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TFTableModelPredictBatchOp",
            "description": "TFTableModelPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.tensorflow.TFTableModelPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.tensorflow.TFTableModelPredictBatchOp",
            "params": [
              "reservedCols",
              "graphDefTag",
              "signatureDefKey",
              "inputSignatureDefs",
              "outputSignatureDefs",
              "selectedCols",
              "outputSchemaStr"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TFTableModelTrainBatchOp",
            "description": "TFTableModelTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.tensorflow.TFTableModelTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.tensorflow.TFTableModelTrainBatchOp",
            "params": [
              "numPSs",
              "numWorkers",
              "selectedCols",
              "pythonEnv",
              "intraOpParallelism",
              "userFiles",
              "mainScriptFile",
              "userParams"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TensorFlow2BatchOp",
            "description": "TensorFlow2BatchOp",
            "id": "com.alibaba.alink.operator.batch.tensorflow.TensorFlow2BatchOp",
            "className": "com.alibaba.alink.operator.batch.tensorflow.TensorFlow2BatchOp",
            "params": [
              "numPSs",
              "numWorkers",
              "outputSchemaStr",
              "selectedCols",
              "pythonEnv",
              "intraOpParallelism",
              "userFiles",
              "mainScriptFile",
              "userParams"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TensorFlowBatchOp",
            "description": "TensorFlowBatchOp",
            "id": "com.alibaba.alink.operator.batch.tensorflow.TensorFlowBatchOp",
            "className": "com.alibaba.alink.operator.batch.tensorflow.TensorFlowBatchOp",
            "params": [
              "numPSs",
              "numWorkers",
              "outputSchemaStr",
              "selectedCols",
              "pythonEnv",
              "intraOpParallelism",
              "userFiles",
              "mainScriptFile",
              "userParams"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.tensorflow"
      },
      {
        "isDir": true,
        "name": "时间序列",
        "children": [
          {
            "name": "ArimaBatchOp",
            "description": "ArimaBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.ArimaBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.ArimaBatchOp",
            "params": [
              "predictNum",
              "order",
              "seasonalOrder",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "estMethod",
              "seasonalPeriod",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AutoArimaBatchOp",
            "description": "AutoArimaBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.AutoArimaBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.AutoArimaBatchOp",
            "params": [
              "d",
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "estMethod",
              "icType",
              "maxOrder",
              "maxSeasonalOrder",
              "seasonalPeriod",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AutoGarchBatchOp",
            "description": "AutoGarchBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.AutoGarchBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.AutoGarchBatchOp",
            "params": [
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "icType",
              "maxOrder",
              "ifGARCH11",
              "minusMean",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DeepARPreProcessBatchOp",
            "description": "DeepARPreProcessBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.DeepARPreProcessBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.DeepARPreProcessBatchOp",
            "params": [
              "timeCol",
              "selectedCol",
              "vectorCol",
              "window",
              "stride",
              "outputCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DeepARPredictBatchOp",
            "description": "DeepARPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.DeepARPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.DeepARPredictBatchOp",
            "params": [
              "timeCol",
              "selectedCol",
              "vectorCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DeepARTrainBatchOp",
            "description": "DeepARTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.DeepARTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.DeepARTrainBatchOp",
            "params": [
              "timeCol",
              "selectedCol",
              "vectorCol",
              "window",
              "stride",
              "learningRate",
              "numEpochs",
              "batchSize",
              "checkpointFilePath",
              "pythonEnv",
              "intraOpParallelism",
              "numWorkers",
              "numPSs",
              "removeCheckpointBeforeTraining"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "HoltWintersBatchOp",
            "description": "HoltWintersBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.HoltWintersBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.HoltWintersBatchOp",
            "params": [
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "alpha",
              "beta",
              "gamma",
              "frequency",
              "doTrend",
              "doSeasonal",
              "seasonalType",
              "levelStart",
              "trendStart",
              "seasonalStart",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LSTNetPreProcessBatchOp",
            "description": "LSTNetPreProcessBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.LSTNetPreProcessBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.LSTNetPreProcessBatchOp",
            "params": [
              "timeCol",
              "selectedCol",
              "vectorCol",
              "window",
              "horizon",
              "outputCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LSTNetPredictBatchOp",
            "description": "LSTNetPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.LSTNetPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.LSTNetPredictBatchOp",
            "params": [
              "selectedCol",
              "predictionCol",
              "reservedCols",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LSTNetTrainBatchOp",
            "description": "LSTNetTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.LSTNetTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.LSTNetTrainBatchOp",
            "params": [
              "timeCol",
              "selectedCol",
              "vectorCol",
              "window",
              "horizon",
              "learningRate",
              "numEpochs",
              "batchSize",
              "checkpointFilePath",
              "pythonEnv",
              "intraOpParallelism",
              "numWorkers",
              "numPSs",
              "removeCheckpointBeforeTraining"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LookupValueInTimeSeriesBatchOp",
            "description": "LookupValueInTimeSeriesBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.LookupValueInTimeSeriesBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.LookupValueInTimeSeriesBatchOp",
            "params": [
              "timeSeriesCol",
              "timeCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LookupVectorInTimeSeriesBatchOp",
            "description": "LookupVectorInTimeSeriesBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.LookupVectorInTimeSeriesBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.LookupVectorInTimeSeriesBatchOp",
            "params": [
              "timeSeriesCol",
              "timeCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ProphetBatchOp",
            "description": "ProphetBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.ProphetBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.ProphetBatchOp",
            "params": [
              "uncertaintySamples",
              "stanInit",
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ProphetPredictBatchOp",
            "description": "ProphetPredictBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.ProphetPredictBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.ProphetPredictBatchOp",
            "params": [
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ProphetTrainBatchOp",
            "description": "ProphetTrainBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.ProphetTrainBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.ProphetTrainBatchOp",
            "params": [
              "valueCol",
              "timeCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ShiftBatchOp",
            "description": "ShiftBatchOp",
            "id": "com.alibaba.alink.operator.batch.timeseries.ShiftBatchOp",
            "className": "com.alibaba.alink.operator.batch.timeseries.ShiftBatchOp",
            "params": [
              "shiftNum",
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "batch.timeseries"
      }
    ],
    "id": "batch"
  },
  {
    "isDir": true,
    "name": "流组件",
    "children": [
      {
        "isDir": true,
        "name": "数据导入",
        "children": [
          {
            "name": "AkSourceStreamOp",
            "description": "AkSourceStreamOp",
            "id": "com.alibaba.alink.operator.stream.source.AkSourceStreamOp",
            "className": "com.alibaba.alink.operator.stream.source.AkSourceStreamOp",
            "params": [
              "filePath"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CatalogSourceStreamOp",
            "description": "CatalogSourceStreamOp",
            "id": "com.alibaba.alink.operator.stream.source.CatalogSourceStreamOp",
            "className": "com.alibaba.alink.operator.stream.source.CatalogSourceStreamOp",
            "params": [
              "catalogObject"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CsvSourceStreamOp",
            "description": "CsvSourceStreamOp",
            "id": "com.alibaba.alink.operator.stream.source.CsvSourceStreamOp",
            "className": "com.alibaba.alink.operator.stream.source.CsvSourceStreamOp",
            "params": [
              "filePath",
              "schemaStr",
              "fieldDelimiter",
              "quoteChar",
              "skipBlankLine",
              "rowDelimiter",
              "ignoreFirstLine",
              "lenient"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KafkaSourceStreamOp",
            "description": "KafkaSourceStreamOp",
            "id": "com.alibaba.alink.operator.stream.source.KafkaSourceStreamOp",
            "className": "com.alibaba.alink.operator.stream.source.KafkaSourceStreamOp",
            "params": [
              "bootstrapServers",
              "groupId",
              "startupMode",
              "topic",
              "topicPattern",
              "startTime",
              "properties"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LibSvmSourceStreamOp",
            "description": "LibSvmSourceStreamOp",
            "id": "com.alibaba.alink.operator.stream.source.LibSvmSourceStreamOp",
            "className": "com.alibaba.alink.operator.stream.source.LibSvmSourceStreamOp",
            "params": [
              "filePath",
              "startIndex"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MemSourceStreamOp",
            "description": "MemSourceStreamOp",
            "id": "com.alibaba.alink.operator.stream.source.MemSourceStreamOp",
            "className": "com.alibaba.alink.operator.stream.source.MemSourceStreamOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ModelStreamFileSourceStreamOp",
            "description": "ModelStreamFileSourceStreamOp",
            "id": "com.alibaba.alink.operator.stream.source.ModelStreamFileSourceStreamOp",
            "className": "com.alibaba.alink.operator.stream.source.ModelStreamFileSourceStreamOp",
            "params": [
              "filePath",
              "startTime",
              "schemaStr",
              "scanInterval"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "NumSeqSourceStreamOp",
            "description": "NumSeqSourceStreamOp",
            "id": "com.alibaba.alink.operator.stream.source.NumSeqSourceStreamOp",
            "className": "com.alibaba.alink.operator.stream.source.NumSeqSourceStreamOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RandomTableSourceStreamOp",
            "description": "RandomTableSourceStreamOp",
            "id": "com.alibaba.alink.operator.stream.source.RandomTableSourceStreamOp",
            "className": "com.alibaba.alink.operator.stream.source.RandomTableSourceStreamOp",
            "params": [
              "idCol",
              "outputColConfs",
              "outputCols",
              "numCols",
              "maxRows",
              "timePerSample",
              "timeZones"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RandomVectorSourceStreamOp",
            "description": "RandomVectorSourceStreamOp",
            "id": "com.alibaba.alink.operator.stream.source.RandomVectorSourceStreamOp",
            "className": "com.alibaba.alink.operator.stream.source.RandomVectorSourceStreamOp",
            "params": [
              "idCol",
              "outputCol",
              "size",
              "maxRows",
              "sparsity",
              "timePerSample"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TableSourceStreamOp",
            "description": "TableSourceStreamOp",
            "id": "com.alibaba.alink.operator.stream.source.TableSourceStreamOp",
            "className": "com.alibaba.alink.operator.stream.source.TableSourceStreamOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TextSourceStreamOp",
            "description": "TextSourceStreamOp",
            "id": "com.alibaba.alink.operator.stream.source.TextSourceStreamOp",
            "className": "com.alibaba.alink.operator.stream.source.TextSourceStreamOp",
            "params": [
              "filePath",
              "ignoreFirstLine",
              "textCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TsvSourceStreamOp",
            "description": "TsvSourceStreamOp",
            "id": "com.alibaba.alink.operator.stream.source.TsvSourceStreamOp",
            "className": "com.alibaba.alink.operator.stream.source.TsvSourceStreamOp",
            "params": [
              "filePath",
              "schemaStr",
              "skipBlankLine",
              "ignoreFirstLine"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.source"
      },
      {
        "isDir": true,
        "name": "数据导出",
        "children": [
          {
            "name": "AkSinkStreamOp",
            "description": "AkSinkStreamOp",
            "id": "com.alibaba.alink.operator.stream.sink.AkSinkStreamOp",
            "className": "com.alibaba.alink.operator.stream.sink.AkSinkStreamOp",
            "params": [
              "filePath",
              "overwriteSink",
              "numFiles"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CatalogSinkStreamOp",
            "description": "CatalogSinkStreamOp",
            "id": "com.alibaba.alink.operator.stream.sink.CatalogSinkStreamOp",
            "className": "com.alibaba.alink.operator.stream.sink.CatalogSinkStreamOp",
            "params": [
              "catalogObject"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CsvSinkStreamOp",
            "description": "CsvSinkStreamOp",
            "id": "com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp",
            "className": "com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp",
            "params": [
              "filePath",
              "fieldDelimiter",
              "rowDelimiter",
              "quoteChar",
              "overwriteSink",
              "numFiles"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KafkaSinkStreamOp",
            "description": "KafkaSinkStreamOp",
            "id": "com.alibaba.alink.operator.stream.sink.KafkaSinkStreamOp",
            "className": "com.alibaba.alink.operator.stream.sink.KafkaSinkStreamOp",
            "params": [
              "bootstrapServers",
              "topic",
              "dataFormat",
              "fieldDelimiter",
              "properties"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LibSvmSinkStreamOp",
            "description": "LibSvmSinkStreamOp",
            "id": "com.alibaba.alink.operator.stream.sink.LibSvmSinkStreamOp",
            "className": "com.alibaba.alink.operator.stream.sink.LibSvmSinkStreamOp",
            "params": [
              "filePath",
              "overwriteSink",
              "vectorCol",
              "labelCol",
              "startIndex"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ModelStreamFileSinkStreamOp",
            "description": "ModelStreamFileSinkStreamOp",
            "id": "com.alibaba.alink.operator.stream.sink.ModelStreamFileSinkStreamOp",
            "className": "com.alibaba.alink.operator.stream.sink.ModelStreamFileSinkStreamOp",
            "params": [
              "numKeepModel",
              "filePath"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TextSinkStreamOp",
            "description": "TextSinkStreamOp",
            "id": "com.alibaba.alink.operator.stream.sink.TextSinkStreamOp",
            "className": "com.alibaba.alink.operator.stream.sink.TextSinkStreamOp",
            "params": [
              "filePath",
              "overwriteSink",
              "numFiles"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TsvSinkStreamOp",
            "description": "TsvSinkStreamOp",
            "id": "com.alibaba.alink.operator.stream.sink.TsvSinkStreamOp",
            "className": "com.alibaba.alink.operator.stream.sink.TsvSinkStreamOp",
            "params": [
              "filePath",
              "overwriteSink",
              "numFiles"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.sink"
      },
      {
        "isDir": true,
        "name": "数据处理",
        "children": [
          {
            "name": "AggLookupStreamOp",
            "description": "AggLookupStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.AggLookupStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.AggLookupStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "reservedCols",
              "delimiter",
              "clause",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FlattenMTableStreamOp",
            "description": "FlattenMTableStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.FlattenMTableStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.FlattenMTableStreamOp",
            "params": [
              "selectedCol",
              "schemaStr",
              "reservedCols",
              "handleInvalidMethod"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ImputerPredictStreamOp",
            "description": "ImputerPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.ImputerPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.ImputerPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "outputCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "IndexToStringPredictStreamOp",
            "description": "IndexToStringPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.IndexToStringPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.IndexToStringPredictStreamOp",
            "params": [
              "modelName",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCol",
              "reservedCols",
              "outputCol",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "JsonValueStreamOp",
            "description": "JsonValueStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp",
            "params": [
              "jsonPath",
              "skipFailed",
              "selectedCol",
              "reservedCols",
              "outputCols",
              "outputColTypes",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LookupStreamOp",
            "description": "LookupStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.LookupStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.LookupStreamOp",
            "params": [
              "mapKeyCols",
              "mapValueCols",
              "modelStreamUpdateMethod",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MaxAbsScalerPredictStreamOp",
            "description": "MaxAbsScalerPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.MaxAbsScalerPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.MaxAbsScalerPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "outputCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MinMaxScalerPredictStreamOp",
            "description": "MinMaxScalerPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.MinMaxScalerPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.MinMaxScalerPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "outputCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MultiStringIndexerPredictStreamOp",
            "description": "MultiStringIndexerPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.MultiStringIndexerPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.MultiStringIndexerPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "handleInvalid",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SampleStreamOp",
            "description": "SampleStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.SampleStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.SampleStreamOp",
            "params": [
              "ratio",
              "withReplacement"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SplitStreamOp",
            "description": "SplitStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.SplitStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.SplitStreamOp",
            "params": [
              "fraction",
              "randomSeed"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StandardScalerPredictStreamOp",
            "description": "StandardScalerPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.StandardScalerPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.StandardScalerPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "outputCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StratifiedSampleStreamOp",
            "description": "StratifiedSampleStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.StratifiedSampleStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.StratifiedSampleStreamOp",
            "params": [
              "strataCol",
              "strataRatio",
              "strataRatios"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StringIndexerPredictStreamOp",
            "description": "StringIndexerPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.StringIndexerPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.StringIndexerPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCol",
              "reservedCols",
              "handleInvalid",
              "outputCol",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TypeConvertStreamOp",
            "description": "TypeConvertStreamOp",
            "id": "com.alibaba.alink.operator.stream.dataproc.TypeConvertStreamOp",
            "className": "com.alibaba.alink.operator.stream.dataproc.TypeConvertStreamOp",
            "params": [
              "selectedCols",
              "targetType"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "isDir": true,
            "name": "数据格式转换",
            "children": [
              {
                "name": "ColumnsToCsvStreamOp",
                "description": "ColumnsToCsvStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.ColumnsToCsvStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.ColumnsToCsvStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar",
                  "selectedCols"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "ColumnsToJsonStreamOp",
                "description": "ColumnsToJsonStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.ColumnsToJsonStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.ColumnsToJsonStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "jsonCol",
                  "selectedCols"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "ColumnsToKvStreamOp",
                "description": "ColumnsToKvStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.ColumnsToKvStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.ColumnsToKvStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter",
                  "selectedCols"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "ColumnsToTripleStreamOp",
                "description": "ColumnsToTripleStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.ColumnsToTripleStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.ColumnsToTripleStreamOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnValueSchemaStr",
                  "reservedCols",
                  "selectedCols"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "ColumnsToVectorStreamOp",
                "description": "ColumnsToVectorStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.ColumnsToVectorStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.ColumnsToVectorStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "vectorCol",
                  "vectorSize",
                  "selectedCols"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "CsvToColumnsStreamOp",
                "description": "CsvToColumnsStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.CsvToColumnsStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.CsvToColumnsStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "schemaStr",
                  "csvCol",
                  "csvFieldDelimiter",
                  "quoteChar"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "CsvToJsonStreamOp",
                "description": "CsvToJsonStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.CsvToJsonStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.CsvToJsonStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "jsonCol",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "CsvToKvStreamOp",
                "description": "CsvToKvStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.CsvToKvStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.CsvToKvStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "CsvToTripleStreamOp",
                "description": "CsvToTripleStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.CsvToTripleStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.CsvToTripleStreamOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnValueSchemaStr",
                  "reservedCols",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "CsvToVectorStreamOp",
                "description": "CsvToVectorStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.CsvToVectorStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.CsvToVectorStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "vectorCol",
                  "vectorSize",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "JsonToColumnsStreamOp",
                "description": "JsonToColumnsStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.JsonToColumnsStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.JsonToColumnsStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "schemaStr",
                  "jsonCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "JsonToCsvStreamOp",
                "description": "JsonToCsvStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.JsonToCsvStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.JsonToCsvStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar",
                  "jsonCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "JsonToKvStreamOp",
                "description": "JsonToKvStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.JsonToKvStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.JsonToKvStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter",
                  "jsonCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "JsonToTripleStreamOp",
                "description": "JsonToTripleStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.JsonToTripleStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.JsonToTripleStreamOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnValueSchemaStr",
                  "reservedCols",
                  "jsonCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "JsonToVectorStreamOp",
                "description": "JsonToVectorStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.JsonToVectorStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.JsonToVectorStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "vectorCol",
                  "vectorSize",
                  "jsonCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "KvToColumnsStreamOp",
                "description": "KvToColumnsStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.KvToColumnsStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.KvToColumnsStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "schemaStr",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "KvToCsvStreamOp",
                "description": "KvToCsvStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.KvToCsvStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.KvToCsvStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "KvToJsonStreamOp",
                "description": "KvToJsonStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.KvToJsonStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.KvToJsonStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "jsonCol",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "KvToTripleStreamOp",
                "description": "KvToTripleStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.KvToTripleStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.KvToTripleStreamOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnValueSchemaStr",
                  "reservedCols",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "KvToVectorStreamOp",
                "description": "KvToVectorStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.KvToVectorStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.KvToVectorStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "vectorCol",
                  "vectorSize",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorToColumnsStreamOp",
                "description": "VectorToColumnsStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.VectorToColumnsStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.VectorToColumnsStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "schemaStr",
                  "vectorCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorToCsvStreamOp",
                "description": "VectorToCsvStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.VectorToCsvStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.VectorToCsvStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "csvCol",
                  "schemaStr",
                  "csvFieldDelimiter",
                  "quoteChar",
                  "vectorCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorToJsonStreamOp",
                "description": "VectorToJsonStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.VectorToJsonStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.VectorToJsonStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "jsonCol",
                  "vectorCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorToKvStreamOp",
                "description": "VectorToKvStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.VectorToKvStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.VectorToKvStreamOp",
                "params": [
                  "handleInvalid",
                  "reservedCols",
                  "kvCol",
                  "kvColDelimiter",
                  "kvValDelimiter",
                  "vectorCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorToTripleStreamOp",
                "description": "VectorToTripleStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.format.VectorToTripleStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.format.VectorToTripleStreamOp",
                "params": [
                  "handleInvalid",
                  "tripleColumnValueSchemaStr",
                  "reservedCols",
                  "vectorCol"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              }
            ],
            "id": "stream.dataproc.format"
          },
          {
            "isDir": true,
            "name": "张量",
            "children": [
              {
                "name": "TensorToVectorStreamOp",
                "description": "TensorToVectorStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.tensor.TensorToVectorStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.tensor.TensorToVectorStreamOp",
                "params": [
                  "convertMethod",
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorToTensorStreamOp",
                "description": "VectorToTensorStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.tensor.VectorToTensorStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.tensor.VectorToTensorStreamOp",
                "params": [
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              }
            ],
            "id": "stream.dataproc.tensor"
          },
          {
            "isDir": true,
            "name": "向量",
            "children": [
              {
                "name": "VectorAssemblerStreamOp",
                "description": "VectorAssemblerStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.vector.VectorAssemblerStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.vector.VectorAssemblerStreamOp",
                "params": [
                  "handleInvalidMethod",
                  "selectedCols",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorElementwiseProductStreamOp",
                "description": "VectorElementwiseProductStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.vector.VectorElementwiseProductStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.vector.VectorElementwiseProductStreamOp",
                "params": [
                  "scalingVector",
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorImputerPredictStreamOp",
                "description": "VectorImputerPredictStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.vector.VectorImputerPredictStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.vector.VectorImputerPredictStreamOp",
                "params": [
                  "modelStreamFilePath",
                  "modelStreamScanInterval",
                  "modelStreamStartTime",
                  "outputCol",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorInteractionStreamOp",
                "description": "VectorInteractionStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.vector.VectorInteractionStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.vector.VectorInteractionStreamOp",
                "params": [
                  "selectedCols",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorMaxAbsScalerPredictStreamOp",
                "description": "VectorMaxAbsScalerPredictStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.vector.VectorMaxAbsScalerPredictStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.vector.VectorMaxAbsScalerPredictStreamOp",
                "params": [
                  "modelStreamFilePath",
                  "modelStreamScanInterval",
                  "modelStreamStartTime",
                  "outputCol",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorMinMaxScalerPredictStreamOp",
                "description": "VectorMinMaxScalerPredictStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.vector.VectorMinMaxScalerPredictStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.vector.VectorMinMaxScalerPredictStreamOp",
                "params": [
                  "modelStreamFilePath",
                  "modelStreamScanInterval",
                  "modelStreamStartTime",
                  "outputCol",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorNormalizeStreamOp",
                "description": "VectorNormalizeStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.vector.VectorNormalizeStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.vector.VectorNormalizeStreamOp",
                "params": [
                  "p",
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorPolynomialExpandStreamOp",
                "description": "VectorPolynomialExpandStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.vector.VectorPolynomialExpandStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.vector.VectorPolynomialExpandStreamOp",
                "params": [
                  "degree",
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorSizeHintStreamOp",
                "description": "VectorSizeHintStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.vector.VectorSizeHintStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.vector.VectorSizeHintStreamOp",
                "params": [
                  "size",
                  "handleInvalidMethod",
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorSliceStreamOp",
                "description": "VectorSliceStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.vector.VectorSliceStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.vector.VectorSliceStreamOp",
                "params": [
                  "indices",
                  "selectedCol",
                  "outputCol",
                  "reservedCols",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              },
              {
                "name": "VectorStandardScalerPredictStreamOp",
                "description": "VectorStandardScalerPredictStreamOp",
                "id": "com.alibaba.alink.operator.stream.dataproc.vector.VectorStandardScalerPredictStreamOp",
                "className": "com.alibaba.alink.operator.stream.dataproc.vector.VectorStandardScalerPredictStreamOp",
                "params": [
                  "outputCol",
                  "numThreads"
                ],
                "numInPorts": 2,
                "numOutPorts": 2
              }
            ],
            "id": "stream.dataproc.vector"
          }
        ],
        "id": "stream.dataproc"
      },
      {
        "isDir": true,
        "name": "SQL",
        "children": [
          {
            "name": "AsStreamOp",
            "description": "AsStreamOp",
            "id": "com.alibaba.alink.operator.stream.sql.AsStreamOp",
            "className": "com.alibaba.alink.operator.stream.sql.AsStreamOp",
            "params": [
              "clause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FilterStreamOp",
            "description": "FilterStreamOp",
            "id": "com.alibaba.alink.operator.stream.sql.FilterStreamOp",
            "className": "com.alibaba.alink.operator.stream.sql.FilterStreamOp",
            "params": [
              "clause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SelectStreamOp",
            "description": "SelectStreamOp",
            "id": "com.alibaba.alink.operator.stream.sql.SelectStreamOp",
            "className": "com.alibaba.alink.operator.stream.sql.SelectStreamOp",
            "params": [
              "clause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UnionAllStreamOp",
            "description": "UnionAllStreamOp",
            "id": "com.alibaba.alink.operator.stream.sql.UnionAllStreamOp",
            "className": "com.alibaba.alink.operator.stream.sql.UnionAllStreamOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "WhereStreamOp",
            "description": "WhereStreamOp",
            "id": "com.alibaba.alink.operator.stream.sql.WhereStreamOp",
            "className": "com.alibaba.alink.operator.stream.sql.WhereStreamOp",
            "params": [
              "clause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "WindowGroupByStreamOp",
            "description": "WindowGroupByStreamOp",
            "id": "com.alibaba.alink.operator.stream.sql.WindowGroupByStreamOp",
            "className": "com.alibaba.alink.operator.stream.sql.WindowGroupByStreamOp",
            "params": [
              "selectClause",
              "groupByClause",
              "intervalUnit",
              "windowType",
              "sessionGap",
              "slidingLength",
              "windowLength"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.sql"
      },
      {
        "isDir": true,
        "name": "特征工程",
        "children": [
          {
            "name": "BinarizerStreamOp",
            "description": "BinarizerStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.BinarizerStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.BinarizerStreamOp",
            "params": [
              "threshold",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BucketizerStreamOp",
            "description": "BucketizerStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.BucketizerStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.BucketizerStreamOp",
            "params": [
              "cutsArray",
              "cutsArrayStr",
              "leftOpen",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "handleInvalid",
              "encode",
              "dropLast",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CrossFeaturePredictStreamOp",
            "description": "CrossFeaturePredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.CrossFeaturePredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.CrossFeaturePredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "outputCol",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DCTStreamOp",
            "description": "DCTStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.DCTStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.DCTStreamOp",
            "params": [
              "inverse",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "EqualWidthDiscretizerPredictStreamOp",
            "description": "EqualWidthDiscretizerPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.EqualWidthDiscretizerPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.EqualWidthDiscretizerPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "handleInvalid",
              "encode",
              "dropLast",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FeatureHasherStreamOp",
            "description": "FeatureHasherStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.FeatureHasherStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.FeatureHasherStreamOp",
            "params": [
              "selectedCols",
              "outputCol",
              "reservedCols",
              "numFeatures",
              "categoricalCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "HashCrossFeatureStreamOp",
            "description": "HashCrossFeatureStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.HashCrossFeatureStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.HashCrossFeatureStreamOp",
            "params": [
              "selectedCols",
              "numFeatures",
              "outputCol",
              "reservedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "HopTimeWindowStreamOp",
            "description": "HopTimeWindowStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.HopTimeWindowStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.HopTimeWindowStreamOp",
            "params": [
              "hopTime",
              "partitionCols",
              "windowTime",
              "latency",
              "watermarkType",
              "timeCol",
              "clause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MultiHotPredictStreamOp",
            "description": "MultiHotPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.MultiHotPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.MultiHotPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "handleInvalid",
              "encode",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "OneHotPredictStreamOp",
            "description": "OneHotPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.OneHotPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.OneHotPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "handleInvalid",
              "encode",
              "dropLast",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "OverCountWindowStreamOp",
            "description": "OverCountWindowStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp",
            "params": [
              "precedingRows",
              "partitionCols",
              "latency",
              "watermarkType",
              "reservedCols",
              "timeCol",
              "clause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "OverTimeWindowStreamOp",
            "description": "OverTimeWindowStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.OverTimeWindowStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.OverTimeWindowStreamOp",
            "params": [
              "precedingTime",
              "partitionCols",
              "latency",
              "watermarkType",
              "reservedCols",
              "timeCol",
              "clause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "PcaPredictStreamOp",
            "description": "PcaPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.PcaPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.PcaPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "reservedCols",
              "predictionCol",
              "vectorCol",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "QuantileDiscretizerPredictStreamOp",
            "description": "QuantileDiscretizerPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.QuantileDiscretizerPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.QuantileDiscretizerPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCols",
              "reservedCols",
              "outputCols",
              "handleInvalid",
              "encode",
              "dropLast",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SessionTimeWindowStreamOp",
            "description": "SessionTimeWindowStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.SessionTimeWindowStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.SessionTimeWindowStreamOp",
            "params": [
              "sessionGapTime",
              "partitionCols",
              "clause",
              "latency",
              "watermarkType",
              "timeCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TreeModelEncoderStreamOp",
            "description": "TreeModelEncoderStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.TreeModelEncoderStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.TreeModelEncoderStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "reservedCols",
              "predictionCol",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TumbleTimeWindowStreamOp",
            "description": "TumbleTimeWindowStreamOp",
            "id": "com.alibaba.alink.operator.stream.feature.TumbleTimeWindowStreamOp",
            "className": "com.alibaba.alink.operator.stream.feature.TumbleTimeWindowStreamOp",
            "params": [
              "partitionCols",
              "windowTime",
              "latency",
              "watermarkType",
              "timeCol",
              "clause"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.feature"
      },
      {
        "isDir": true,
        "name": "文本",
        "children": [
          {
            "name": "BertTextEmbeddingStreamOp",
            "description": "BertTextEmbeddingStreamOp",
            "id": "com.alibaba.alink.operator.stream.nlp.BertTextEmbeddingStreamOp",
            "className": "com.alibaba.alink.operator.stream.nlp.BertTextEmbeddingStreamOp",
            "params": [
              "selectedCol",
              "outputCol",
              "maxSeqLength",
              "doLowerCase",
              "layer",
              "bertModelName",
              "modelPath",
              "reservedCols",
              "intraOpParallelism"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DocCountVectorizerPredictStreamOp",
            "description": "DocCountVectorizerPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.nlp.DocCountVectorizerPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.nlp.DocCountVectorizerPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DocHashCountVectorizerPredictStreamOp",
            "description": "DocHashCountVectorizerPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.nlp.DocHashCountVectorizerPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.nlp.DocHashCountVectorizerPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "numThreads",
              "selectedCol",
              "outputCol",
              "reservedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DocWordCountStreamOp",
            "description": "DocWordCountStreamOp",
            "id": "com.alibaba.alink.operator.stream.nlp.DocWordCountStreamOp",
            "className": "com.alibaba.alink.operator.stream.nlp.DocWordCountStreamOp",
            "params": [
              "docIdCol",
              "contentCol",
              "wordDelimiter"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KeywordsExtractionStreamOp",
            "description": "KeywordsExtractionStreamOp",
            "id": "com.alibaba.alink.operator.stream.nlp.KeywordsExtractionStreamOp",
            "className": "com.alibaba.alink.operator.stream.nlp.KeywordsExtractionStreamOp",
            "params": [
              "selectedCol",
              "topN",
              "windowSize",
              "dampingFactor",
              "maxIter",
              "outputCol",
              "epsilon"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "NGramStreamOp",
            "description": "NGramStreamOp",
            "id": "com.alibaba.alink.operator.stream.nlp.NGramStreamOp",
            "className": "com.alibaba.alink.operator.stream.nlp.NGramStreamOp",
            "params": [
              "n",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RegexTokenizerStreamOp",
            "description": "RegexTokenizerStreamOp",
            "id": "com.alibaba.alink.operator.stream.nlp.RegexTokenizerStreamOp",
            "className": "com.alibaba.alink.operator.stream.nlp.RegexTokenizerStreamOp",
            "params": [
              "pattern",
              "gaps",
              "minTokenLength",
              "toLowerCase",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SegmentStreamOp",
            "description": "SegmentStreamOp",
            "id": "com.alibaba.alink.operator.stream.nlp.SegmentStreamOp",
            "className": "com.alibaba.alink.operator.stream.nlp.SegmentStreamOp",
            "params": [
              "userDefinedDict",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StopWordsRemoverStreamOp",
            "description": "StopWordsRemoverStreamOp",
            "id": "com.alibaba.alink.operator.stream.nlp.StopWordsRemoverStreamOp",
            "className": "com.alibaba.alink.operator.stream.nlp.StopWordsRemoverStreamOp",
            "params": [
              "caseSensitive",
              "stopWords",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TokenizerStreamOp",
            "description": "TokenizerStreamOp",
            "id": "com.alibaba.alink.operator.stream.nlp.TokenizerStreamOp",
            "className": "com.alibaba.alink.operator.stream.nlp.TokenizerStreamOp",
            "params": [
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "Word2VecPredictStreamOp",
            "description": "Word2VecPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.nlp.Word2VecPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.nlp.Word2VecPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "selectedCol",
              "reservedCols",
              "outputCol",
              "wordDelimiter",
              "predMethod",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.nlp"
      },
      {
        "isDir": true,
        "name": "分类",
        "children": [
          {
            "name": "BertTextClassifierPredictStreamOp",
            "description": "BertTextClassifierPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.BertTextClassifierPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.BertTextClassifierPredictStreamOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "predictionDetailCol",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BertTextPairClassifierPredictStreamOp",
            "description": "BertTextPairClassifierPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.BertTextPairClassifierPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.BertTextPairClassifierPredictStreamOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "predictionDetailCol",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "C45PredictStreamOp",
            "description": "C45PredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.C45PredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.C45PredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CartPredictStreamOp",
            "description": "CartPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.CartPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.CartPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DecisionTreePredictStreamOp",
            "description": "DecisionTreePredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.DecisionTreePredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.DecisionTreePredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmClassifierPredictStreamOp",
            "description": "FmClassifierPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.FmClassifierPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.FmClassifierPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GbdtPredictStreamOp",
            "description": "GbdtPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.GbdtPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.GbdtPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "Id3PredictStreamOp",
            "description": "Id3PredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.Id3PredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.Id3PredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LinearSvmPredictStreamOp",
            "description": "LinearSvmPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.LinearSvmPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.LinearSvmPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LogisticRegressionPredictStreamOp",
            "description": "LogisticRegressionPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.LogisticRegressionPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.LogisticRegressionPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "MultilayerPerceptronPredictStreamOp",
            "description": "MultilayerPerceptronPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.MultilayerPerceptronPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.MultilayerPerceptronPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "NaiveBayesPredictStreamOp",
            "description": "NaiveBayesPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.NaiveBayesPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.NaiveBayesPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "NaiveBayesTextPredictStreamOp",
            "description": "NaiveBayesTextPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.NaiveBayesTextPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.NaiveBayesTextPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RandomForestPredictStreamOp",
            "description": "RandomForestPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.RandomForestPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.RandomForestPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SoftmaxPredictStreamOp",
            "description": "SoftmaxPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.SoftmaxPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.SoftmaxPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TFTableModelClassifierPredictStreamOp",
            "description": "TFTableModelClassifierPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.classification.TFTableModelClassifierPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.classification.TFTableModelClassifierPredictStreamOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "predictionDetailCol",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.classification"
      },
      {
        "isDir": true,
        "name": "回归",
        "children": [
          {
            "name": "AftSurvivalRegPredictStreamOp",
            "description": "AftSurvivalRegPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.AftSurvivalRegPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.AftSurvivalRegPredictStreamOp",
            "params": [
              "quantileProbabilities",
              "vectorCol",
              "predictionDetailCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BertTextPairRegressorPredictStreamOp",
            "description": "BertTextPairRegressorPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.BertTextPairRegressorPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.BertTextPairRegressorPredictStreamOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "BertTextRegressorPredictStreamOp",
            "description": "BertTextRegressorPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.BertTextRegressorPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.BertTextRegressorPredictStreamOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "CartRegPredictStreamOp",
            "description": "CartRegPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.CartRegPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.CartRegPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DecisionTreeRegPredictStreamOp",
            "description": "DecisionTreeRegPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.DecisionTreeRegPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.DecisionTreeRegPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmRegressorPredictStreamOp",
            "description": "FmRegressorPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.FmRegressorPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.FmRegressorPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GbdtRegPredictStreamOp",
            "description": "GbdtRegPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.GbdtRegPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.GbdtRegPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GlmPredictStreamOp",
            "description": "GlmPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.GlmPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.GlmPredictStreamOp",
            "params": [
              "linkPredResultCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "IsotonicRegPredictStreamOp",
            "description": "IsotonicRegPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.IsotonicRegPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.IsotonicRegPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LassoRegPredictStreamOp",
            "description": "LassoRegPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.LassoRegPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.LassoRegPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LinearRegPredictStreamOp",
            "description": "LinearRegPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.LinearRegPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.LinearRegPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RandomForestRegPredictStreamOp",
            "description": "RandomForestRegPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.RandomForestRegPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.RandomForestRegPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "RidgeRegPredictStreamOp",
            "description": "RidgeRegPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.RidgeRegPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.RidgeRegPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TFTableModelRegressorPredictStreamOp",
            "description": "TFTableModelRegressorPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.regression.TFTableModelRegressorPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.regression.TFTableModelRegressorPredictStreamOp",
            "params": [
              "predictionCol",
              "reservedCols",
              "inferBatchSize"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.regression"
      },
      {
        "isDir": true,
        "name": "聚类",
        "children": [
          {
            "name": "BisectingKMeansPredictStreamOp",
            "description": "BisectingKMeansPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.clustering.BisectingKMeansPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.clustering.BisectingKMeansPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GeoKMeansPredictStreamOp",
            "description": "GeoKMeansPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.clustering.GeoKMeansPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.clustering.GeoKMeansPredictStreamOp",
            "params": [
              "predictionDistanceCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "GmmPredictStreamOp",
            "description": "GmmPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.clustering.GmmPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.clustering.GmmPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "KMeansPredictStreamOp",
            "description": "KMeansPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.clustering.KMeansPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.clustering.KMeansPredictStreamOp",
            "params": [
              "predictionDistanceCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LdaPredictStreamOp",
            "description": "LdaPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.clustering.LdaPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.clustering.LdaPredictStreamOp",
            "params": [
              "selectedCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "StreamingKMeansStreamOp",
            "description": "StreamingKMeansStreamOp",
            "id": "com.alibaba.alink.operator.stream.clustering.StreamingKMeansStreamOp",
            "className": "com.alibaba.alink.operator.stream.clustering.StreamingKMeansStreamOp",
            "params": [
              "predictionCol",
              "halfLife",
              "predictionDistanceCol",
              "predictionClusterCol",
              "reservedCols",
              "timeInterval"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.clustering"
      },
      {
        "isDir": true,
        "name": "推荐",
        "children": [
          {
            "name": "AlsItemsPerUserRecommStreamOp",
            "description": "AlsItemsPerUserRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.AlsItemsPerUserRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.AlsItemsPerUserRecommStreamOp",
            "params": [
              "userCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AlsRateRecommStreamOp",
            "description": "AlsRateRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.AlsRateRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.AlsRateRecommStreamOp",
            "params": [
              "userCol",
              "itemCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AlsSimilarItemsRecommStreamOp",
            "description": "AlsSimilarItemsRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.AlsSimilarItemsRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.AlsSimilarItemsRecommStreamOp",
            "params": [
              "itemCol",
              "k",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AlsSimilarUsersRecommStreamOp",
            "description": "AlsSimilarUsersRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.AlsSimilarUsersRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.AlsSimilarUsersRecommStreamOp",
            "params": [
              "userCol",
              "k",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AlsUsersPerItemRecommStreamOp",
            "description": "AlsUsersPerItemRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.AlsUsersPerItemRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.AlsUsersPerItemRecommStreamOp",
            "params": [
              "itemCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FlattenKObjectStreamOp",
            "description": "FlattenKObjectStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.FlattenKObjectStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.FlattenKObjectStreamOp",
            "params": [
              "selectedCol",
              "outputCols",
              "outputColTypes",
              "reservedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmItemsPerUserRecommStreamOp",
            "description": "FmItemsPerUserRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.FmItemsPerUserRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.FmItemsPerUserRecommStreamOp",
            "params": [
              "userCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FmUsersPerItemRecommStreamOp",
            "description": "FmUsersPerItemRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.FmUsersPerItemRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.FmUsersPerItemRecommStreamOp",
            "params": [
              "itemCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ItemCfItemsPerUserRecommStreamOp",
            "description": "ItemCfItemsPerUserRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.ItemCfItemsPerUserRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.ItemCfItemsPerUserRecommStreamOp",
            "params": [
              "userCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ItemCfRateRecommStreamOp",
            "description": "ItemCfRateRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.ItemCfRateRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.ItemCfRateRecommStreamOp",
            "params": [
              "userCol",
              "itemCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ItemCfSimilarItemsRecommStreamOp",
            "description": "ItemCfSimilarItemsRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.ItemCfSimilarItemsRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.ItemCfSimilarItemsRecommStreamOp",
            "params": [
              "itemCol",
              "k",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ItemCfUsersPerItemRecommStreamOp",
            "description": "ItemCfUsersPerItemRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.ItemCfUsersPerItemRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.ItemCfUsersPerItemRecommStreamOp",
            "params": [
              "itemCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "SwingRecommStreamOp",
            "description": "SwingRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.SwingRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.SwingRecommStreamOp",
            "params": [
              "itemCol",
              "k",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UserCfItemsPerUserRecommStreamOp",
            "description": "UserCfItemsPerUserRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.UserCfItemsPerUserRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.UserCfItemsPerUserRecommStreamOp",
            "params": [
              "userCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UserCfRateRecommStreamOp",
            "description": "UserCfRateRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.UserCfRateRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.UserCfRateRecommStreamOp",
            "params": [
              "userCol",
              "itemCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UserCfSimilarUsersRecommStreamOp",
            "description": "UserCfSimilarUsersRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.UserCfSimilarUsersRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.UserCfSimilarUsersRecommStreamOp",
            "params": [
              "userCol",
              "k",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UserCfUsersPerItemRecommStreamOp",
            "description": "UserCfUsersPerItemRecommStreamOp",
            "id": "com.alibaba.alink.operator.stream.recommendation.UserCfUsersPerItemRecommStreamOp",
            "className": "com.alibaba.alink.operator.stream.recommendation.UserCfUsersPerItemRecommStreamOp",
            "params": [
              "itemCol",
              "k",
              "excludeKnown",
              "initRecommCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "recommCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.recommendation"
      },
      {
        "isDir": true,
        "name": "评估",
        "children": [
          {
            "name": "EvalBinaryClassStreamOp",
            "description": "EvalBinaryClassStreamOp",
            "id": "com.alibaba.alink.operator.stream.evaluation.EvalBinaryClassStreamOp",
            "className": "com.alibaba.alink.operator.stream.evaluation.EvalBinaryClassStreamOp",
            "params": [
              "predictionCol",
              "predictionDetailCol",
              "timeInterval",
              "labelCol",
              "positiveLabelValueString"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "EvalMultiClassStreamOp",
            "description": "EvalMultiClassStreamOp",
            "id": "com.alibaba.alink.operator.stream.evaluation.EvalMultiClassStreamOp",
            "className": "com.alibaba.alink.operator.stream.evaluation.EvalMultiClassStreamOp",
            "params": [
              "predictionCol",
              "timeInterval",
              "labelCol",
              "predictionDetailCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.evaluation"
      },
      {
        "isDir": true,
        "name": "异常检测",
        "children": [
          {
            "name": "IsolationForestsPredictStreamOp",
            "description": "IsolationForestsPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.outlier.IsolationForestsPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.outlier.IsolationForestsPredictStreamOp",
            "params": [
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.outlier"
      },
      {
        "isDir": true,
        "name": "在线学习",
        "children": [
          {
            "name": "FtrlModelFilterStreamOp",
            "description": "FtrlModelFilterStreamOp",
            "id": "com.alibaba.alink.operator.stream.onlinelearning.FtrlModelFilterStreamOp",
            "className": "com.alibaba.alink.operator.stream.onlinelearning.FtrlModelFilterStreamOp",
            "params": [
              "aucThreshold",
              "accuracyThreshold",
              "vectorCol",
              "labelCol",
              "positiveLabelValueString"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FtrlPredictStreamOp",
            "description": "FtrlPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.onlinelearning.FtrlPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.onlinelearning.FtrlPredictStreamOp",
            "params": [
              "vectorCol",
              "modelStreamFilePath",
              "modelStreamScanInterval",
              "modelStreamStartTime",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "FtrlTrainStreamOp",
            "description": "FtrlTrainStreamOp",
            "id": "com.alibaba.alink.operator.stream.onlinelearning.FtrlTrainStreamOp",
            "className": "com.alibaba.alink.operator.stream.onlinelearning.FtrlTrainStreamOp",
            "params": [
              "l1",
              "l2",
              "alpha",
              "beta",
              "labelCol",
              "vectorCol",
              "vectorSize",
              "featureCols",
              "withIntercept",
              "timeInterval"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.onlinelearning"
      },
      {
        "isDir": true,
        "name": "相似度",
        "children": [
          {
            "name": "StringSimilarityPairwiseStreamOp",
            "description": "StringSimilarityPairwiseStreamOp",
            "id": "com.alibaba.alink.operator.stream.similarity.StringSimilarityPairwiseStreamOp",
            "className": "com.alibaba.alink.operator.stream.similarity.StringSimilarityPairwiseStreamOp",
            "params": [
              "lambda",
              "metric",
              "windowSize",
              "numBucket",
              "numHashTables",
              "seed",
              "selectedCols",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TextSimilarityPairwiseStreamOp",
            "description": "TextSimilarityPairwiseStreamOp",
            "id": "com.alibaba.alink.operator.stream.similarity.TextSimilarityPairwiseStreamOp",
            "className": "com.alibaba.alink.operator.stream.similarity.TextSimilarityPairwiseStreamOp",
            "params": [
              "lambda",
              "metric",
              "windowSize",
              "numBucket",
              "numHashTables",
              "seed",
              "selectedCols",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.similarity"
      },
      {
        "isDir": true,
        "name": "工具类",
        "children": [
          {
            "name": "MTableSerializeStreamOp",
            "description": "MTableSerializeStreamOp",
            "id": "com.alibaba.alink.operator.stream.utils.MTableSerializeStreamOp",
            "className": "com.alibaba.alink.operator.stream.utils.MTableSerializeStreamOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "PrintStreamOp",
            "description": "PrintStreamOp",
            "id": "com.alibaba.alink.operator.stream.utils.PrintStreamOp",
            "className": "com.alibaba.alink.operator.stream.utils.PrintStreamOp",
            "params": [
              "refreshInterval",
              "maxLimit"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TensorSerializeStreamOp",
            "description": "TensorSerializeStreamOp",
            "id": "com.alibaba.alink.operator.stream.utils.TensorSerializeStreamOp",
            "className": "com.alibaba.alink.operator.stream.utils.TensorSerializeStreamOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UDFStreamOp",
            "description": "UDFStreamOp",
            "id": "com.alibaba.alink.operator.stream.utils.UDFStreamOp",
            "className": "com.alibaba.alink.operator.stream.utils.UDFStreamOp",
            "params": [
              "funcName",
              "selectedCols",
              "outputCol",
              "reservedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "UDTFStreamOp",
            "description": "UDTFStreamOp",
            "id": "com.alibaba.alink.operator.stream.utils.UDTFStreamOp",
            "className": "com.alibaba.alink.operator.stream.utils.UDTFStreamOp",
            "params": [
              "funcName",
              "selectedCols",
              "outputCols",
              "reservedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "VectorSerializeStreamOp",
            "description": "VectorSerializeStreamOp",
            "id": "com.alibaba.alink.operator.stream.utils.VectorSerializeStreamOp",
            "className": "com.alibaba.alink.operator.stream.utils.VectorSerializeStreamOp",
            "params": [],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.utils"
      },
      {
        "isDir": true,
        "name": "audio",
        "children": [
          {
            "name": "ExtractMfccFeatureStreamOp",
            "description": "ExtractMfccFeatureStreamOp",
            "id": "com.alibaba.alink.operator.stream.audio.ExtractMfccFeatureStreamOp",
            "className": "com.alibaba.alink.operator.stream.audio.ExtractMfccFeatureStreamOp",
            "params": [
              "windowSecond",
              "hopSecond",
              "nMfcc",
              "sampleRate",
              "selectedCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ReadAudioToTensorStreamOp",
            "description": "ReadAudioToTensorStreamOp",
            "id": "com.alibaba.alink.operator.stream.audio.ReadAudioToTensorStreamOp",
            "className": "com.alibaba.alink.operator.stream.audio.ReadAudioToTensorStreamOp",
            "params": [
              "durationTime",
              "startTime",
              "sampleRate",
              "relativeFilePathCol",
              "rootFilePath",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.audio"
      },
      {
        "isDir": true,
        "name": "image",
        "children": [
          {
            "name": "ReadImageToTensorStreamOp",
            "description": "ReadImageToTensorStreamOp",
            "id": "com.alibaba.alink.operator.stream.image.ReadImageToTensorStreamOp",
            "className": "com.alibaba.alink.operator.stream.image.ReadImageToTensorStreamOp",
            "params": [
              "imageWidth",
              "imageHeight",
              "rootFilePath",
              "outputCol",
              "relativeFilePathCol",
              "reservedCols"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "WriteTensorToImageStreamOp",
            "description": "WriteTensorToImageStreamOp",
            "id": "com.alibaba.alink.operator.stream.image.WriteTensorToImageStreamOp",
            "className": "com.alibaba.alink.operator.stream.image.WriteTensorToImageStreamOp",
            "params": [
              "rootFilePath",
              "tensorCol",
              "relativeFilePathCol",
              "reservedCols",
              "imageType"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.image"
      },
      {
        "isDir": true,
        "name": "tensorflow",
        "children": [
          {
            "name": "TFSavedModelPredictStreamOp",
            "description": "TFSavedModelPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.tensorflow.TFSavedModelPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.tensorflow.TFSavedModelPredictStreamOp",
            "params": [
              "modelPath",
              "reservedCols",
              "graphDefTag",
              "signatureDefKey",
              "inputSignatureDefs",
              "outputSignatureDefs",
              "selectedCols",
              "outputSchemaStr"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TFTableModelPredictStreamOp",
            "description": "TFTableModelPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.tensorflow.TFTableModelPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.tensorflow.TFTableModelPredictStreamOp",
            "params": [
              "reservedCols",
              "graphDefTag",
              "signatureDefKey",
              "inputSignatureDefs",
              "outputSignatureDefs",
              "selectedCols",
              "outputSchemaStr"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TensorFlow2StreamOp",
            "description": "TensorFlow2StreamOp",
            "id": "com.alibaba.alink.operator.stream.tensorflow.TensorFlow2StreamOp",
            "className": "com.alibaba.alink.operator.stream.tensorflow.TensorFlow2StreamOp",
            "params": [
              "numPSs",
              "numWorkers",
              "outputSchemaStr",
              "selectedCols",
              "pythonEnv",
              "intraOpParallelism",
              "userFiles",
              "mainScriptFile",
              "userParams"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "TensorFlowStreamOp",
            "description": "TensorFlowStreamOp",
            "id": "com.alibaba.alink.operator.stream.tensorflow.TensorFlowStreamOp",
            "className": "com.alibaba.alink.operator.stream.tensorflow.TensorFlowStreamOp",
            "params": [
              "numPSs",
              "numWorkers",
              "outputSchemaStr",
              "selectedCols",
              "pythonEnv",
              "intraOpParallelism",
              "userFiles",
              "mainScriptFile",
              "userParams"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.tensorflow"
      },
      {
        "isDir": true,
        "name": "时间序列",
        "children": [
          {
            "name": "ArimaStreamOp",
            "description": "ArimaStreamOp",
            "id": "com.alibaba.alink.operator.stream.timeseries.ArimaStreamOp",
            "className": "com.alibaba.alink.operator.stream.timeseries.ArimaStreamOp",
            "params": [
              "predictNum",
              "order",
              "seasonalOrder",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "estMethod",
              "seasonalPeriod",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AutoArimaStreamOp",
            "description": "AutoArimaStreamOp",
            "id": "com.alibaba.alink.operator.stream.timeseries.AutoArimaStreamOp",
            "className": "com.alibaba.alink.operator.stream.timeseries.AutoArimaStreamOp",
            "params": [
              "d",
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "estMethod",
              "icType",
              "maxOrder",
              "maxSeasonalOrder",
              "seasonalPeriod",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "AutoGarchStreamOp",
            "description": "AutoGarchStreamOp",
            "id": "com.alibaba.alink.operator.stream.timeseries.AutoGarchStreamOp",
            "className": "com.alibaba.alink.operator.stream.timeseries.AutoGarchStreamOp",
            "params": [
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "icType",
              "maxOrder",
              "ifGARCH11",
              "minusMean",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "DeepARPredictStreamOp",
            "description": "DeepARPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.timeseries.DeepARPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.timeseries.DeepARPredictStreamOp",
            "params": [
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ExpandExtendedVarsStreamOp",
            "description": "ExpandExtendedVarsStreamOp",
            "id": "com.alibaba.alink.operator.stream.timeseries.ExpandExtendedVarsStreamOp",
            "className": "com.alibaba.alink.operator.stream.timeseries.ExpandExtendedVarsStreamOp",
            "params": [
              "vectorCol",
              "numVars",
              "extendedVectorCol",
              "reservedCols",
              "outputCol"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "HoltWintersStreamOp",
            "description": "HoltWintersStreamOp",
            "id": "com.alibaba.alink.operator.stream.timeseries.HoltWintersStreamOp",
            "className": "com.alibaba.alink.operator.stream.timeseries.HoltWintersStreamOp",
            "params": [
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "alpha",
              "beta",
              "gamma",
              "frequency",
              "doTrend",
              "doSeasonal",
              "seasonalType",
              "levelStart",
              "trendStart",
              "seasonalStart",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LSTNetPredictStreamOp",
            "description": "LSTNetPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.timeseries.LSTNetPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.timeseries.LSTNetPredictStreamOp",
            "params": [
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LookupValueInTimeSeriesStreamOp",
            "description": "LookupValueInTimeSeriesStreamOp",
            "id": "com.alibaba.alink.operator.stream.timeseries.LookupValueInTimeSeriesStreamOp",
            "className": "com.alibaba.alink.operator.stream.timeseries.LookupValueInTimeSeriesStreamOp",
            "params": [
              "timeSeriesCol",
              "timeCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "LookupVectorInTimeSeriesStreamOp",
            "description": "LookupVectorInTimeSeriesStreamOp",
            "id": "com.alibaba.alink.operator.stream.timeseries.LookupVectorInTimeSeriesStreamOp",
            "className": "com.alibaba.alink.operator.stream.timeseries.LookupVectorInTimeSeriesStreamOp",
            "params": [
              "timeSeriesCol",
              "timeCol",
              "outputCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ProphetPredictStreamOp",
            "description": "ProphetPredictStreamOp",
            "id": "com.alibaba.alink.operator.stream.timeseries.ProphetPredictStreamOp",
            "className": "com.alibaba.alink.operator.stream.timeseries.ProphetPredictStreamOp",
            "params": [
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ProphetStreamOp",
            "description": "ProphetStreamOp",
            "id": "com.alibaba.alink.operator.stream.timeseries.ProphetStreamOp",
            "className": "com.alibaba.alink.operator.stream.timeseries.ProphetStreamOp",
            "params": [
              "uncertaintySamples",
              "stanInit",
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          },
          {
            "name": "ShiftStreamOp",
            "description": "ShiftStreamOp",
            "id": "com.alibaba.alink.operator.stream.timeseries.ShiftStreamOp",
            "className": "com.alibaba.alink.operator.stream.timeseries.ShiftStreamOp",
            "params": [
              "shiftNum",
              "predictNum",
              "valueCol",
              "predictionCol",
              "predictionDetailCol",
              "reservedCols",
              "numThreads"
            ],
            "numInPorts": 2,
            "numOutPorts": 2
          }
        ],
        "id": "stream.timeseries"
      }
    ],
    "id": "stream"
  }
];
