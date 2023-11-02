import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestReadImageToTensorStreamOp(unittest.TestCase):
    def test_readimagetotensorstreamop(self):

        df_data = pd.DataFrame([
            'sphx_glr_plot_scripted_tensor_transforms_001.png'
        ])
        
        stream_data = StreamOperator.fromDataframe(df_data, schemaStr = 'path string')
        
        ReadImageToTensorStreamOp()\
            .setRootFilePath("http://alink-test-datatset.oss-cn-hangzhou-zmf.aliyuncs.com/images/")\
        	.setRelativeFilePathCol("path")\
        	.setOutputCol("tensor")\
            .linkFrom(stream_data)\
            .print()
            
        StreamOperator.execute()
        
        pass