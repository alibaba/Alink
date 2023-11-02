import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestReadImageToTensor(unittest.TestCase):
    def test_readimagetotensor(self):

        df_data = pd.DataFrame([
            'sphx_glr_plot_scripted_tensor_transforms_001.png'
        ])
        
        batch_data = BatchOperator.fromDataframe(df_data, schemaStr = 'path string')
        
        ReadImageToTensor()\
            .setRootFilePath("http://alink-test-datatset.oss-cn-hangzhou-zmf.aliyuncs.com/images/")\
        	.setRelativeFilePathCol("path")\
        	.setOutputCol("tensor")\
            .transform(batch_data)\
            .print()
        
        pass