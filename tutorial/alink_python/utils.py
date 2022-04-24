from pyalink.alink import *
import os
from datetime import datetime
from math import floor

ROOT_DIR = "/Users/yangxu/alink/data/"

def generateSchemaString(colNames, colTypes):
    n = len(colNames)
    str = ''
    for i in range(n):
        if i>0 :
            str = str + ","
        str = str + colNames[i] + " " + colTypes[i]
    return str


def splitTrainTestIfNotExist(source, trainFilePath, testFilePath, ratio):
    if not(os.path.exists(trainFilePath) or os.path.exists(testFilePath)):
        spliter = SplitBatchOp().setFraction(ratio)
        
        source.link(spliter)
        
        spliter.link(
            AkSinkBatchOp().setFilePath(trainFilePath)
        )
        
        spliter.getSideOutput(0)\
            .link(
                AkSinkBatchOp().setFilePath(testFilePath)
            )
        
        BatchOperator.execute()
		
        
class Stopwatch :
    
    def __init__(self):
        self.timer_start = datetime.now()
        self.timer_end = datetime.now()
    
    def start(self):
        self.timer_start = datetime.now()
        
    def stop(self):
        self.timer_end = datetime.now()
        
    def reset(self):
        self.timer_start = datetime.now()
    
    def getElapsedTimeSpan(self):
        elapse = self.timer_end - self.timer_start
        
        r = "";
        if elapse.days > 0 :
            r += str(elapse.days) + " days  "

        if elapse.seconds >= 3600 :
            r += str(floor(elapse.seconds/3600)) + " hours  "

        if elapse.seconds >= 60 :
            r += str(floor(elapse.seconds%3600/60)) + " minutes  "

        if elapse.seconds > 0 :
            r += str(elapse.seconds%60) + " seconds  "

        if elapse.microseconds > 0 :
            r += str(elapse.microseconds/1000) + " milliseconds  "
            
        return r