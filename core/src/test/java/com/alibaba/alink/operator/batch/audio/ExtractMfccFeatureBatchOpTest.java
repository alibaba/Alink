package com.alibaba.alink.operator.batch.audio;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class ExtractMfccFeatureBatchOpTest extends AlinkTestBase {
    @Test
    public void testExtractMfccFeatureBatchOp() throws Exception {
        String DATA_DIR = "http://alink-audio-data.oss-cn-hangzhou-zmf.aliyuncs.com/casia/wangzhe/happy";
        String[] allFiles = {"246.wav"};
        int sampleRate = 16000;
        String tensorName = "tensor";
        String mfccName = "mfcc";
        String wavFile = "wav_file_path";
        List<Row> rows = new MemSourceBatchOp(allFiles, wavFile)
                .link(new ReadAudioToTensorBatchOp()
                        .setRootFilePath(DATA_DIR)
                        .setSampleRate(sampleRate)
                        .setRelativeFilePathCol(wavFile)
                        .setDuration(2)
                        .setOutputCol(tensorName)
                )
                .link(new ExtractMfccFeatureBatchOp()
                        .setSelectedCol(tensorName)
                        .setSampleRate(sampleRate)
                        .setWindowTime(0.128)
                        .setHopTime(0.032)
                        .setNumMfcc(26)
                        .setOutputCol(mfccName))
                .select(new String[]{wavFile, mfccName})
                .collect();
        FloatTensor tensor = (FloatTensor) rows.get(0).getField(1);
        float aFloat = tensor.getFloat(0, 0, 0);
        double number = (double) (Math.round(aFloat * 100) / 100.0);
        assertEquals(48.78, number);
    }
}
