package com.alibaba.alink.operator.common.clustering.lda;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.VectorIterator;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.List;

/**
 * Em corpus step.
 * Build the corpus, the current word and its topic in each doc, and build the word-topic matrix.
 */
public class EmCorpusStep extends ComputeFunction {

    private int numTopic;
    private double alpha;
    private double beta;

    private RandomDataGenerator rand = new RandomDataGenerator();

    /**
     * Constructor.
     * @param numTopic the number of topics.
     * @param alpha alpha param.
     * @param beta beta param.
     */
    public EmCorpusStep(int numTopic, double alpha, double beta) {
        this.numTopic = numTopic;
        this.alpha = alpha;
        this.beta = beta;
    }

    @Override
    public void calc(ComContext context) {
        int vocabularySize = ((List<Integer>) context.getObj(LdaVariable.vocabularySize)).get(0);
        if (context.getStepNo() == 1) {
            DenseMatrix nWordTopics = new DenseMatrix(vocabularySize + 1, numTopic);
            context.putObj(LdaVariable.nWordTopics, nWordTopics.getData());
            List<SparseVector> data = context.getObj(LdaVariable.data);
            if (data == null) {
                return;
            }
            int localDocSize = data.size();
            Document[] docs = new Document[localDocSize];
            DenseMatrix nDocTopics = new DenseMatrix(localDocSize, numTopic);
            context.putObj(LdaVariable.corpus, docs);
            context.putObj(LdaVariable.nDocTopics, nDocTopics);
            int docId = 0;
            int topic, word;
            for (SparseVector sparseVector : data) {

                int wordNum = 0;
                for (double value : sparseVector.getValues()) {
                    wordNum += value;
                }
                Document doc = new Document(wordNum);
                int idx = 0;
                VectorIterator iter = sparseVector.iterator();
                while (iter.hasNext()) {
                    word = iter.getIndex();
                    for (int j = 0; j < (int) iter.getValue(); j++) {
                        topic = rand.nextInt(0, numTopic - 1);
                        doc.setWordIdxs(idx, word);
                        doc.setTopicIdxs(idx, topic);
                        updateDocWordTopics(nDocTopics, nWordTopics, docId, word, vocabularySize, topic, 1);
                        idx++;
                    }
                    iter.next();
                }
                docs[docId] = doc;
                docId++;
            }
            context.removeObj(LdaVariable.data);
        } else {
            Document[] docs = context.getObj(LdaVariable.corpus);
            if (docs == null) {
                return;
            }
            DenseMatrix nDocTopics = context.getObj(LdaVariable.nDocTopics);
            DenseMatrix nWordTopics = new DenseMatrix(vocabularySize + 1, numTopic,
                    context.getObj(LdaVariable.nWordTopics), false);
            int docId = 0;
            double[] p = new double[numTopic];
            double pSum;
            int newTopic;
            for (Document doc : docs) {
                int wordCount = doc.getLength();
                for (int i = 0; i < wordCount; ++i) {
                    int word = doc.getWordIdxs(i);
                    int topic = doc.getTopicIdxs(i);
                    updateDocWordTopics(nDocTopics, nWordTopics, docId, word, vocabularySize, topic, -1);
                    pSum = 0;
                    for (int k = 0; k < numTopic; k++) {
                        //calculate the probability that word belongs to each topic, and then generate the topic.
                        pSum += (nWordTopics.get(word, k) + beta)
                                * (nDocTopics.get(docId, k) + alpha)
                                / (nWordTopics.get(vocabularySize, k) + vocabularySize * beta);
                        p[k] = pSum;

                    }
                    double u = rand.nextUniform(0, 1) * pSum;
                    newTopic = findProbIdx(p, u);
                    doc.setTopicIdxs(i, newTopic);
                    updateDocWordTopics(nDocTopics, nWordTopics, docId, word, vocabularySize, newTopic, 1);
                }
                docId++;
            }
            nWordTopics = new DenseMatrix(nWordTopics.numRows(), nWordTopics.numCols());
            for (Document doc : docs) {
                int length = doc.getLength();
                for (int i = 0; i < length; i++) {
                    nWordTopics.add(doc.getWordIdxs(i), doc.getTopicIdxs(i), 1);
                    nWordTopics.add(vocabularySize, doc.getTopicIdxs(i), 1);
                }
            }
            context.putObj(LdaVariable.nWordTopics, nWordTopics.getData());
        }
    }

    private int findProbIdx(double[] p, double u) {
        for (int i = 0; i < p.length; i++) {
            if (p[i] >= u) {
                return i;
            }
        }
        return p.length - 1;
    }

    private void updateDocWordTopics(DenseMatrix nDocTopics, DenseMatrix nWordTopics,
                                     int docId, int word, int vocabularySize, int topic, int value) {
        nDocTopics.add(docId, topic, value);
        nWordTopics.add(word, topic, value);
        nWordTopics.add(vocabularySize, topic, value);
    }
}
