package com.alibaba.alink.operator.common.nlp.jiebasegment.viterbi;

import com.alibaba.alink.operator.common.nlp.jiebasegment.CharacterUtil;
import com.alibaba.alink.operator.common.nlp.jiebasegment.Node;
import com.alibaba.alink.operator.common.nlp.jiebasegment.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.Collections;


public class FinalSeg {
    private static FinalSeg singleInstance;
    private static final String PROB_EMIT = "/prob_emit.txt";
    private static char[] states = new char[] { 'B', 'M', 'E', 'S' };
    private static Map<Character, Map<Character, Double>> emit;
    private static Map<Character, Double> start;
    private static Map<Character, Map<Character, Double>> trans;
    private static Map<Character, char[]> prevStatus;
    private static Double MIN_FLOAT = -3.14e100;;


    private FinalSeg() {
    }


    public synchronized static FinalSeg getInstance() {
        if (null == singleInstance) {
            singleInstance = new FinalSeg();
            singleInstance.loadModel();
        }
        return singleInstance;
    }


    private void loadModel() {
        long s = System.currentTimeMillis();
        prevStatus = new HashMap<Character, char[]>();
        prevStatus.put('B', new char[] { 'E', 'S' });
        prevStatus.put('M', new char[] { 'M', 'B' });
        prevStatus.put('S', new char[] { 'S', 'E' });
        prevStatus.put('E', new char[] { 'B', 'M' });

        start = new HashMap<Character, Double>();
        start.put('B', -0.26268660809250016);
        start.put('E', -3.14e+100);
        start.put('M', -3.14e+100);
        start.put('S', -1.4652633398537678);

        trans = new HashMap<Character, Map<Character, Double>>();
        Map<Character, Double> transB = new HashMap<Character, Double>();
        transB.put('E', -0.510825623765990);
        transB.put('M', -0.916290731874155);
        trans.put('B', transB);
        Map<Character, Double> transE = new HashMap<Character, Double>();
        transE.put('B', -0.5897149736854513);
        transE.put('S', -0.8085250474669937);
        trans.put('E', transE);
        Map<Character, Double> transM = new HashMap<Character, Double>();
        transM.put('E', -0.33344856811948514);
        transM.put('M', -1.2603623820268226);
        trans.put('M', transM);
        Map<Character, Double> transS = new HashMap<Character, Double>();
        transS.put('B', -0.7211965654669841);
        transS.put('S', -0.6658631448798212);
        trans.put('S', transS);

        InputStream is = this.getClass().getResourceAsStream(PROB_EMIT);
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
            emit = new HashMap<Character, Map<Character, Double>>();
            Map<Character, Double> values = null;
            while (br.ready()) {
                String line = br.readLine();
                String[] tokens = line.split("\t");
                if (tokens.length == 1) {
                    values = new HashMap<Character, Double>();
                    emit.put(tokens[0].charAt(0), values);
                }
                else {
                    values.put(tokens[0].charAt(0), Double.valueOf(tokens[1]));
                }
            }
        }
        catch (IOException e) {
            System.err.println(String.format(Locale.getDefault(), "%s: load model failure!", PROB_EMIT));
        }
        finally {
            try {
                if (null != is)
                    is.close();
            }
            catch (IOException e) {
                System.err.println(String.format(Locale.getDefault(), "%s: close failure!", PROB_EMIT));
            }
        }
        System.out.println(String.format(Locale.getDefault(), "model load finished, time elapsed %d ms.",
            System.currentTimeMillis() - s));
    }


    public void cut(String sentence, List<String> tokens) {
        StringBuilder chinese = new StringBuilder();
        StringBuilder other = new StringBuilder();
        for (int i = 0; i < sentence.length(); ++i) {
            char ch = sentence.charAt(i);
            if (CharacterUtil.isChineseLetter(ch)) {
                if (other.length() > 0) {
                    processOtherUnknownWords(other.toString(), tokens);
                    other = new StringBuilder();
                }
                chinese.append(ch);
            }
            else {
                if (chinese.length() > 0) {
                    viterbi(chinese.toString(), tokens);
                    chinese = new StringBuilder();
                }
                other.append(ch);
            }

        }
        if (chinese.length() > 0)
            viterbi(chinese.toString(), tokens);
        else {
            processOtherUnknownWords(other.toString(), tokens);
        }
    }


    public void viterbi(String sentence, List<String> tokens) {
        Vector<Map<Character, Double>> v = new Vector<Map<Character, Double>>();
        Map<Character, Node> path = new HashMap<Character, Node>();

        v.add(new HashMap<Character, Double>());
        for (char state : states) {
            Double emP = emit.get(state).get(sentence.charAt(0));
            if (null == emP)
                emP = MIN_FLOAT;
            v.get(0).put(state, start.get(state) + emP);
            path.put(state, new Node(state, null));
        }

        for (int i = 1; i < sentence.length(); ++i) {
            Map<Character, Double> vv = new HashMap<Character, Double>();
            v.add(vv);
            Map<Character, Node> newPath = new HashMap<Character, Node>();
            for (char y : states) {
                Double emp = emit.get(y).get(sentence.charAt(i));
                if (emp == null)
                    emp = MIN_FLOAT;
                Pair<Character> candidate = null;
                for (char y0 : prevStatus.get(y)) {
                    Double tranp = trans.get(y0).get(y);
                    if (null == tranp)
                        tranp = MIN_FLOAT;
                    tranp += (emp + v.get(i - 1).get(y0));
                    if (null == candidate)
                        candidate = new Pair<Character>(y0, tranp);
                    else if (candidate.freq <= tranp) {
                        candidate.freq = tranp;
                        candidate.key = y0;
                    }
                }
                vv.put(y, candidate.freq);
                newPath.put(y, new Node(y, path.get(candidate.key)));
            }
            path = newPath;
        }
        double probE = v.get(sentence.length() - 1).get('E');
        double probS = v.get(sentence.length() - 1).get('S');
        Vector<Character> posList = new Vector<Character>(sentence.length());
        Node win;
        if (probE < probS)
            win = path.get('S');
        else
            win = path.get('E');

        while (win != null) {
            posList.add(win.value);
            win = win.parent;
        }
        Collections.reverse(posList);

        int begin = 0, next = 0;
        for (int i = 0; i < sentence.length(); ++i) {
            char pos = posList.get(i);
            if (pos == 'B')
                begin = i;
            else if (pos == 'E') {
                tokens.add(sentence.substring(begin, i + 1));
                next = i + 1;
            }
            else if (pos == 'S') {
                tokens.add(sentence.substring(i, i + 1));
                next = i + 1;
            }
        }
        if (next < sentence.length())
            tokens.add(sentence.substring(next));
    }


    private void processOtherUnknownWords(String other, List<String> tokens) {
        Matcher mat = CharacterUtil.reSkip.matcher(other);
        int offset = 0;
        while (mat.find()) {
            if (mat.start() > offset) {
                tokens.add(other.substring(offset, mat.start()));
            }
            tokens.add(mat.group());
            offset = mat.end();
        }
        if (offset < other.length())
            tokens.add(other.substring(offset));
    }
}
