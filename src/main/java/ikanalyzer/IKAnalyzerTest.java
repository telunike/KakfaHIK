package ikanalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.IOException;
import java.io.StringReader;

public class IKAnalyzerTest {

    public static String startIKAnalyzer(String line) {
        IKAnalyzer ikAnalyzer = new IKAnalyzer();

        //使用智能分词
        //ik2012与3.0的区别。3.0里面吗，没有这个方法
        ikAnalyzer.setUseSmart(false);
        try{
            String resultData = printAnalyzerResult(ikAnalyzer, line);
            System.out.println(resultData);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static String printAnalyzerResult(Analyzer analyzer, String keyWord) throws IOException {
        String resultData = "";
        String infoData = "";

        TokenStream tokenStream = analyzer.tokenStream("contect", new StringReader(keyWord));
        tokenStream.addAttribute(CharTermAttribute.class);
        while (tokenStream.incrementToken()) {
            CharTermAttribute charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
            infoData = infoData + "   " + charTermAttribute.toString();
        }

        if (infoData != null&&infoData.length() != 0) {
            resultData = resultData + resultData + infoData.trim() + "\r\n";
        } else {
            resultData = "";
        }
        return resultData;
    }

    public static void main(String[] args) {
        String line = "据路透社报道，印度尼西亚社会事务部一官员星期二(29 日)表示，日惹市附近当地时间 27 日晨 5 时 53 分发生的里氏 6.2 级地震已经造成至少 5427 人死亡，20000 余人受伤，近 20 万人无家可归";
        startIKAnalyzer(line);
    }
}
