package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DocumentTerm implements Serializable {

    private static final long serialVersionUID = 2971103659210399973L;

    private NewsArticle news;

    private List<String> terms;


    private int length;

    public DocumentTerm() {
    }

    public DocumentTerm(NewsArticle news, List<String> terms, int length) {
        this.news = news;
        this.terms = terms;
        this.length = length;
    }

    public Map<String, Integer> getTf() {
        Map<String, Integer> tf = new HashMap<>();
        for (String t : terms) {
            if (tf.containsKey(t)) {
                tf.put(t, tf.get(t) + 1);
            } else {
                tf.put(t, 1);
            }
        }
        return tf;
    }

    public List<String> getTerms() {
        return terms;
    }

    public void setTerms(List<String> terms) {
        this.terms = terms;
    }

    public NewsArticle getNews() {
        return news;
    }

    public void setNews(NewsArticle news) {
        this.news = news;
    }

    public int getLength() {
        return this.length;
    }

    public void setLength(int length) {
        this.length = length;
    }


}
