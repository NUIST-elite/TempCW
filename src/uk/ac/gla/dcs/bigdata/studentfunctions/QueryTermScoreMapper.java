package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple3;
import scala.Tuple4;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTerm;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QueryTermScoreMapper implements FlatMapFunction<Query, Tuple3<Query, DocumentTerm, Double>> {
    private static final long serialVersionUID = 69069678273824493L;

    private Broadcast<List<Tuple3<DocumentTerm, String, Double>>> bcTermScore;

    public QueryTermScoreMapper(Broadcast<List<Tuple3<DocumentTerm, String, Double>>> bcTermScore) {
        this.bcTermScore = bcTermScore;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Broadcast<List<Tuple3<DocumentTerm, String, Double>>> getBcTermScore() {
        return bcTermScore;
    }

    public void setBcTermScore(Broadcast<List<Tuple3<DocumentTerm, String, Double>>> bcTermScore) {
        this.bcTermScore = bcTermScore;
    }

    @Override
    public Iterator<Tuple3<Query, DocumentTerm, Double>> call(Query query) throws Exception {
        List<Tuple3<DocumentTerm, String, Double>> termsScore = bcTermScore.value();
        List<String> queryTerms = query.getQueryTerms();
        short[] queryTermCounts = query.getQueryTermCounts();

        List<Tuple3<Query, DocumentTerm, Double>> res = new ArrayList<>(0);
        for (Tuple3<DocumentTerm, String, Double> tuple : termsScore) {
            int index = queryTerms.indexOf(tuple._2());
            if(index != -1) {
                int count = queryTermCounts[index];
                double score = tuple._3() * count;
                Tuple3<Query, DocumentTerm, Double> bundle = new Tuple3<>(query, tuple._1(), score);
                res.add(bundle);
            }
        }

        return res.iterator();
    }
}
