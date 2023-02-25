package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTerm;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QueryRankingMapper implements MapFunction<Tuple3<Query, DocumentTerm, Double>, DocumentRanking> {
    private static final long serialVersionUID = 519467971761562169L;

    @Override
    public DocumentRanking call(Tuple3<Query, DocumentTerm, Double> tuple) throws Exception {
        DocumentTerm documentTerm = tuple._2();
        NewsArticle news = documentTerm.getNews();
        RankedResult result = new RankedResult(news.getId(), news, tuple._3());
        List<RankedResult> list = new ArrayList<>();
        list.add(result);
        return new DocumentRanking(tuple._1(), list);
    }
}
