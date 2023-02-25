package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.ReduceFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class QueryRankingReducer implements ReduceFunction<DocumentRanking> {
    private static final long serialVersionUID = -640642651091597616L;

    @Override
    public DocumentRanking call(DocumentRanking d1, DocumentRanking d2) throws Exception {
        List<RankedResult> l1 = d1.getResults();
        List<RankedResult> l2 = d2.getResults();

        Iterator<RankedResult> iter1 = l1.iterator();
        while (iter1.hasNext()) {
            RankedResult r1 = iter1.next();
            Iterator<RankedResult> iter2 = l2.iterator();
            while (iter2.hasNext()) {
                RankedResult r2 = iter2.next();
                String t1 = r1.getArticle().getTitle();
                String t2 = r2.getArticle().getTitle();
                if (t1 == null || t1.isEmpty())  {
                    iter1.remove();
                    break;
                }
                if (t2 == null || t2.isEmpty()) {
                    iter2.remove();
                    continue;
                }
                if (TextDistanceCalculator.similarity(t1, t2) < 0.5) {
                    iter1.remove();
                    break;
                }
            }
        }

        l1.addAll(l2);
        Collections.sort(l1, Collections.reverseOrder());
        if (l1.size() <= 10) {
            return new DocumentRanking(d1.getQuery(), l1);
        } else {
            return new DocumentRanking(d1.getQuery(), l1.subList(0, 10));
        }
    }
}
