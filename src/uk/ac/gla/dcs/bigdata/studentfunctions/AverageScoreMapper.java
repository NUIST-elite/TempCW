package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapGroupsFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTerm;

import java.util.Iterator;

public class AverageScoreMapper implements MapGroupsFunction<Tuple2<Query, DocumentTerm>, Tuple3<Query, DocumentTerm, Double>, Tuple3<Query, DocumentTerm, Double>> {
    @Override
    public Tuple3<Query, DocumentTerm, Double> call(Tuple2<Query, DocumentTerm> key, Iterator<Tuple3<Query, DocumentTerm, Double>> iterator) throws Exception {
        double score = 0;
        int count = 0;
        while (iterator.hasNext()) {
            score += iterator.next()._3();
            count += 1;
        }

        return new Tuple3<>(key._1(), key._2(), score / count);
    }
}
