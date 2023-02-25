package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class TupleRankingMapper implements MapFunction<Tuple2<Query, DocumentRanking>, DocumentRanking> {
    private static final long serialVersionUID = -805455476577289555L;

    @Override
    public DocumentRanking call(Tuple2<Query, DocumentRanking> tuple) throws Exception {
        return tuple._2();
    }
}
