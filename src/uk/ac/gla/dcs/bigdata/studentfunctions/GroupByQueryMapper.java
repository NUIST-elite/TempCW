package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTerm;

public class GroupByQueryMapper implements MapFunction<Tuple3<Query, DocumentTerm, Double>, Query> {
    private static final long serialVersionUID = -3383254740686430825L;

    @Override
    public Query call(Tuple3<Query, DocumentTerm, Double> queryDocumentTermDoubleTuple3) throws Exception {
        return queryDocumentTermDoubleTuple3._1();
    }
}
