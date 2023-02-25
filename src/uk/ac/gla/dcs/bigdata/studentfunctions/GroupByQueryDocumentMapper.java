package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import scala.Tuple2;
import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTerm;

public class GroupByQueryDocumentMapper implements MapFunction<Tuple3<Query, DocumentTerm, Double>, Tuple2<Query, DocumentTerm>> {
    private static final long serialVersionUID = -5661659723198837945L;

    public GroupByQueryDocumentMapper() {
    }

    @Override
    public Tuple2<Query, DocumentTerm> call(Tuple3<Query, DocumentTerm, Double> queryDocumentTermStringDoubleTuple4) throws Exception {
        return new Tuple2<>(queryDocumentTermStringDoubleTuple4._1(), queryDocumentTermStringDoubleTuple4._2());
    }
}
