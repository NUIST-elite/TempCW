package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.util.Iterator;

public class QueryMapper implements FlatMapFunction<Query, String> {
    private static final long serialVersionUID = 2032676937203475038L;

    @Override
    public Iterator<String> call(Query query) throws Exception {
        return query.getQueryTerms().iterator();
    }
}
