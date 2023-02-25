package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTerm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TFMapper implements MapFunction<DocumentTerm, Map> {
    private static final long serialVersionUID = 4304909664755328608L;

    @Override
    public Map call(DocumentTerm documentTerm) throws Exception {
        return documentTerm.getTf();
    }
}
