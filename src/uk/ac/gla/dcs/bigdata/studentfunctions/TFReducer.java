package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.ReduceFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTerm;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TFReducer implements ReduceFunction<Map> {
    private static final long serialVersionUID = -7498361617183588515L;

    @Override
    public Map call(Map t1, Map t2) throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, Integer> map1 = t1;
        @SuppressWarnings("unchecked")
        Map<String, Integer> map2 = t2;

        map2.forEach((key, value) -> map1.merge(key, value, Integer::sum));
        return map1;
    }
}
