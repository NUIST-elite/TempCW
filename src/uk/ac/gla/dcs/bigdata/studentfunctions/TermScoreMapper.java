package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTerm;

import java.util.*;

public class TermScoreMapper implements FlatMapFunction<DocumentTerm, Tuple3<DocumentTerm, String, Double>> {
    private static final long serialVersionUID = 8549220726959146108L;

    private Broadcast<Set<String>> bcQueryTerms;

    private Broadcast<Map<String, Integer>> bcTF;

    private long totalDocsInCorpus;

    private double averageDocumentLengthInCorpus;

    public TermScoreMapper(Broadcast<Set<String>> bcQueryTerms, Broadcast<Map<String, Integer>> bcTF, long totalDocsInCorpus, double averageDocumentLengthInCorpus) {
        this.bcQueryTerms = bcQueryTerms;
        this.bcTF = bcTF;
        this.totalDocsInCorpus = totalDocsInCorpus;
        this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Broadcast<Set<String>> getBcQueryTerms() {
        return bcQueryTerms;
    }

    public void setBcQueryTerms(Broadcast<Set<String>> bcQueryTerms) {
        this.bcQueryTerms = bcQueryTerms;
    }

    public Broadcast<Map<String, Integer>> getBcTF() {
        return bcTF;
    }

    public void setBcTF(Broadcast<Map<String, Integer>> bcTF) {
        this.bcTF = bcTF;
    }

    public long getTotalDocsInCorpus() {
        return totalDocsInCorpus;
    }

    public void setTotalDocsInCorpus(long totalDocsInCorpus) {
        this.totalDocsInCorpus = totalDocsInCorpus;
    }

    public double getAverageDocumentLengthInCorpus() {
        return averageDocumentLengthInCorpus;
    }

    public void setAverageDocumentLengthInCorpus(double averageDocumentLengthInCorpus) {
        this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
    }

    @Override
    public Iterator<Tuple3<DocumentTerm, String, Double>> call(DocumentTerm documentTerm) throws Exception {
        Set<String> queryTerms = bcQueryTerms.value();
        Map<String, Integer> tfMap = bcTF.value();
        Map<String, Integer> docTf = documentTerm.getTf();

        List<Tuple3<DocumentTerm, String, Double>> res = new ArrayList<>();
        for (String term: queryTerms) {
            int currentDocumentLength = documentTerm.getLength();
            if (currentDocumentLength == 0 || !tfMap.containsKey(term) || !docTf.containsKey(term)) {
                continue;
            }

            int totalTermFrequencyInCorpus = tfMap.get(term);
            short termFrequencyInCurrentDocument = docTf.get(term).shortValue();

            double score = DPHScorer.getDPHScore(termFrequencyInCurrentDocument,
                    totalTermFrequencyInCorpus,
                    currentDocumentLength,
                    averageDocumentLengthInCorpus,
                    totalDocsInCorpus);
            Tuple3<DocumentTerm, String, Double> bundle = new Tuple3<>(documentTerm,term, score);
            res.add(bundle);
        }

        return res.iterator();
    }
}
