package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTerm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DocumentTermMapper implements MapFunction<NewsArticle, DocumentTerm> {

    private static final long serialVersionUID = -2248837954996024589L;

    LongAccumulator termsCountAccumulator;
    LongAccumulator newsCountAccumulator;


    public DocumentTermMapper(LongAccumulator termsCountAccumulator, LongAccumulator newsCountAccumulator) {
        super();
        this.termsCountAccumulator = termsCountAccumulator;
        this.newsCountAccumulator = newsCountAccumulator;
    }

    @Override
    public DocumentTerm call(NewsArticle article) {
        newsCountAccumulator.add(1);
        // get title
        StringBuilder content = new StringBuilder(article.getTitle() + " ");
        // count paragraph
        int count = 0;
        for (ContentItem i : article.getContents()) {
            if (i == null) continue;
            String type = i.getSubtype();
            if (type != null)
                if (type.equals("paragraph")) {
                    content.append(i.getContent());
                    content.append(" ");
                    count++;
                }
            if (count == 5) break;

        }

        TextPreProcessor tpp = new TextPreProcessor();
        List<String> terms = tpp.process(content.toString());
        int length = terms.size();
        termsCountAccumulator.add(length);



        return new DocumentTerm(article, terms, length);
    }
}
