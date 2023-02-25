package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;

import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTerm;

import javax.print.Doc;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		//if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json";

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		LongAccumulator termAccumulator = spark.sparkContext().longAccumulator();
		LongAccumulator newsAccumulator = spark.sparkContext().longAccumulator();

//		TextPreProcessor tpp = new TextPreProcessor();
//		Broadcast<TextPreProcessor> bcTpp = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(tpp);
		// Map
		Encoder<DocumentTerm> documentTermEncoder = Encoders.bean(DocumentTerm.class);
		DocumentTermMapper documentTermMapper = new DocumentTermMapper(termAccumulator, newsAccumulator);
		Dataset<DocumentTerm> documentTerm = news.map(documentTermMapper, documentTermEncoder);

		// Map
		Encoder<Map> mapEncoder = Encoders.javaSerialization(Map.class);
		TFMapper tfMapper = new TFMapper();
		Dataset<Map> tf = documentTerm.map(tfMapper, mapEncoder);

		// Reduce
		@SuppressWarnings("unchecked")
		Map<String, Integer> termFrequency = tf.reduce(new TFReducer());

		long totalDocsInCorpus = newsAccumulator.value();
		long totalLength = termAccumulator.value();
		double averageDocumentLengthInCorpus = totalLength * 1.0 / totalDocsInCorpus;

		// Map
		QueryMapper queryMapper = new QueryMapper();
		Dataset<String> queryTerms = queries.flatMap(queryMapper, Encoders.STRING());
		Set<String> queryTermsSet = new HashSet<>(queryTerms.collectAsList());

		Broadcast<Map<String, Integer>> bcTF = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termFrequency);
		Broadcast<Set<String>> bcQueryTerms = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryTermsSet);
		// Map
		Encoder<Tuple3<DocumentTerm, String, Double>> termScoreEncoder = Encoders.tuple(Encoders.bean(DocumentTerm.class), Encoders.STRING(), Encoders.DOUBLE());
		TermScoreMapper termScoreMapper = new TermScoreMapper(bcQueryTerms, bcTF, totalDocsInCorpus, averageDocumentLengthInCorpus);
		Dataset<Tuple3<DocumentTerm, String, Double>> termScore = documentTerm.flatMap(termScoreMapper, termScoreEncoder);

		Broadcast<List<Tuple3<DocumentTerm, String, Double>>> bcTermsScore = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termScore.collectAsList());
		// Map
		Encoder<Tuple3<Query, DocumentTerm, Double>> tuple3Encoder = Encoders.tuple(Encoders.bean(Query.class), Encoders.bean(DocumentTerm.class), Encoders.DOUBLE());
		QueryTermScoreMapper queryTermScoreMapper = new QueryTermScoreMapper(bcTermsScore);
		Dataset<Tuple3<Query, DocumentTerm, Double>> queryTermScore = queries.flatMap(queryTermScoreMapper, tuple3Encoder);

		// group by query and doc
		Encoder<Tuple2<Query, DocumentTerm>> tuple2Encoder = Encoders.tuple(Encoders.bean(Query.class), Encoders.bean(DocumentTerm.class));
		GroupByQueryDocumentMapper groupByQueryDocumentMapper = new GroupByQueryDocumentMapper();
		KeyValueGroupedDataset<Tuple2<Query, DocumentTerm>, Tuple3<Query, DocumentTerm, Double>> groupByQueryDocument = queryTermScore.groupByKey(groupByQueryDocumentMapper, tuple2Encoder);

		// Map
		Encoder<Tuple3<Query, DocumentTerm, Double>> encoder = Encoders.tuple(Encoders.bean(Query.class), Encoders.bean(DocumentTerm.class), Encoders.DOUBLE());
		Dataset<Tuple3<Query, DocumentTerm, Double>> queryAvgScore = groupByQueryDocument.mapGroups(new AverageScoreMapper(), encoder);

		// Group by Query
		Encoder<Query> queryEncoder = Encoders.bean(Query.class);
		KeyValueGroupedDataset<Query, Tuple3<Query, DocumentTerm, Double>> groupByQuery = queryAvgScore.groupByKey(new GroupByQueryMapper(), queryEncoder);

		// map the value
		QueryRankingMapper queryRankingMapper = new QueryRankingMapper();
		KeyValueGroupedDataset<Query, DocumentRanking> queryRankingMap = groupByQuery.mapValues(queryRankingMapper, Encoders.bean(DocumentRanking.class));

		// Reduce By key
		Dataset<Tuple2<Query, DocumentRanking>> queryRanking = queryRankingMap.reduceGroups(new QueryRankingReducer());

		// Map
		Dataset<DocumentRanking> res = queryRanking.map(new TupleRankingMapper(), Encoders.bean(DocumentRanking.class));

		// collect
		List<DocumentRanking> documentRankings = res.collectAsList();

		return documentRankings; // replace this with the the list of DocumentRanking output by your topology
	}
	
	
}
