package Stocks;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Calendar;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple2;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
	
public class Spark2Screener1 {
	static Logger logger = Logger.getLogger(Spark2Screener1.class);

	public static void main(String[] args) throws IOException, URISyntaxException {

		if (args.length < 2) {
			System.err.println("Usage: <inputPath> <outputPath>");
			System.exit(1);
		}

		// setup job configuration and context
		SparkConf sparkConf = new SparkConf().setAppName("Stock Screener with Spark");
		sparkConf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// setup input and output
		JavaRDD<String> inputPath = sc.textFile(args[0], 1);
		// notice, this takes the output path and adds a date and time
		String outputPath = args[1] + "_" + Calendar.getInstance().getTimeInMillis();

		// parse the input line into an array of symbols
		JavaRDD<String> symbol = inputPath.flatMap(new FlatMapFunction<String, String>() {
			private final static String recordRegex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
			private final static int capIndex = 3;
			private final static int sectorIndex = 5;
			public final static String NO_INFO = "n/a";
			@Override
			public Iterable<String> call(String s) throws Exception {
				// initial screen
				logger.info("Iterable<String> call(String s) s=["+s);
				String[] tokens = s.split(recordRegex, -1);
				String symbol = tokens[0];
				String sectorStr = tokens[sectorIndex];
				String capStr = tokens[capIndex].replace("\"", "");
				String[] finalTokens=new String[1];
				
				if (tokens.length != 9 || symbol.equalsIgnoreCase("Symbol") || tokens[sectorIndex].equalsIgnoreCase("Sector") 
						|| sectorStr.equalsIgnoreCase(NO_INFO) || !capStr.endsWith("B")) {
					finalTokens[0] = "";
				}
				else
					finalTokens[0] = s; // PASS IT ON
				return Arrays.asList(finalTokens);
			}
		});

		// define mapToPair to create pairs of <word, 1>
		JavaPairRDD<String, String> pairs = symbol.mapToPair(new PairFunction<String, String, String>() {
			private final static String recordRegex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
			private final static int sectorIndex = 5;
			private final static int capIndex = 3;
			public final static String NO_INFO = "n/a";
			private final double BILLION = 1000000000.0;
			@Override
			public Tuple2<String, String> call(String value) throws Exception {
				if (value == null || value.isEmpty())
					return new Tuple2<String, String>("","");
				
				String[] tokens = value.toString().split(recordRegex, -1);
				String symbol = tokens[0];
				String sectorStr = tokens[5];
				String capStr = tokens[capIndex].replace("\"", "");
				capStr = (capStr.substring(1, capStr.length())).replaceAll("B", "");
				double finalCap = new Double(capStr);
				String finalValue= symbol + "===" + finalCap * BILLION;
				
				logger.info("Tuple2 call() tokens=["+ tokens);
				return new Tuple2<String, String>(sectorStr,finalValue);
			}
		});

		/*
		 * define a reduceByKey by providing an *associative* operation that can
		 * reduce any 2 values down to 1 (e.g. two integers sum to one integer).
		 * The operation reduces all values for each key to one value.
		 */
		JavaPairRDD<String, String> counts = pairs.reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String a, String b) throws Exception {
				logger.info("reduceByKey a=[" + a + "] b=["+ b);
				return a + ", "+ b;
			}
		});

		JavaPairRDD<String, String> sortedCounts = counts.sortByKey();

		/*-
		 * start the job by indicating a save action
		 * The following starts the job and tells it to save the output to outputPath
		 */
		sortedCounts.saveAsTextFile(outputPath);

		// done
		sc.close();
	}
}
