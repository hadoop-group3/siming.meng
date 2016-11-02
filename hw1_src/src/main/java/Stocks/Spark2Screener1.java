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

import scala.Tuple2;
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
			@Override
			public Iterable<String> call(String s) throws Exception {
				String[] tokens = s.split(recordRegex, -1);
				return Arrays.asList(tokens);
			}
		});

		// define mapToPair to create pairs of <word, 1>
		JavaPairRDD<String, String> pairs = symbol.mapToPair(new PairFunction<Object, String, String>() {
			private final static int sectorIndex = 5;
			private final static int capIndex = 3;
			public final static String NO_INFO = "n/a";
			private final double BILLION = 1000000000.0;
			@Override
			public Tuple2<String, String> call(Object tokens) throws Exception {
				String[] tkns = (String[])tokens;
				return new Tuple2<String, String>(tkns[sectorIndex], tkns[0]+ "|||"+tkns[capIndex]);
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
				return a + b;
			}
		});

		JavaPairRDD<String, Integer> sortedCounts = counts.sortByKey();

		/*-
		 * start the job by indicating a save action
		 * The following starts the job and tells it to save the output to outputPath
		 */
		sortedCounts.saveAsTextFile(outputPath);

		// done
		sc.close();
	}
}
