package homework2;

import java.io.Serializable;

import java.io.Serializable;
import java.io.StringReader;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import au.com.bytecode.opencsv.CSVReader;
import scala.Tuple2;
import org.apache.log4j.Logger;

public class HW2_Part2 implements Serializable {
	static Logger logger = Logger.getLogger(HW2_Part2.class);
	public static class ParseLine implements PairFunction<String, String, String[]> {
		@Override
		public Tuple2<String, String[]> call(String line) throws Exception {
			CSVReader reader = new CSVReader(new StringReader(line));
			String[] elements = reader.readNext();
			String key = elements[0];
			return new Tuple2<String, String[]>(key, elements);
		}
	}

	static class DividendComparator implements Comparator<Tuple2<String, Float>>, Serializable {

		final static DividendComparator INSTANCE = new DividendComparator();

		@Override
		public int compare(Tuple2<String, Float> kv1, Tuple2<String, Float> kv2) {

			Float value1 = kv1._2();
			Float value2 = kv2._2();

			return -value1.compareTo(value2); // sort descending

			// return value1[0].compareTo(value2[0]); // sort ascending
		}
	}

	private static final int DIVIDEND_INDEX = 4;
	private static final int YEARLY_LOW_INDEX = 8;
	private static final int YEARLY_HIGH_INDEX = 9;

	public static void main(String[] args) throws Exception {
		/*
		 * Validate that two arguments were passed from the command line.
		 */
		if (args.length != 3) {
			System.out.println("Arguments provided:  ");
			for (String arg : args) {
				System.out.println(arg);
			}
			System.out.printf("Usage: Provide <input file1> <input file2>  <output dir> \n");
			System.out.printf("Example: data/companies/SP500-constituents-financials.csv data/companies/companylistNASDAQ.csv output/hw2_2 \n");
			System.exit(-1);
		}
		HW2_Part2 part2 = new HW2_Part2();
		part2.run( args);

	}
	public void run(String[] args) throws Exception {
		String csvFile = args[0];
		String csvWithQuotesFile = args[1];
		SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.setMaster("local");
		conf.setAppName("HW2_Part2");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> csvRecords = sc.textFile(csvFile);
		JavaRDD<String> csvWithQuotesRecords = sc.textFile(csvWithQuotesFile);
		JavaPairRDD<String, String[]> keyedRDD1 = csvRecords.mapToPair(new ParseLine());
		JavaPairRDD<String, String[]> keyedRDD2 = csvWithQuotesRecords.mapToPair(new ParseLine());

		JavaPairRDD<String, Tuple2<String[], String[]>> joinResults = keyedRDD1.join(keyedRDD2);

		joinResults.cache();
		

		// answer to question: how many stocks in SP500 are on the NASDAQ
		int totalSymbolsOnBothMarket = (int) joinResults.count();
		System.out.println("Number of records on both the NASDAQ and the SP500:  " + totalSymbolsOnBothMarket);
		// print symbols on both markets
		List<Tuple2<String, Tuple2<String[], String[]>>> allSymbols = joinResults.collect();
		int commonStockCounter=0;
		for (Tuple2<String, Tuple2<String[], String[]>> symbol : allSymbols)
			System.out.println("Common stock[" + ++commonStockCounter + "]:" + symbol._1 );
		
		JavaPairRDD<String, Float> dividends = joinResults.mapValues(x -> {
			try {
				return Float.valueOf(x._1()[DIVIDEND_INDEX]);
			} catch (NumberFormatException e) {
				return 0f;
			}
		});

		List<Tuple2<String, Float>> top10Dividends = dividends.takeOrdered(10, DividendComparator.INSTANCE);

		for (Tuple2<String, Float> element : top10Dividends)
			System.out.println(element._1 + "," + element._2);

	}
}
