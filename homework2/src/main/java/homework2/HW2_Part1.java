package homework2;

import java.io.Serializable;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

//import Stocks.Spark2Screener1;
import au.com.bytecode.opencsv.CSVReader;
import scala.Tuple2;

/**
 * Part 1 - Extract the information we want from the file
 * 
 * Use an accumulator to count all the valid and invalid records in the file for the S&P 500
 * 
 * List the <Symbol, Dividend Yield, Price/Earning> information as output.
 * 
 * Make sure you filter out anything that is not a valid entry
 * 
 * ---------------------------------------
 * 
 * Input: companies/SP500-constituents-financials.csv
 * 
 * **Look at the file**, the first line is a header, listing the information in each column
 * 
 * Output a list containing a Tuple3 of <Symbol, Dividend Yield, Price/Earnings>
 * 
 * Output the number of valid records in the file - is it 500?
 * 
 * ----------------------------------------
 * 
 * @author SIMING MENG
 *
 */
public class HW2_Part1 {
	static Logger logger = Logger.getLogger(HW2_Part1.class);
	private final static String recordRegex = ",";
	private final static Pattern REGEX = Pattern.compile(recordRegex);
	
	private static final int SYMBOL_INDEX = 0;
	private static final int DIVIDEND_INDEX = 4;
	private static final int PE_INDEX = 5;
	/*
	 * TODO initialize the indices for parsing - symbol is at index 0, dividend is at index 4, price-earnings is at
	 * index 5
	 */
	private static class ParseLine implements PairFunction<String, String, String[]> {
		@Override
		public Tuple2<String, String[]> call(String line) throws Exception {
			CSVReader reader = new CSVReader(new StringReader(line));
			String[] elements = reader.readNext();
			String key = elements[0];
			
			String[] symbolDetails= elements;
			if (symbolDetails.length==0 || symbolDetails[SYMBOL_INDEX].equalsIgnoreCase("symbol"))
				return new Tuple2<String, String[]>("",null);
				
			String[] details = new String[3];
			details[0] = symbolDetails[SYMBOL_INDEX];
			
			if (symbolDetails[DIVIDEND_INDEX] == null )
				details[1] = "null";
			else if (symbolDetails[DIVIDEND_INDEX].isEmpty())
				details[1] = "0.0";
			else
				details[1] = symbolDetails[DIVIDEND_INDEX];
			
			if (symbolDetails[PE_INDEX] == null )
				details[2] = "null";
			else if (symbolDetails[PE_INDEX].isEmpty())
				details[2] = ""+Float.NEGATIVE_INFINITY;
			else
				details[2] = symbolDetails[PE_INDEX];
			
			try {
				Float div = Float.parseFloat(details[1]);
				Float pe = Float.parseFloat(details[2]);
			}
			catch (NumberFormatException nfe){
				logger.error("NumberFormatException:");
				return new Tuple2<String, String[]>("",null);
			}
			return new Tuple2<String, String[]>(key, elements);
		}
	}

	/*
	 * TODO initialize a String for representing non-existent information
	 * 
	 * In the file, the dividend and price-to-earnings information may be blank ("")
	 */

	/*
	 * TODO initialize a String for representing the header
	 * 
	 * - for instance, you can check the symbol field, if it equals "Symbol" you know you are parsing the header
	 */

	/*
	 * You may want a testing flag so you can switch off debugging information easily
	 */
	private static boolean testing = true;

	/**
	 * In main I have supplied basic information for starting the job in Eclipse.
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		/*
		 * Validate that two arguments were passed from the command line.
		 */
		if (args.length != 2) {
			System.out.println("Arguments provided:  ");
			for (String arg : args) {
				System.out.println(arg);
			}
			System.out.printf("Usage: Provide <input dir> <output dir> \n");
			System.out.printf("Example: data/companies/SP500-constituents-financials.csv output/hw2_1 \n");
			System.exit(-1);
		}
		HW2_Part1 part1 = new HW2_Part1();
		part1.run( args);

	}
	
	public void run(String[] args) throws Exception {
		/*
		 * setup output
		 */
		String outputPath = args[1] + "_" + Calendar.getInstance().getTimeInMillis();

		/*
		 * TODO setup accumulators for counting records - for instance, you may want to count valid records, invalid
		 * records, the number of records with no dividend supplied, etc.
		 */

		/*
		 * setup job configuration and context
		 */
		SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.setMaster("local");
		conf.setAppName("HW2 Part 1");
		JavaSparkContext sc = new JavaSparkContext(conf);

		/*
		 * Read the lines in a file
		 */
		JavaRDD<String> lines = sc.textFile(args[0]);

		/*
		 * if testing, cache the file and then check out the first ten lines in the file
		 */
		if (testing) {
			/*
			 * Show the header - notice, it is the first line in the file, so we can use first() - an action.
			 * 
			 * Data that is not cached is discarded after an action, so we should cache here or Spark will read the file
			 * again to recreate lines for the next action.
			 */
			lines.cache();
			System.out.println(lines.first());
		}

		/*
		 * - TODO use map to parse each line to a Tuple3 containing <Symbol, Dividend Yield, Price/Earnings>
		 * 
		 * Notice: There are 15 fields, separated by commas, on each line of the file
		 * 
		 * Your map function may start like this:
		 * 
		 * JavaRDD<Tuple3<String, String, String>> stockInfo = lines .map(new Function<String, Tuple3<String, String,
		 * String>>() {
		 */
		
		//JavaRDD<String> csvWithQuotesRecords = sc.textFile(csvWithQuotesFile);
		JavaRDD<String> csvRecords = lines;
		JavaPairRDD<String, String[]> keyedRDD1 = csvRecords.mapToPair(new ParseLine());
		//JavaPairRDD<String, String[]> keyedRDD2 = csvWithQuotesRecords.mapToPair(new ParseLine());
		
		keyedRDD1.cache();
		
		logger.info("--->Number of valid records for SP500:  " + keyedRDD1.count());
		//for (Tuple2<String, Float> element : keyedRDD1)
			//System.out.println(element._1 + "," + element._2);

		/**
		JavaPairRDD<String, String[]> pairs = lines.mapToPair(new ParseLine<String, String, String[]>() {
			private final static String recordRegex = ",";
			
			@Override
			public Tuple2<String, String[]> call(String value) throws Exception {
				if (value == null || value.isEmpty())
					return new Tuple2<String, String[]>("",null);
					
				String[] symbolDetails= value.split(recordRegex);
				if (symbolDetails.length==0 || symbolDetails[SYMBOL_INDEX].equalsIgnoreCase("symbol"))
					return new Tuple2<String, String[]>("",null);
					
				String[] details = new String[3];
				details[0] = symbolDetails[SYMBOL_INDEX];
				details[1] = symbolDetails[DIVIDEND_INDEX];
				details[2] = symbolDetails[PE_INDEX];
				return new Tuple2<String, String[]>(details[0], details);
 			}
		});**/
		

		/*-
		 * TODO Filter out invalid records 
		 * 
		 * - filter out bad fields - dividend and price-earning fields that don't contain floats
		 * - filter out the header
		 * 
		 * - use the filter as an opportunity to count valid and invalid records
		 *
		 * Your filter may start like this:
		 * 
		 * JavaRDD<Tuple3<String, String, String>> filteredInfo = stockInfo
		 *		.filter(new Function<Tuple3<String, String, String>, Boolean>() {
		 */

		/*-
		 * TODO You may want to sort the tuples before you write them to file.
		 * 
		 * To do so, you can use sortBy using the first element, symbol, found in filteredInfo._1()
		 * 
		 * This is just so the output list is easier to read/decipher. Note, it does initiate a "wide-transformation"
		 * 
		 * --- this sortBy implementation worked for me... -------------
		 *
		 * JavaRDD<Tuple3<String, String, String>> sortedInfo = filteredInfo
		 *		.sortBy(new Function<Tuple3<String, String, String>, String>() {
		 *
		 *			@Override
		 *			public String call(Tuple3<String, String, String> info) throws Exception {
		 *				return info._1();
		 *			}
		 *
		 *		}, true, 1);
		 */

		/*-
		 * and action! 
		 * TODO  write the information out to a file using "saveAsTextFile"
		 * 
		 */

		/*
		 * TODO Now that an action has run, the accumulators will be defined You can view them using something like
		 * this:
		 * 
		 * System.out.println("Valid records:  " + validRecords.value());
		 */

		/*
		 * bye
		 */
		sc.close();
	}
}
