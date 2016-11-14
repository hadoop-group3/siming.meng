package homework2;

import java.util.Calendar;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
 * @author <enter your name here>
 *
 */
public class HW2_Part1 {

	private final static String recordRegex = ",";
	private final static Pattern REGEX = Pattern.compile(recordRegex);

	/*
	 * TODO initialize the indices for parsing - symbol is at index 0, dividend is at index 4, price-earnings is at
	 * index 5
	 */

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
	public static void main(String[] args) {
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

		/*
		 * setup job configuration and context
		 */
		SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.setMaster("local");
		conf.setAppName("HW2 Part 1");
		JavaSparkContext sc = new JavaSparkContext(conf);

		/*
		 * setup output
		 */
		String outputPath = args[1] + "_" + Calendar.getInstance().getTimeInMillis();

		/*
		 * TODO setup accumulators for counting records - for instance, you may want to count valid records, invalid
		 * records, the number of records with no dividend supplied, etc.
		 */

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
