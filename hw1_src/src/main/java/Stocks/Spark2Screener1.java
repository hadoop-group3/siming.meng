package Stocks;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;

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
	final private static String separator = "===";
	public final static String NO_INFO = "n/a";
	public final static double BILLION = 1000000000.0;
	public final static double MILLION = 1000000.0;
	
	public static void main(String[] args) throws IOException, URISyntaxException {

		if (args.length < 2) {
			System.err.println("Usage: <inputPath> <outputPath>");
			System.exit(1);
		}

		// For summary reports
		//HashMap<String, Integer> sectorCompanyCount = new HashMap<String, Integer>();
		//HashMap<String, Double> sectorCapTotal = new HashMap<String, Double>();
		
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
			
				/** For Billion cap companies
				if (tokens.length != 9 || symbol.equalsIgnoreCase("Symbol") || tokens[sectorIndex].equalsIgnoreCase("Sector") 
						|| sectorStr.equalsIgnoreCase(NO_INFO) || !capStr.endsWith("B")) {
					finalTokens[0] = "";
				}
				else
					finalTokens[0] = s; // PASS IT ON
					***/
				if ( sectorStr.equalsIgnoreCase("\"Sector\"") || sectorStr.equalsIgnoreCase(NO_INFO)
						|| symbol.equalsIgnoreCase("\"Symbol\"") || capStr.endsWith("B") ) {		
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
			
			// For summary reports
			public HashMap<String, Integer> sectorCompanyCount = new HashMap<String, Integer>();
			public HashMap<String, Double> sectorCapTotal = new HashMap<String, Double>();
			
			@Override
			public Tuple2<String, String> call(String value) throws Exception {
				if (value == null || value.isEmpty())
					return new Tuple2<String, String>("","");

				//logger.info("stock=["+value);
				String[] tokens = value.toString().split(recordRegex, -1);
				String symbol = tokens[0];
				String sectorStr = tokens[5];
				String capStr = tokens[capIndex].indexOf(NO_INFO)>-1?"0.00": tokens[capIndex].replace("\"", "");
				capStr = (capStr.substring(1, capStr.length())).replaceAll("M", "");

				double finalCap =  new Double(capStr);
				
				Integer companyCount = sectorCompanyCount.get(sectorStr);
				Double capTotal = sectorCapTotal.get(sectorStr);
				
				if (companyCount == null)
					companyCount=1;
				else 
					++companyCount;

				sectorCompanyCount.put(sectorStr, companyCount);
				
				if (capTotal == null)
					capTotal = finalCap * MILLION;
				else 
					capTotal += finalCap * MILLION;
				
				sectorCapTotal.put(sectorStr, capTotal);
				
				return new Tuple2<String, String>(sectorStr,symbol + separator +companyCount+ separator +capTotal);
			}
		});

		/*
		 * define a reduceByKey by providing an *associative* operation that can
		 * reduce any 2 values down to 1 (e.g. two integers sum to one integer).
		 * The operation reduces all values for each key to one value.
		 */
		JavaPairRDD<String, String> counts = pairs.reduceByKey(new Function2<String, String, String>() {
			private double sectorTotalCap = 0.0;
			private int companyCount = 0;
			//final String separator = "===";
			final String companyCountStr = "\nTotal Companies: ";
			final String sectorTotalCapStr = "\nTotal Market Cap: ";

			@Override
			public String call(String a, String b) throws Exception {
				//logger.info("reduceByKey a=[" + a + "] b=["+ b);
				if (a.isEmpty() && b.isEmpty())
					return "";
				else
				{	
					String aSymbol = parseSymbolAddCap(a);
					String bSymbol = parseSymbolAddCap(b);
					String mergedString = updateSummaryForSector(aSymbol, bSymbol);
					
					//Need clean up the cache for calculating other sectors
					sectorTotalCap = 0.0;
					companyCount = 0;
					return mergedString;
				}
			}
			private String updateSummaryForSector(String a, String bSymbol)
			{
				String outputString;
				if (a== null || a.isEmpty())
					return bSymbol;
				int pos = a.indexOf(this.companyCountStr);
				if (pos > 0) //  found-> remove the old summary report, insert bSymbol here.
					outputString = a.substring(0, pos) + ", " + bSymbol;
				else 
					outputString = a + ", " + bSymbol;
				
				// Append summary report for this sector so far
				outputString += companyCountStr + companyCount;
				NumberFormat dFormat = NumberFormat.getCurrencyInstance();
				outputString +=  sectorTotalCapStr + dFormat.format(sectorTotalCap) ;
				
				return outputString;
			}
			private String parseSymbolAddCap(String value) {
				if (value == null || value.isEmpty())
					return "";
				
				if (value.indexOf(separator)<0) // existing string w/o the separator. so return w/o processing
					return value;
				String symbol = "";
				String[] symbolDetails = value.toString().split(separator);
				
				String capStr = symbolDetails[2];
				String companyCntStr = symbolDetails[1];
				symbol = symbolDetails[0];
				
				Double tempSectorTotalCap = new Double(capStr);
				Integer tempCompanyCount = new Integer(companyCntStr);
				
				if (companyCount <=tempCompanyCount) //start storing the largest count for reporting
				{
					companyCount = tempCompanyCount;
					sectorTotalCap = tempSectorTotalCap;
				}
				
				return symbol;
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
