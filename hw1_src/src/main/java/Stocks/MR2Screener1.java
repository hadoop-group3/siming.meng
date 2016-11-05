package Stocks;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Calendar;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class MR2Screener1 {
	static Logger logger = Logger.getLogger(MR2Screener1.class);

	public static class SectorReducer extends Reducer<Text, Text, Text, Text> {
		private final static String SECTOR_COUNT_LABEL = "SectorCount";

		/**
		 * The reduce method runs once for each key received from the shuffle
		 * and sort phase of the MapReduce framework. The method receives:
		 *
		 * @param Text
		 *            key type
		 * @param IntWritable
		 *            values type
		 * @param Context
		 *            info about the job's config, writers for output
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			//logger.info("In reduce()");
			StringBuffer symbols = new StringBuffer();
			double sectorTotalCap = 0.0;
			int companyCount = 0;

			for (Text value : values) {
				//logger.info("key=[" + key + "] value=" + value);
				String[] symbolDetails = value.toString().split("===");
				
				if (!values.iterator().hasNext() && symbolDetails.length<2)
				{
					String overallCompanyCntLabel = "Total companies attempted";
					context.write(new Text(overallCompanyCntLabel), new Text(symbolDetails[0]));
					return;
				}
				String capStr = symbolDetails[1];
				
				sectorTotalCap += new Double(capStr);

				symbols.append(", " + symbolDetails[0]);
				companyCount++;
			}

			// Counter totalCompanies = context.getCounter(key.toString());
			Text KEY = new Text("Sector: " + key);
			NumberFormat dFormat = NumberFormat.getCurrencyInstance();
			StringBuffer finalOutput = new StringBuffer();
			finalOutput.append("\nSymbols: "+ symbols.toString().substring(2, symbols.length()));
			finalOutput.append("\nTotal Companies: "+ companyCount + "; \nTotal Market Cap: "
					+ dFormat.format(sectorTotalCap) );
			context.write(KEY, new Text(finalOutput.toString()+"\n"));
		}
	}

	/**
	 * To define a map function for your MapReduce job, subclass the Mapper
	 * class and override the map method. The class definition requires four
	 * parameters:
	 *
	 * @param LongWritable
	 *            type of input key
	 * @param Text
	 *            type of input value
	 * @param Text
	 *            type of output key (same type for Reducer input key)
	 * @param IntWritable
	 *            type of output value (same type for Reducer input value)
	 */
	public static class SectorMapperWithCounter extends Mapper<Object, Text, Text, Text> {
		private final static String recordRegex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
		private final static String SECTOR_COUNT_LABEL = "SectorCount";
		private final static int sectorIndex = 5;
		private final static int capIndex = 3;
		public final static String NO_INFO = "n/a";
		private final double BILLION = 1000000000.0;
		private final double MILLION = 1000000.0;

		/*-
		 * This function splits each input line into an array of words.
		 * For each word in the array is output as <word, 1>
		 *
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] tokens = value.toString().split(recordRegex, -1);
			String symbol = tokens[0];
			String sectorStr = tokens[5];
			String capStr = tokens[capIndex].replace("\"", "");
			// logger.info("key=[" + key + ", " + value.toString());
			// logger.info("Token len=" + tokens.length + " sector=[" +
			// sectorStr + "] capStr=[" + capStr);

			/*** If Billion cap companies are included
			if (tokens.length == 9 && !tokens[1].equalsIgnoreCase("Sector") && !sectorStr.equalsIgnoreCase(NO_INFO)
					&& capStr.endsWith("B")) {		// used if Billion cap companies are excluded
				capStr = (capStr.substring(1, capStr.length())).replaceAll("B", "");
				double finalCap = new Double(capStr);
				context.write(new Text(sectorStr), new Text(symbol + "===" + finalCap * BILLION));
				// logger.info("Found a B-company");
				context.getCounter(SECTOR_COUNT_LABEL, sectorStr).increment(1);
				context.getCounter(SECTOR_COUNT_LABEL, "Total Billion Companies processed successfully").increment(1);
			}
			***/
			// used if Billion cap companies are excluded
			if (tokens.length == 9 && !sectorStr.equalsIgnoreCase("\"Sector\"") && !sectorStr.equalsIgnoreCase(NO_INFO)
					&& !symbol.equalsIgnoreCase("\"Symbol\"") && !capStr.endsWith("B") ) {		
				double finalCap = 0;
				
				if (!capStr.equalsIgnoreCase(NO_INFO))
				{
					capStr = (capStr.substring(1, capStr.length())).replaceAll("M", "");
					finalCap = new Double(capStr);
				}
				context.write(new Text(sectorStr), new Text(symbol + "===" + finalCap * MILLION));
				// logger.info("Found a M-company");
				context.getCounter(SECTOR_COUNT_LABEL, sectorStr).increment(1);
				context.getCounter(SECTOR_COUNT_LABEL, "Total Non-Billion Companies processed successfully").increment(1);
			}
			context.getCounter(SECTOR_COUNT_LABEL, "Total companies attempted").increment(1);
		}
		/***
		 * @Override protected void setup(Mapper<Object, Text, Text,
		 *           Text>.Context context) throws IOException,
		 *           InterruptedException { // TODO Auto-generated method stub
		 *           //super.setup(context); Configuration conf =
		 *           context.getConfiguration(); sector = }
		 ***/

		@Override
		protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
				
			String overallCompanyCntLabel = "Total companies attempted";
			Counter counter = context.getCounter(SECTOR_COUNT_LABEL, overallCompanyCntLabel);
			
			context.write(new Text(overallCompanyCntLabel), new Text(""+counter.getValue()));
		}
	}

	/**
	 * The main method defines and starts the word-count job
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		/*
		 * Validate that two arguments were passed from the command line.
		 */
		if (args.length != 2) {
			System.out.printf("Usage: Provide <input dir> <output dir>\n");
			logger.info("missing input");
			System.exit(-1);
		}

		// create job instance
		Job job = Job.getInstance();
		job.setJarByClass(MR2Screener1.class);
		job.setJobName("Stock Screener");

		// setup input and output
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// note, this creates a new output path using the time of creation
		String output = args[1] + "_" + Calendar.getInstance().getTimeInMillis();
		FileOutputFormat.setOutputPath(job, new Path(output));

		/*
		 * Specify the mapper and reducer classes.
		 */
		job.setMapperClass(SectorMapperWithCounter.class);
		job.setReducerClass(SectorReducer.class);

		/*
		 * Specify the number of reduce tasks
		 *
		 */
		job.setNumReduceTasks(1);

		/*
		 * Specify the job's output key and value classes.
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		boolean success = job.waitForCompletion(true);

		System.exit(success ? 0 : 1);
	}
}
