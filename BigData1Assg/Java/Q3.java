package bigdata.assignment1;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Q3 {

	
	public static class BusinessRatingMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String bdata[] = value.toString().split("\\^");
			context.write(new Text(bdata[2]),
					new Text((bdata[3])));

		}
	}

	
	public static class TopTenBusinessReducer extends
			Reducer<Text, Text, Text, Text> {

		private TreeMap<String, Float> tMap1 = new TreeMap<>();
		private RatingComparator ratingComparator = new RatingComparator(tMap1);
		private TreeMap<String, Float> mapSort = new TreeMap<String, Float>(
				ratingComparator);

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			float total = 0;
			float count = 0;
			for (Text value : values) {
				total += Float.parseFloat(value.toString());
				count++;
			}
			float average = new Float(total / count);
			tMap1.put(key.toString(), average);
		}

		
		class RatingComparator implements Comparator<String> {

			TreeMap<String, Float> tMap2;

			public RatingComparator(TreeMap<String, Float> j) {
				this.tMap2 = j;
			}

			public int compare(String x, String y) {
				if (tMap2.get(x) >= tMap2.get(y)) {
					return -1;
				} else {
					return 1;
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mapSort.putAll(tMap1);

			int c = 10;
			for (Entry<String, Float> e : mapSort.entrySet()) {
				if (c == 0) {
					break;
				}
				context.write(new Text(e.getKey()),
						new Text(String.valueOf(e.getValue())));
				c--;
			}
		}
	}

	
	public static class TopTenBusinessRatingMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String data = value.toString().trim();
			String[] dataDetails = data.split("\t");
			String n = dataDetails[0].trim();
			String r = dataDetails[1].trim();
			context.write(new Text(n), new Text("T1|" + n + "|" + r));

		}
	}

	
	public static class BusinessDetailMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String d[] = value.toString().split("\\^");
			context.write(new Text(d[0].trim()), new Text("T2|"
					+ d[0].trim() + "|" + d[1].trim()
					+ "|" + d[2].trim()));

		}
	}


	public static class TopTenBusinessDetailReducer extends
			Reducer<Text, Text, Text, Text> {

		private ArrayList<String> arrayList1 = new ArrayList<String>();
		private ArrayList<String> arrayList2 = new ArrayList<String>();
		private static String splitTo = "\\|";

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text rt : values) {
				String value = rt.toString();
				if (value.startsWith("T1")) {
					arrayList1.add(value.substring(3));
				} else {
					arrayList2.add(value.substring(3));
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (String bustop : arrayList1) {
				for (String busdetail : arrayList2) {
					String[] bst = bustop.split(splitTo);
					String bid = bst[0].trim();

					String[] bt2 = busdetail.split(splitTo);
					String bid2 = bt2[0].trim();

					if (bid.equals(bid2)) {
						context.write(new Text(bid), new Text(
								bt2[1] + "\t" + bt2[2] + "\t"
										+ bst[1]));
						break;
					}
				}
			}
		}
	}

	
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration config = new Configuration();
		String[] abArgs = new GenericOptionsParser(config, args)
				.getRemainingArgs();

		if (abArgs.length != 4) {
			System.err.println("Argument problem");
			System.err
					.println("hadoop jar <jarname> <classname> <input1> <input2> <intermediate_output> <final_output>");
			System.exit(0);
		}

		// Creating new job for businesses
		Job firstJob = Job.getInstance(config, "JOB1");
		firstJob.setJarByClass(Q3.class);
		firstJob.setMapperClass(BusinessRatingMapper.class);
		firstJob.setReducerClass(TopTenBusinessReducer.class);

		firstJob.setOutputKeyClass(Text.class);
		firstJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(firstJob, new Path(abArgs[0]));
		FileOutputFormat.setOutputPath(firstJob, new Path(abArgs[2]));

		boolean isJob1Completed = firstJob.waitForCompletion(true);

		if (isJob1Completed) {
			Configuration config2 = new Configuration();
			Job secondJob = Job.getInstance(config2, "JOB2");
			secondJob.setJarByClass(Q3.class);
			secondJob.setOutputKeyClass(Text.class);
			secondJob.setOutputValueClass(Text.class);
			secondJob.setInputFormatClass(TextInputFormat.class);
			secondJob.setOutputFormatClass(TextOutputFormat.class);

			// Set multiple Mappers
			MultipleInputs.addInputPath(secondJob, new Path(args[2]),
					TextInputFormat.class, TopTenBusinessRatingMapper.class);
			MultipleInputs.addInputPath(secondJob, new Path(args[1]),
					TextInputFormat.class, BusinessDetailMapper.class);

			// Single Reduce Class
			secondJob.setReducerClass(TopTenBusinessDetailReducer.class);
			FileOutputFormat.setOutputPath(secondJob, new Path(args[3]));

			secondJob.waitForCompletion(true);
		}
	}

}
