package neu.edu.yelp.recommendation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class YelpDriver {

	public static final int INFINITY = 999;
	public static final String TEMP_FOLDER = "temp/";

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		final String OUT = TEMP_FOLDER + "output";

		if (args.length < 4) {
			System.out
					.println("Usage: YelpDriver <input business> <input reviews> "
							+ "<input users> <input userID> <degree of friends>");
			System.exit(-1);
		}

		Job job = new Job(conf, "One to Many business Inner Join reviews");

		job.setJarByClass(YelpDriver.class);
		job.setMapOutputKeyClass(TaggedKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPaths(job, new Path(args[0]) + ","
				+ new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(TEMP_FOLDER + "cPath"));
		job.setMapperClass(CombineBusinessReviewsMapper.class);
		job.setReducerClass(CombineBusinessReviewsReducer.class);
		job.setPartitionerClass(TaggedJoiningPartitioner.class);
		System.out.println("before wait for completion job");
		if (!job.waitForCompletion(true)) {
			System.exit(1);
		}

		String fPath = args[2];
		String cPath = TEMP_FOLDER + "cPath/part-r-00000";
		String userId = args[3];
		int degree = 0;

		try {
			degree = Integer.parseInt(args[4]);
		} catch (Exception e) {
			degree = 3;
		}

		conf.set("person", userId);
		conf.setInt("degree", degree);

		Job job1 = new Job(conf, "Users Friends adjacency List");
		job1.setJarByClass(YelpDriver.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(fPath));
		FileOutputFormat.setOutputPath(job1, new Path(TEMP_FOLDER
				+ "friendsList"));

		job1.setMapperClass(FriendsAdjacencyListMapper.class);
		job1.setReducerClass(FriendsAdjacencyListReducer.class);
		System.out.println("before wait for completion job1");
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}

		// iterate to get shortest path from others
		String output = OUT + System.nanoTime();
		String input = TEMP_FOLDER + "friendsList/part-r-00000";
		int count = 1;
		conf.setInt("infinity", INFINITY);

		while (count <= degree) {
			Job job2 = new Job(conf, "Transitivity iteration Degree" + count);
			job2.setJarByClass(YelpDriver.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setMapperClass(TransitivityMapper.class);
			job2.setReducerClass(TransitivityReducer.class);
			FileInputFormat.addInputPath(job2, new Path(input));
			FileOutputFormat.setOutputPath(job2, new Path(output));

			System.out.println("before wait for completion job 2");
			if (!job2.waitForCompletion(true)) {
				System.exit(1);
			}

			count++;
			input = output + "/part-r-00000";
			input = output;
			output = OUT + System.nanoTime();
		}

		Job job3 = new Job(conf, "yelp Transitive Matrix");
		job3.setJarByClass(YelpDriver.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path(input));
		FileOutputFormat.setOutputPath(job3, new Path(TEMP_FOLDER
				+ "myTransitiveMatrix"));
		job3.setMapperClass(YelpTransitiveMatrixMapper.class);
		job3.setReducerClass(YelpTransitiveMatrixReducer.class);
		System.out.println("before wait for completion job 3");
		if (!job3.waitForCompletion(true)) {
			System.exit(1);
		}

		// Job job4 = new Job(conf, "yelp similarity");
		// job4.setJarByClass(YelpDriver.class);
		//
		// job4.setOutputKeyClass(Text.class);
		// job4.setOutputValueClass(Text.class);
		// FileInputFormat.addInputPath(job4, new Path(cPath));
		// FileOutputFormat.setOutputPath(job4, new Path(TEMP_FOLDER
		// + "mySimilarityList"));
		// job4.setMapperClass(SimilarityMatrixMapper.class);
		// job4.setReducerClass(SimilarityMatrixReducer.class);
		// System.out.println("before wait for completion job 4");
		// if (!job4.waitForCompletion(true)) {
		// System.exit(1);
		// }

		Job job4 = new Job(conf, "yelp similarity");
		job4.setJarByClass(YelpDriver.class);

		job4.setMapOutputKeyClass(TaggedKey.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputKeyClass(NullWritable.class);
		job4.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job4, new Path(cPath));
		FileOutputFormat.setOutputPath(job4, new Path(TEMP_FOLDER
				+ "mySimilarityList"));
		job4.setMapperClass(ItemSimilarityMapper.class);
		job4.setReducerClass(ItemSimilarityReducer.class);
		job4.setPartitionerClass(TaggedJoiningPartitioner.class);
		System.out.println("before wait for completion job 4");
		if (!job4.waitForCompletion(true)) {
			System.exit(1);
		}

		Job job5 = new Job(conf, "yelp similarity list");
		job5.setJarByClass(YelpDriver.class);

		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job5, new Path(TEMP_FOLDER
				+ "mySimilarityList"));
		FileOutputFormat.setOutputPath(job5, new Path(TEMP_FOLDER
				+ "mySimilarityList1"));
		job5.setMapperClass(SimilarityMapper.class);
		job5.setNumReduceTasks(0);
		// job6.setReducerClass(ItemSimilarityReducer.class);
		System.out.println("before wait for completion job 6");
		if (!job5.waitForCompletion(true)) {
			System.exit(1);
		}

		Job job6 = new Job(conf, "yelp Recommendation Matrix");
		job6.setJarByClass(YelpDriver.class);
		job6.setOutputKeyClass(Text.class);
		job6.setOutputValueClass(Text.class);
		FileInputFormat.addInputPaths(job6, new Path(TEMP_FOLDER
				+ "mySimilarityList1/part-m-00000")
				+ "," + new Path(TEMP_FOLDER + "myTransitiveMatrix"));
		FileOutputFormat.setOutputPath(job6, new Path("myRecommendations"));
		job6.setNumReduceTasks(1);
		job6.setMapperClass(YelpRecommendationMapper.class);
		job6.setReducerClass(YelpRecommendationReducer.class);
		System.out.println("before wait for completion job 5");
		if (!job6.waitForCompletion(true)) {
			System.exit(1);
		}
	}

}
