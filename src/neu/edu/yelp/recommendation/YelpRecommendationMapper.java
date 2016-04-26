package neu.edu.yelp.recommendation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YelpRecommendationMapper extends
		Mapper<LongWritable, Text, Text, Text> {


	public void map(LongWritable key, Text value, Context context) {
		try {
			String line = value.toString();
			if (line != null && line.length() > 0) {
//				System.out.println(line);
				if (line.split("\t")[0].length() == 1) {
					// transitive
					int distance = Integer.parseInt(line.split("\t")[0]);
					String otherId = line.split("\t")[1].split(" ")[1];
					context.write(new Text(otherId), new Text("d " + distance));	
//					System.out.println(otherId + " " + distance);
				} else if (line.split("\t")[0].length() > 1) {
					// similarity
					String otherId = line.split("\t")[0].split(" ")[0];
					Double similarity = Double.parseDouble(line.split("\t")[1]);
							//.split(" ")[1]);
					context.write(new Text(otherId), new Text("s " + similarity));
//					System.out.println(otherId + " " + similarity);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
}
