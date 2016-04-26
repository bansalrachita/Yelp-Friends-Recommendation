package neu.edu.yelp.recommendation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransitivityMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) {
		try {
			String line = value.toString();
			Text word = new Text();
			String[] sPath = line.split("\t| ");
			int distanceAdd = Integer.parseInt(sPath[1]) + 1;// add a hop
			String[] pointsTo = sPath[2].split(",");// connected to
			for (int i = 0; i < pointsTo.length; i++) {
				word.set("VALUE " + distanceAdd); // tells me to look at distance value
				context.write(new Text(pointsTo[i]),word);
				word.clear();
			}
			// pass in current node's distance (if it is the lowest distance)
			word.set("VALUE " + sPath[1]);
			context.write(new Text(sPath[0]), word);
			word.clear();

			word.set("NODES " + sPath[2]);
			context.write(new Text(sPath[0]), word);
			word.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
