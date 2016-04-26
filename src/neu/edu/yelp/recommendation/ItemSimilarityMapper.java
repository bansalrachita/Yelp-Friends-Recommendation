package neu.edu.yelp.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ItemSimilarityMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	String person = null;
	Gson gson = new Gson();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		person = context.getConfiguration().get("person",
				"qL7Astun3i7qwr2IL5iowA");
		System.out.println("Reduce setup called! " + person);
	}

	public void map(LongWritable key, Text value, Context context) {
		try {
			String line = value.toString();
			if (line != null && line.length() > 0) {
				JsonObject jsonObject = gson.fromJson(line, JsonObject.class);
				JsonElement jsonObjUserId = jsonObject.get("user_id");
				JsonElement jsonObjBusinessId = jsonObject.get("business_id");
				JsonElement jsonObjStars = jsonObject.get("stars");
				JsonElement jsonObjType = jsonObject.get("type");
				
				if (null != jsonObjType
						&& jsonObjType.toString().equals("\"review\"")
						&& jsonObjUserId.toString().equals("\"person\"")) {
					context.write(
							new Text("P" + jsonObjBusinessId.toString().substring(1,
									jsonObjBusinessId.toString().length() - 1)),
							new Text(jsonObjUserId.toString()
									+ " "
									+ jsonObjStars.toString()
											.substring(
													1,
													jsonObjStars.toString()
															.length() - 1)));
					// <P businessId, userId stars>
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
