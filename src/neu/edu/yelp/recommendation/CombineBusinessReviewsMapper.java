package neu.edu.yelp.recommendation;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class CombineBusinessReviewsMapper extends
		Mapper<LongWritable, Text, TaggedKey, Text> {

	Gson gson = new Gson();
	TaggedKey taggedKey = new TaggedKey();
	IntWritable business = new IntWritable(1);
	IntWritable reviews = new IntWritable(2);

	public void map(LongWritable key, Text value, Context context) {
		try {
			String line = value.toString();
			if (line != null && line.length() > 0) {

				JsonObject jsonObject = gson.fromJson(line, JsonObject.class);
				JsonElement jsonObjType = jsonObject.get("type");
				JsonElement jsonObjBusinessId = jsonObject.get("business_id");

				if (jsonObjType.toString().equals("\"business\"")
						&& jsonObjBusinessId.toString().length() > 0) {

					JsonElement jsonObjCity = jsonObject.get("city");

					if (jsonObjCity.toString()
							.equalsIgnoreCase("\"Las Vegas\"")) {

						taggedKey.set(new Text(jsonObjBusinessId.toString()),
								business);

						context.write(taggedKey, new Text("\"city\": "
								+ jsonObjCity.toString()));
					}

				} else if (jsonObjType.toString().equals("\"review\"")
						&& jsonObjBusinessId.toString().length() > 0) {

					JsonElement jsonObjStars = jsonObject.get("stars");
					JsonElement jsonObjUserId = jsonObject.get("user_id");
					JsonElement jsonObjReviewId = jsonObject.get("review_id");

					taggedKey.set(new Text(jsonObjBusinessId.toString()),
							reviews);

					context.write(taggedKey, new Text("\"user_id\": "
							+ jsonObjUserId.toString() + ", \"stars\": "
							+ jsonObjStars.toString() + ", \"review_id\": "
							+ jsonObjReviewId.toString() + ", \"type\": "
							+ jsonObjType.toString()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
