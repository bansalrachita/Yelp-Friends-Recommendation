package neu.edu.yelp.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ItemSimilarityMapper extends
		Mapper<LongWritable, Text, TaggedKey, Text> {

	String person = null;
	Gson gson = new Gson();
	TaggedKey taggedKey = new TaggedKey();
	IntWritable businessPerson = new IntWritable(1);
	IntWritable businessOther = new IntWritable(2);

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		person = context.getConfiguration().get("person",
				"qL7Astun3i7qwr2IL5iowA");
	}

	public void map(LongWritable key, Text value, Context context) {
		try {
			String line = value.toString();
			System.out.println(line);
			if (line != null && line.length() > 0) {
				JsonObject jsonObject = gson.fromJson(line, JsonObject.class);
				JsonElement jsonObjUserId = jsonObject.get("user_id");
				JsonElement jsonObjBusinessId = jsonObject.get("business_id");
				JsonElement jsonObjStars = jsonObject.get("stars");
				JsonElement jsonObjType = jsonObject.get("type");
//				System.out.println("jsonObjBusinessId : " + jsonObjBusinessId
//						+ "jsonObjUserId : " + jsonObjUserId);
				if (null != jsonObjType
						&& jsonObjType.toString().equals("\"review\"")
						&& jsonObjUserId.toString()
								.equals("\"" + person + "\"")) {

					taggedKey
							.set(new Text(jsonObjBusinessId.toString()
									.substring(
											1,
											jsonObjBusinessId.toString()
													.length() - 1)),
									businessPerson);
					context.write(
							taggedKey,
							new Text("Business"
									+ " "
									+ jsonObjUserId.toString()
											.substring(
													1,
													jsonObjUserId.toString()
															.length() - 1)
									+ " " + jsonObjStars.toString()));
					// <businessId, userId stars>
				} else if (null != jsonObjType
						&& jsonObjType.toString().equals("\"review\"")
						&& !jsonObjUserId.toString().equals(
								"\"" + person + "\"")) {

					taggedKey
							.set(new Text(jsonObjBusinessId.toString()
									.substring(
											1,
											jsonObjBusinessId.toString()
													.length() - 1)),
									businessOther);
					context.write(
							taggedKey,
							new Text(jsonObjUserId.toString().substring(1,
									jsonObjUserId.toString().length() - 1)
									+ " " + jsonObjStars.toString()));
				}
			}

			// businessId Business userId stars
			// businessId userId stars
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
