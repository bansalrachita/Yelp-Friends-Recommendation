package neu.edu.yelp.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class FriendsAdjacencyListMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	Gson gson = new Gson();
	int infinty = 0;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		infinty = context.getConfiguration().getInt("infinity", 999);
	}

	public void map(LongWritable key, Text value, Context context) {
		try {
			String line = value.toString();
			if (line != null && line.length() > 0) {
				JsonObject jsonObject = gson.fromJson(line, JsonObject.class);
				JsonElement jsonObjType = jsonObject.get("type");
				JsonElement jsonObjUserId = jsonObject.get("user_id");
				if (jsonObjType.toString().equals("\"user\"")) {
					JsonElement jsonObjFriends = jsonObject.get("friends");
					context.write(
							new Text(jsonObjUserId.toString().substring(1,
									jsonObjUserId.toString().length() - 1)),
							new Text(infinty
									+ " "
									+ jsonObjFriends
											.toString()
											.replace("\"", "")
											.substring(
													1,
													jsonObjFriends.toString()
															.replace("\"", "")
															.length() - 1)));
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
