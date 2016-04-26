package neu.edu.yelp.recommendation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TaggedKey implements Writable, WritableComparable<TaggedKey> {

	private Text joinKey = new Text();
	private IntWritable tag = new IntWritable();

	@Override
	public int compareTo(TaggedKey taggedKey) {
		int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
		if (compareValue == 0) {
			compareValue = this.tag.compareTo(taggedKey.getTag());
		}
		return compareValue;
	}

	public Text getJoinKey() {
		return joinKey;
	}

	public void set(Text joinKey, IntWritable tag) {
		this.joinKey = joinKey;
		this.tag = tag;
	}

	public void setJoinKey(Text joinKey) {
		this.joinKey = joinKey;
	}

	public IntWritable getTag() {
		return tag;
	}

	public void setTag(IntWritable tag) {
		this.tag = tag;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		joinKey.readFields(in);
		tag.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		joinKey.write(out);
		tag.write(out);

	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TaggedKey) {
			TaggedKey other = (TaggedKey) o;
			return joinKey.equals(other.joinKey) && tag.equals(other.tag);
		}

		return false;
	}

//	@Override
//	public int hashCode() {
//		return joinKey.hashCode() + tag.hashCode();
//	};

	@Override
	public String toString() {
		return joinKey.toString();
	}

}
