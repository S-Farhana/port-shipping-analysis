import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class RegionAggregatorReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double total = 0.0;
        int count = 0;
        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            if (parts.length >= 4) {
                try { total += Double.parseDouble(parts[3].trim()); count++; }
                catch (NumberFormatException e) {}
            }
        }
        if (count > 0) {
            context.write(key, new Text(
                "avg_efficiency=" + String.format("%.2f", total / count) +
                "\ttotal_records=" + count));
        }
    }
}