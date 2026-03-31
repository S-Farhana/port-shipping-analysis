import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RegionPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        String region = key.toString().trim().toLowerCase();
        if (region.equals("asia")) return 0;
        else if (region.equals("europe")) return 1;
        else return 2;
    }
}