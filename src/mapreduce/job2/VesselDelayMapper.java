import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class VesselDelayMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private boolean isHeader = true;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (isHeader && line.startsWith("vessel_id")) { isHeader = false; return; }
        isHeader = false;
        String[] fields = line.split(",");
        if (fields.length < 7) return;
        try {
            String port = fields[1].trim();
            double dwellHours = Double.parseDouble(fields[6].trim());
            context.write(new Text(port), new DoubleWritable(dwellHours));
        } catch (NumberFormatException e) {}
    }
}