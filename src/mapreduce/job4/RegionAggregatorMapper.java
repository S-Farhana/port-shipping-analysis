import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RegionAggregatorMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("vessel_id")) return;
        String[] fields = line.split(",");
        if (fields.length < 9) return;
        try {
            String portname   = fields[1].trim();
            String region     = fields[3].trim();
            double dwellHours = Double.parseDouble(fields[6].trim());
            String cargoType  = fields[7].trim();
            double cargoTons  = Double.parseDouble(fields[8].trim());
            if (dwellHours <= 0) return;
            double efficiency = cargoTons / dwellHours;
            context.write(new Text(region),
                new Text(portname + "\t" + region + "\t" + cargoType + "\t" + efficiency));
        } catch (NumberFormatException e) {}
    }
}