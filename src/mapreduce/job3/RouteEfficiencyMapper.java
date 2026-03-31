import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RouteEfficiencyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text routeKey = new Text();
    private DoubleWritable efficiency = new DoubleWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        // Skip header
        if (line.startsWith("vessel_id")) return;

        String[] fields = line.split(",");
        if (fields.length < 9) return;

        try {
            String portname   = fields[1].trim();  // portname
            double dwellHours = Double.parseDouble(fields[6].trim());  // dwell_hours
            String cargoType  = fields[7].trim();  // cargo_type
            double cargoTons  = Double.parseDouble(fields[8].trim());  // cargo_tons

            if (dwellHours <= 0) return;

            double score = cargoTons / dwellHours;
            routeKey.set(portname + "_" + cargoType);
            efficiency.set(score);
            context.write(routeKey, efficiency);

        } catch (NumberFormatException e) {
            // skip bad rows
        }
    }
}