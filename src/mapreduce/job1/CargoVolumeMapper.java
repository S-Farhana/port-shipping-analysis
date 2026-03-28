import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class CargoVolumeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text portName = new Text();
    private DoubleWritable cargoValue = new DoubleWritable();
    private boolean isHeader = true;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip header line
        if (isHeader && line.startsWith("date")) {
            isHeader = false;
            return;
        }
        isHeader = false;

        String[] fields = line.split(",");

        // Need at least 22 fields (export column is index 21)
        if (fields.length < 22) return;

        try {
            String port = fields[5].trim();   // portname
            double export = Double.parseDouble(fields[21].trim()); // export column
            double importVal = Double.parseDouble(fields[14].trim()); // portcalls

            double totalCargo = export + importVal;

            portName.set(port);
            cargoValue.set(totalCargo);
            context.write(portName, cargoValue);

        } catch (NumberFormatException e) {
            // Skip malformed rows
        }
    }
}