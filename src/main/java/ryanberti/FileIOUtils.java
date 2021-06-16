package ryanberti;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/***
 * A set of utils to help define a FileIO write transform, and helpers to write/read event timestamps
 * to/from partition paths. This could probably be a Scala class, but a lot of the example FileNaming
 * code is in Java, and Scio's dynamic writing functionality doesn't give us direct access to the
 * generated file names.
 */
public class FileIOUtils {

    // This format/pattern pair should match so we can generate and parse Instants from paths
    private static String partitionFormat = "y=%04d/m=%02d/d=%02d/h=%02d/m=%02d";
    private static Pattern partitionFormatPattern = Pattern.compile(".*y=(\\d{4})/m=(\\d{2})/d=(\\d{2})/h=(\\d{2})/m=(\\d{2}).*");

    // Helpers that use the format / pattern
    public static String instantToPartitionPath(Instant instant) {
        int year = instant.get(DateTimeFieldType.year());
        int month = instant.get(DateTimeFieldType.monthOfYear());
        int day = instant.get(DateTimeFieldType.dayOfMonth());
        int hour = instant.get(DateTimeFieldType.hourOfDay());
        int minute = instant.get(DateTimeFieldType.minuteOfHour());

        return String.format(partitionFormat, year, month, day, hour, minute);
    }

    public static Instant partitionPathToInstant(String path) {
        Matcher m = partitionFormatPattern.matcher(path);
        if(!m.find()) throw new IllegalStateException("Path input did not match expected pattern: " + path);
        return new DateTime()
                .withZone(DateTimeZone.UTC)
                .withYear(Integer.parseInt(m.group(1)))
                .withMonthOfYear(Integer.parseInt(m.group(2)))
                .withDayOfMonth(Integer.parseInt(m.group(3)))
                .withHourOfDay(Integer.parseInt(m.group(4)))
                .withMinuteOfHour(Integer.parseInt(m.group(5)))
                .withSecondOfMinute(0)
                .withMillisOfSecond(0)
                .toInstant();
    }

    // Use the default FileNaming instance to generate a filename, but build a path
    // from the window information using our helper methods
    private static FileIO.Write.FileNaming defaultNaming = FileIO.Write.defaultNaming("", ".parquet");
    public static class PartitionFileNaming implements FileIO.Write.FileNaming {

        private final long processingTime;

        public PartitionFileNaming(Long processingTime) {
            this.processingTime = processingTime;
        }

        @Override
        public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
            return String.format("%s/rt=%d/%s",
                    instantToPartitionPath(((IntervalWindow)window).start()),
                    processingTime,
                    defaultNaming.getFilename(window, pane, numShards, shardIndex, compression));
        }
    }

    public static FileIO.Write<Long, GenericRecord> forTarget(String target, org.apache.avro.Schema schema) {
         return FileIO
                 .<Long, GenericRecord>writeDynamic()
                 .via(ParquetIO.sink(schema))
                 .to(target)
                 .by((SerializableFunction<GenericRecord, Long>) input -> Instant.now().getMillis())
                 .withDestinationCoder(VarLongCoder.of())
                 .withNumShards(1)
                 .withNaming(PartitionFileNaming::new);
    }
}
