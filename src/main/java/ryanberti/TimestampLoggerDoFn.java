package ryanberti;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.Instant;
import org.slf4j.Logger;

/***
 * A no-op DoFn which just logs the timestamp and window associated
 * with each element; useful for debugging but shouldn't be utilized in
 * production pipelines
 *
 * @param <T> the input and output type of this DoFn
 */
public class TimestampLoggerDoFn<T> extends DoFn<T,T> {

    private Logger logger;
    private String message;

    public TimestampLoggerDoFn(Logger logger, String message) {
        this.logger = logger;
        this.message = message;
    }

    @ProcessElement
    public void processElement(@Element T input, @Timestamp Instant timestamp, BoundedWindow window, OutputReceiver<T> out) {

        String windowString = "";
        if (window instanceof IntervalWindow) {
            IntervalWindow iwindow = (IntervalWindow)window;
            windowString = iwindow.start() + " -> " + iwindow.end();
        } else {
            windowString = window.maxTimestamp() + "";
        }

        logger.info(String.format(message,timestamp.getMillis(), windowString));
        out.output(input);
    }
}
