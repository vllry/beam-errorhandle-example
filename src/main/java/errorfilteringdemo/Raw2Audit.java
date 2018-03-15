package errorfilteringdemo;

import errorfilteringdemo.datum.Failure;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import errorfilteringdemo.datum.Audit;
import errorfilteringdemo.helpers.MiscHelpers;
import org.apache.beam.sdk.values.TupleTagList;


public class Raw2Audit extends DoFn<String, Audit> {

    public static TupleTag<Audit> validTag = new TupleTag<Audit>(){};
    public static TupleTag<Failure> failuresTag = new TupleTag<Failure>(){};

    /*
    Take the PCollection of log strings, and output a PCollectionTuple with Audit objects and Failure objects.
    (ProcessElement should be static in Beam 2.3.0+, otherwise you will have problems trying to serialize the transform itself).
     */
    public static PCollectionTuple process(PCollection<String> logStrings) {
        return logStrings.apply("Create PubSub objects", ParDo.of(new DoFn<String, Audit>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String logLine = c.element();
                Audit auditd = new Audit();

                try {
                    auditd.type = MiscHelpers.regexHelper("type=(.*?)\\s", logLine);
                    auditd.pid = Integer.parseInt(MiscHelpers.regexHelper("pid=(.*?)\\s", logLine));
                    auditd.uid = Integer.parseInt(MiscHelpers.regexHelper("uid=(.*?)\\s", logLine));
                    // Max Integer value is 2,147,483,647. ;)
                    auditd.auid = Integer.parseInt(MiscHelpers.regexHelper("auid=(.*?)\\s", logLine));

                    c.output(auditd);  // The "main" channel doesn't need the TupleTag specified.

                }
                catch (Throwable throwable) {
                    Failure failure = new Failure(logLine, auditd, throwable);
                    c.output(failuresTag, failure);  // Attach to failure tag.
                }

            }
        }).withOutputTags(
                validTag,
                TupleTagList.of(failuresTag)
        ));
    }

}
