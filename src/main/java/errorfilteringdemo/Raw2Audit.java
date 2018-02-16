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


public class Raw2Audit extends DoFn {

    private final TupleTag<Audit> validTag = new TupleTag<Audit>(){};
    private final TupleTag<String> failuresTag = new TupleTag<String>(){};
    private PCollection<Audit> validPCollection;
    private PCollection<String> failedPCollection;

    public Raw2Audit(PCollection<String> logStrings) {
        PCollectionTuple outputTuple = logStrings.apply("Create PubSub objects", ParDo.of(new DoFn<String, Audit>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String line = c.element();
                Audit auditd = new Audit();

                try {
                    auditd.type = MiscHelpers.regexHelper("type=(.*?)\\s", line);
                    auditd.pid = Integer.parseInt(MiscHelpers.regexHelper("pid=(.*?)\\s", line));
                    auditd.uid = Integer.parseInt(MiscHelpers.regexHelper("uid=(.*?)\\s", line));
                    // Max Integer value is 2,147,483,647. ;)
                    auditd.auid = Integer.parseInt(MiscHelpers.regexHelper("auid=(.*?)\\s", line));
                }
                catch (Throwable throwable) {
                    Failure failure = new Failure(auditd, throwable);
                    c.output(failuresTag, failure.toString());  // TODO: replace with objects.
                }

                c.output(auditd);
            }
        }).withOutputTags(
                validTag,
                TupleTagList.of(failuresTag)
        ));

        this.validPCollection = outputTuple.get(validTag);
        this.failedPCollection = outputTuple.get(failuresTag);
    }

    public PCollection<Audit> getValid() {
        return validPCollection;
    }

    public PCollection<String> getFailed() {
        return failedPCollection;
    }

}
