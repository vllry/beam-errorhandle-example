package errorfilteringdemo;

import errorfilteringdemo.datum.Failure;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import errorfilteringdemo.datum.Audit;
import org.apache.beam.sdk.values.TupleTagList;


public class Raw2Audit extends DoFn {

    final TupleTag<Audit> valid = new TupleTag<Audit>(){};
    final TupleTag<String> failures = new TupleTag<String>(){};

    private static String regexHelper(String regex, String text) {
        Matcher matcher = Pattern.compile(regex).matcher(text);
        matcher.find();
        return matcher.group(1);
    }

    public PCollectionTuple transform(PCollection<String> logStrings) {
        return logStrings.apply("Create PubSub objects", ParDo.of(new DoFn<String, Audit>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String line = c.element();
                Audit auditd = new Audit();

                try {
                    auditd.type = regexHelper("type=(.*?)\\s", line);
                    auditd.pid = Integer.parseInt(regexHelper("pid=(.*?)\\s", line));
                    auditd.uid = Integer.parseInt(regexHelper("uid=(.*?)\\s", line));
                    // Max Integer value is 2,147,483,647. ;)
                    auditd.auid = Integer.parseInt(regexHelper("auid=(.*?)\\s", line));
                }
                catch (Throwable throwable) {
                    Failure failure = new Failure(auditd, throwable);
                    c.output(failures, failure.toString());
                }

                c.output(auditd);
            }
        }).withOutputTags(
                valid,
                TupleTagList.of(failures)
        ));
    }

}
