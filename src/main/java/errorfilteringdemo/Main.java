package errorfilteringdemo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class Main {

    public static void main(String[] args) {
        // From https://beam.apache.org/documentation/programming-guide/#additional-outputs

        // To emit elements to multiple output PCollections, create a TupleTag object to identify each collection
        // that your ParDo produces. For example, if your ParDo produces three output PCollections (the main output
        // and two additional outputs), you must create three TupleTags. The following example code shows how to
        // create TupleTags for a ParDo with three output PCollections.

        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

        // Input PCollection to our ParDo.
        PCollection<String> words = p.apply(
                "ReadMyFile", TextIO.read().from("./input.txt"));

        // The ParDo will filter words whose length is below a cutoff and add them to
        // the main ouput PCollection<String>.
        // If a word is above the cutoff, the ParDo will add the word length to an
        // output PCollection<Integer>.
        // If a word starts with the string "MARKER", the ParDo will add that word to an
        // output PCollection<String>.
        final int wordLengthCutOff = 5;

        // Create three TupleTags, one for each output PCollection.
        // Output that contains words below the length cutoff.
        final TupleTag<String> wordsBelowCutOffTag =
                new TupleTag<String>(){};
        // Output that contains word lengths.
        final TupleTag<String> wordsAboveCutOffTag =
                new TupleTag<String>(){};
        // Output that contains "MARKER" words.
        final TupleTag<String> markedWordsTag =
                new TupleTag<String>(){};

        // Passing Output Tags to ParDo:
        // After you specify the TupleTags for each of your ParDo outputs, pass the tags to your ParDo by invoking
        // .withOutputTags. You pass the tag for the main output first, and then the tags for any additional outputs
        // in a TupleTagList. Building on our previous example, we pass the three TupleTags for our three output
        // PCollections to our ParDo. Note that all of the outputs (including the main output PCollection) are
        // bundled into the returned PCollectionTuple.

        PCollectionTuple results =
                words.apply(ParDo
                        .of(new DoFn<String, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                String word = c.element();
                                if (word.length() <= wordLengthCutOff) {
                                    // Emit short word to the main output.
                                    // In this example, it is the output with tag wordsBelowCutOffTag.
                                    c.output(word);
                                } else {
                                    // Emit long word length to the output with tag wordsAboveCutOffTag.
                                    c.output(wordsAboveCutOffTag, word);
                                }
                                if (word.startsWith("word")) {
                                    // Emit word to the output with tag markedWordsTag.
                                    c.output(markedWordsTag, word);
                                }
                        }})
                        // Specify the tag for the main output.
                        .withOutputTags(wordsBelowCutOffTag,
                                // Specify the tags for the two additional outputs as a TupleTagList.
                                TupleTagList.of(wordsAboveCutOffTag)
                                        .and(markedWordsTag)));

        results.get(wordsAboveCutOffTag).apply("WriteToText",
                TextIO.write().to("output.txt")
                        .withSuffix(".txt"));

        p.run();
    }

}
