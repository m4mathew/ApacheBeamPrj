package apacheBeamPrj;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

public class Dataflow {

	public static void main(String[] args) {
		
		Pipeline pipeline = Pipeline.create();
		PCollection<String> outPCollection = pipeline.apply(TextIO.read().from("/home/mmathew/eclipse-newworkspace/ApacheBeamPrj/src/main/java/apacheBeamPrj/input.csv"));
		outPCollection.apply(TextIO.write().to("/home/mmathew/eclipse-newworkspace/ApacheBeamPrj/src/main/java/apacheBeamPrj/output.csv"));
	
		pipeline.run();
	}

}
