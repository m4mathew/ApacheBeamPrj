package apacheBeamPrj;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Regex;
import org.apache.beam.sdk.values.PCollection;

public class Dataflow {

	public static void main(String[] args) {
		
		Pipeline pipeline = Pipeline.create();
		PCollection<String> lines = pipeline.apply(TextIO.read().from("/home/mmathew/eclipse-newworkspace/ApacheBeamPrj/src/main/java/apacheBeamPrj/input.csv"));
		PCollection<String> outPCollection = lines.apply(Regex.replaceAll("\\d(?=\\d{3})","*"));
		outPCollection.apply(TextIO.write().to("/home/mmathew/eclipse-newworkspace/ApacheBeamPrj/src/main/java/apacheBeamPrj/output.csv"));
	
		pipeline.run();
	}

}
