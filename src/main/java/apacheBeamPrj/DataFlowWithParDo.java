package apacheBeamPrj;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class DataFlowWithParDo {
	static class ProcessLineFn extends DoFn<String, String> {
		private static final long serialVersionUID = 1L;

		//process each line to get only first element
		@ProcessElement
		  public void processElement(@Element String line, OutputReceiver<String> out) {
			 String[] elements = line.split(",");
			 out.output(elements[0]);
		  }
		}

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> lines = pipeline.apply(TextIO.read().from("/home/mmathew/eclipse-newworkspace/ApacheBeamPrj/src/main/java/apacheBeamPrj/input.csv"));
		
		PCollection<String> outPCollection = lines.apply(ParDo.of(new ProcessLineFn()));
	
		outPCollection.apply(TextIO.write().to("/home/mmathew/eclipse-newworkspace/ApacheBeamPrj/src/main/java/apacheBeamPrj/output.csv"));
	
		pipeline.run();

	}

}
