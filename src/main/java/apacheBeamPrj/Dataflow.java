package apacheBeamPrj;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Regex;
import org.apache.beam.sdk.values.PCollection;

public class Dataflow {
	public static interface InputOptions extends PipelineOptions {
		  @Description("Path of the file to read from")
		  @Default.String("/home/mmathew/eclipse-newworkspace/ApacheBeamPrj/src/main/java/apacheBeamPrj/input.csv")
		  String getInputFile();
		  void setInputFile(String value);
		  
		}
	
	public static void main(String[] args) {
		InputOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
			      .as(InputOptions.class);
		Pipeline pipeline = Pipeline.create(options);
		PCollection<String> lines = pipeline.apply(TextIO.read().from(options.getInputFile()));
		PCollection<String> outPCollection = lines.apply(Regex.replaceAll("\\d(?=\\d{3})","*"));
		outPCollection.apply(TextIO.write().to("/home/mmathew/eclipse-newworkspace/ApacheBeamPrj/src/main/java/apacheBeamPrj/output.csv"));
	
		pipeline.run();
	}

}
