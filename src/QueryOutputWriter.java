import java.io.PrintWriter;
import java.util.stream.Stream;

public interface QueryOutputWriter {
    PrintWriter outputWrite(String outFileName);

    public static PrintWriter processOutput(String outFileName,QueryOutputWriter outputWriter) {
        return outputWriter.outputWrite(outFileName);
    }
}
