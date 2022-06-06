import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MovieQuery {
    public static void queryAll(String inFileName, String outFileName) {
        List<String> data;
        try {
            data = readCSV(inFileName);
        } catch (Exception e) {
            System.out.println("Wrong file path : " + inFileName);
            return;
        }
        System.out.println(data);
        // get rid of headers when it is a collection :
        // Title,Year,Genre,RunTime,Rating,Votes,Director,Cast,Gross
        data.remove(0);
        query1(data, outFileName+"-1.out");
        query2(data, outFileName+"-2.out");
    }

    public static List readCSV(String inputFileName) throws IOException {
        List<String> file = Files.lines(Paths.get(inputFileName)).collect(Collectors.toList());
        return file;
    }
    public static void query1(List<String> collection, String outFileName){
        try(PrintWriter outputFile = new PrintWriter(Files.newBufferedWriter(Paths.get(outFileName)))) {
            StreamProcessor.applyLambdaToStream(collection.stream(), MovieQuery::fetchTitles)
                    .forEach(outputFile::println);
        }
        catch(Exception e){
            System.out.println(e);
        }
    }
    public static void query2(List<String> collection, String outFileName){
        try(PrintWriter outputFile = new PrintWriter(Files.newBufferedWriter(Paths.get(outFileName)))) {
            StreamProcessor.applyLambdaToStream(collection.stream(), MovieQuery::fetchDirectorsWithRatings)
                    .forEach(outputFile::println);
        }
        catch(Exception e){
            System.out.println(e);
        }
    }
    public static Stream<String> fetchTitles(Stream<String> stream) {
        // try-resource here
        return stream.map(line -> line.split(","))
                    .map(arr -> {
                        String first = arr[0];
                        return first;
                    })
                    .map(String::toUpperCase)
                    .sorted();
    }
    public static Stream<String> fetchDirectorsWithRatings(Stream<String> stream) {
        // try-resource here
        return stream.map(line -> line.split(","))
                .filter(x-> Double.valueOf(x[4]) >= 8.5
                )
                .map(arr -> {
                    String first = arr[6];
                    return first;
                })
                .distinct()
                .map(String::toUpperCase)
                .sorted();
    }
}

