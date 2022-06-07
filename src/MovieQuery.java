import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MovieQuery {
    public static void queryAll(String inFileName, String outFileName) {
        List<String> rawData;
        try{
            Path path = Paths.get(inFileName);
            rawData = Files.lines(path).collect(Collectors.toList());
        } catch (Exception e) {
            System.out.println("Wrong file path : " + inFileName);
            return;
        }
        System.out.println(rawData);
        // get rid of headers as it is still a collection : Title,Year,Genre,RunTime,Rating,Votes,Director,Cast,Gross
        // also transform each line into array for queries to use stream<String[]>.
        rawData.remove(0);
        List<String[]> preparedData = rawData.stream().map(line -> line.split(",")).collect(Collectors.toList());
        query1(preparedData, outFileName+"-1.out");
        query2(preparedData, outFileName+"-2.out");
        query3(preparedData, outFileName+"-3.out");
    }

    public static void query1(List<String[]> collection, String outFileName){
        PrintWriter outputFile =  QueryOutputWriter.processOutput(outFileName, MovieQuery::outputQuery);
        StreamProcessor.applyLambdaToStream(collection.stream(), MovieQuery::fetchTitles)
                    .forEach(outputFile::println);
    }
    public static void query2(List<String[]> collection, String outFileName){
        PrintWriter outputFile =  QueryOutputWriter.processOutput(outFileName, MovieQuery::outputQuery);
        StreamProcessor.applyLambdaToStream(collection.stream(), MovieQuery::fetchDirectorsWithRatings)
                    .forEach(outputFile::println);
    }
    public static void query3(List<String[]> collection, String outFileName){
        PrintWriter outputFile =  QueryOutputWriter.processOutput(outFileName, MovieQuery::outputQuery);
        StreamProcessor.applyLambdaToStream(collection.stream(), MovieQuery::adventureWithLeastRevenue)
                .forEach(outputFile::println);
    }
    public static Stream<String> adventureWithLeastRevenue(Stream<String[]> stream) {
        return stream.map( arr -> MovieQuery.indexStreamArray(arr, 0))
                .map(String::toUpperCase)
                .sorted();
    }
    public static Stream<String> fetchTitles(Stream<String[]> stream) {
        return stream.map( arr -> MovieQuery.indexStreamArray(arr, 0))
                    .map(String::toUpperCase)
                    .sorted();
    }
    public static Stream<String> fetchDirectorsWithRatings(Stream<String[]> stream) {
        return stream
                .filter(x-> Double.valueOf(x[4]) >= 8.5
                )
                .map( arr -> MovieQuery.indexStreamArray(arr, 6))
                .distinct()
                .map(String::toUpperCase)
                .sorted();
    }
    public static PrintWriter outputQuery(String outFileName){
        // try-resource here :)
        try(PrintWriter outputFile = new PrintWriter(Files.newBufferedWriter(Paths.get(outFileName)))) {
            return outputFile;
        }
        catch(Exception e){
            System.out.println(e);
            return null;
        }
    }
    public static String indexStreamArray(String[] arr, int index){
        return arr[index];
    }
}

