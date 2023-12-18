package software.ulpgc.bigData.InvertedIndex.BookApi;

import com.google.gson.Gson;
import spark.Request;
import spark.Response;

import java.text.SimpleDateFormat;
import java.util.*;

import static spark.Spark.*;
import static spark.Spark.stop;

public class SparkWebServiceHazelcast implements APISource{
    private final DataManagerHazelcast dataManager;
    private final Gson gson = new Gson();

    private static void manageException(Exception exception, Request request, Response response) {
        exception.printStackTrace();
        response.status(500);
        response.body("Internal Server Error");
    }

    public APISource startServer() {
        port(8080);
        return this;
    }
    public SparkWebServiceHazelcast() {
        this.dataManager = new DataManagerHazelcast();
    }
    public void start() {
        get("/stats", this::getStatsRequest);
        get("/documents/:words", this::getKeywordsRequest);

        exception(Exception.class, SparkWebServiceHazelcast::manageException);

    }


    @Override
    public void stopServer() {
        stop();
    }

    private Object getStatsRequest(Request request, Response response) {
        long startTime = System.currentTimeMillis();

        String type = request.queryParams("type");

        if ("count".equals(type)) {
            HashMap<String, Integer> mapBookCount = dataManager.getBooksWordsCount();
            long endTime = System.currentTimeMillis();
            long elapsedTime = endTime - startTime;

            System.out.println("Execution time: " + elapsedTime + " ms");

            response.type("application/json");
            return gson.toJson(mapBookCount);
        } else {
            response.status(400);
            return "Invalid 'type' parameter. Expected 'count'.";
        }
    }

    private Object getKeywordsRequest(Request request, Response response) {
        long startTime = System.currentTimeMillis();

        String wordsParam = request.params(":words");
        String author = request.queryParams("author");
        String fromYear = request.queryParams("from");
        String toYear = request.queryParams("to");
        if (wordsParam == null || wordsParam.trim().isEmpty()) {
            response.status(400);
            return "Parameter 'words' is required";
        }
        String[] wordsArray = wordsParam.split("\\+");
        List<HashMap> listResults = new ArrayList<>();
        for (String keyWord : wordsArray) {
            HashMap<String, List<Associate>> results = dataManager.searchBooks(keyWord, author, fromYear, toYear);
            listResults.add(results);
        }
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;

        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
        String timestampString = dateFormat.format(now);

        System.out.println(timestampString + "...words:" + Arrays.toString(wordsArray));
        System.out.println("Execution time: " + elapsedTime + " ms");
        response.type("application/json");
        return gson.toJson(listResults);
    }
}
