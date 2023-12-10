package software.ulpgc.bigData.InvertedIndex.BookCleaner;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import software.ulpgc.bigData.InvertedIndex.crawler.BatchDownloader;
import software.ulpgc.bigData.InvertedIndex.crawler.GutenbergFileReader;
import software.ulpgc.bigData.InvertedIndex.crawler.GuttenbergDatalakeCreator;

import javax.jms.JMSException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Thread.sleep;

public class GutenbergSplitter implements DocumentSplitter {
    GutenbergFileReader bookReader;
    GuttenbergDatalakeCreator gutenbergDatalakeCreator;
    GutenbergCleaner gutenbergCleaner;
    BatchDownloader batchDownloader;
    UTF8FileChecker utf8FileChecker;
    private Map<String, String> extensionDictionary;
    software.ulpgc.bigData.InvertedIndex.BookCleaner.ArtemisCommunicator artemisCommunicator;

    public GutenbergSplitter() {
        extensionDictionary = new HashMap<>();
        extensionDictionary.put("raw", ".txt");
        extensionDictionary.put("content", ".txt");
        extensionDictionary.put("metadata", ".json");
        this.bookReader = new GutenbergFileReader();
        this.gutenbergCleaner = new GutenbergCleaner();
        this.gutenbergDatalakeCreator = new GuttenbergDatalakeCreator();
        this.utf8FileChecker = new UTF8FileChecker();
        this.batchDownloader = new BatchDownloader(10, bookReader, new String[10]);
        this.artemisCommunicator = new ArtemisCommunicator();
    }
    @Override
    public void splitDocument(String path) throws IOException, InterruptedException {
        splitWithRetries(path, 3);
    }

    private void splitWithRetries(String path, int retries) throws IOException, InterruptedException {
        if (retries <= 0) {
            System.err.println("Maximum numbers of retry exceeded.");
            return;
        }

        try {
            String text = bookReader.read(path, "raw");
            String pathToFile = bookReader.getFilePath(path);
            File file = new File(pathToFile);

            if (utf8FileChecker.isUTF8(file)) {
                String lastNotice = "before using this eBook.";

                int metadataStartIndex = text.indexOf(lastNotice);
                int firstDelimiterIndex = text.indexOf("*** START");
                int secondDelimiterIndex = text.indexOf("***", firstDelimiterIndex + 3);
                int thirdDelimiterIndex = text.indexOf("*** END");

                String metadata = text.substring(metadataStartIndex + lastNotice.length() + 1, firstDelimiterIndex);
                String content = text.substring(secondDelimiterIndex + 4, thirdDelimiterIndex);
                storeFile(path, gutenbergCleaner.cleanText(content), "content");
                storeFile(path, prepareMetadata(metadata), "metadata");
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("Retrying in 1 second...");
            sleep(2000);
            splitWithRetries(path, retries - 1);
        }
    }


    @Override
    public String prepareMetadata(String text) {
        text = text.trim().replaceAll(" +", " ");
        Pattern pattern = Pattern.compile("([^:]+):\\s*([^\\n]+(?:\\n(?!\\w:)[^\\n]+)*)");

        Matcher matcher = pattern.matcher(text);
        Map<String, String> dictionary = new HashMap<>();
        while (matcher.find()) {
            String field = matcher.group(1).trim();
            String value = matcher.group(2).trim().replaceAll("\\s+", " ");
            dictionary.put(field, value);
        }
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(dictionary);
    }

    @Override
    public void storeFile(String name, String content, String type) {
        String path = gutenbergDatalakeCreator.setFilePath(name, new Date());
        name = name +"_"+ type+extensionDictionary.get(type);
        String fullpath = path + name;
        try {
            FileWriter writer = new FileWriter(fullpath);
            writer.write(content);
            writer.close();
            artemisCommunicator.sendMessageToQueue(artemisCommunicator.createConnection(), fullpath, type);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Failed to save the file.");
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }
}