package software.ulpgc.bigData.InvertedIndex.DatamartBuilder;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import javax.jms.JMSException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class FileDatalake implements Datalake {
    ArtemisCommunicator artemisCommunicator;

    public FileDatalake() {
        this.artemisCommunicator = new ArtemisCommunicator();
    }

    public static int countWordOccurrences(List<String> words, String word) {
        return (int) words.stream()
                .filter(w -> w.equals(word))
                .count();
    }

    public static Map<Associate, Word> invertedIndexFromFilesWithCount(String booksDirectory, Book book, int wordMaxId) {
        Map<Associate, Word> invertedIndex = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(booksDirectory))) {
            List<String> document = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                document.addAll(Arrays.asList(line.split("\\s+")));
            }
            Set<String> uniqueWords = new HashSet<>(document);
            int add = 1;
            for (String word : uniqueWords) {
                int wordId = wordMaxId + add;
                Word newWord = new Word(wordId, word);
                Associate associate = new Associate(newWord.getId(), book.getId(), countWordOccurrences(document, word));
                invertedIndex.put(associate, newWord);
                add += 1;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return invertedIndex;
    }

    @Override
    public Map<Book, Map<Associate, Word>> read(File directory,File metadata, int wordMaxId) throws IOException, JMSException {
        Map<Book, Map<Associate, Word>> result = new HashMap<>();

        Path path = Paths.get(directory.toString());
        String[] parts = path.toString().split("\\\\");
        int index = 2;
        String lastSegment = parts[index];
        Book book = getBook(lastSegment,metadata);
        Map<Associate, Word> invertedIndex = invertedIndexFromFilesWithCount(String.valueOf(directory), book, wordMaxId);
        result.put(book, invertedIndex);
        return result;
    }

    private Book getBook(String lastSegment,File metadata) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(metadata));
        String line;

        Book book = null;
        while ((line = br.readLine()) != null) {
            JsonObject jsonObjet = new Gson().fromJson(line, JsonObject.class);
            String author = "Anonymous";
            if (jsonObjet.getAsJsonPrimitive("Author") != null) {
                author = jsonObjet.getAsJsonPrimitive("Author").getAsString().replace("'", "");
            }
            String title = "Untitle";
            if (jsonObjet.getAsJsonPrimitive("Title") != null) {
                title = jsonObjet.getAsJsonPrimitive("Title").getAsString().replace("'", "");
            }
            int id = Integer.parseInt(lastSegment);
            String releaseYear = "Unknown";
            if (jsonObjet.getAsJsonPrimitive("Release date") != null) {
                releaseYear = jsonObjet.getAsJsonPrimitive("Release date").getAsString().replace("'", "");
                String[] parts = releaseYear.split(" ");
                releaseYear = parts[2];
            }
            book = new Book(id, author, title, releaseYear);

        }
        return book;
    }

}
