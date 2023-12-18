package software.ulpgc.bigData.InvertedIndex.DatamartBuilder;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.org.json.JSONObject;

import java.util.Map;

public class HazelcastDatamart {

    public String getMultimapName(){
        return MULTIMAP_NAME;
    }
    private static final String MULTIMAP_NAME = "datamart";
    private final HazelcastInstance hazelcast;

    public HazelcastDatamart(HazelcastInstance instance){
        hazelcast = instance;
    }

    public void setup(Map<Book, Map<Associate, Word>> bookMap) {
        MultiMap<String, String> multiMap = hazelcast.getMultiMap(MULTIMAP_NAME);

        for (Map.Entry<Book, Map<Associate, Word>> entry : bookMap.entrySet()) {
            Book book = entry.getKey();
            Map<Associate, Word> associates = entry.getValue();

            for (Map.Entry<Associate, Word> associateEntry : associates.entrySet()) {
                Associate associate = associateEntry.getKey();
                Word word = associateEntry.getValue();
                JSONObject json = new JSONObject();
                json.put("bookTitle", book.getTitle());
                json.put("author", book.getAuthor());
                json.put("wordOccurrences", associate.getCount());
                json.put("GutenbergBookId", book.getId());
                json.put("releaseDate", book.getReleaseYear());

                multiMap.put(word.getLabel(), json.toString());
            }
        }
    }
}
