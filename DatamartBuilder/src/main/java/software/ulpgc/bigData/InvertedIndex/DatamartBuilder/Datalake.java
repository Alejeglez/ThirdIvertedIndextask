package software.ulpgc.bigData.InvertedIndex.DatamartBuilder;

import javax.jms.JMSException;
import java.io.File;
import java.io.IOException;
import java.util.Map;

public interface Datalake {
    Map<Book, Map<Associate, Word>> read(File directory,File metadata, int wordMaxId) throws IOException, JMSException;

}
