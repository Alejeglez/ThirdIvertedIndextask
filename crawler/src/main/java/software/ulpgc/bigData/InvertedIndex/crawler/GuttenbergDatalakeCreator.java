package software.ulpgc.bigData.InvertedIndex.crawler;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GuttenbergDatalakeCreator implements DatalakeCreator {


    @Override
    public String setFilePath(String filename, Date currentDate) {

        String filePath;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String folderPath = "datalake//" + dateFormat.format(currentDate) + "//" + filename;
        if (!Files.isDirectory(Path.of(folderPath))) {
            new File(folderPath).mkdirs();
        }
        filePath = folderPath + "//";

        return filePath;
    }
    public void createDateFolder(Date currentDate) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String folderPath = "datalake//" + dateFormat.format(currentDate);
        if (!Files.isDirectory(Path.of(folderPath))) {
            new File("/path/directory").mkdirs();
        }
    }
}

