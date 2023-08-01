package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class SqlUtils {
    public static String readLocalFile(String filePath) {
        BufferedReader reader;
        StringBuilder stringBuilder = new StringBuilder("");
        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line = reader.readLine();
            stringBuilder.append(line);
            stringBuilder.append(" ");
            while (line != null) {
                // read next line
                line = reader.readLine();
                if (line!= null && !line.startsWith("--")) {
                    stringBuilder.append(line);
                    stringBuilder.append(" ");
                }
            }
            reader.close();
            return stringBuilder.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }
}
