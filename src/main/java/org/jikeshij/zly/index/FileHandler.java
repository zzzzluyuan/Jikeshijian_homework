package org.jikeshij.zly.index;

import scala.Tuple2;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class FileHandler {
    // 由于文件放在classpath下，用以下方法获取指定classpath下，文件夹的所有文件名，如本例为0，1，2
    public ArrayList<String> getFileNamesFromDirectory(String directory) {
        File folder = new File(directory);
        File[] listOfFiles = folder.listFiles();
        ArrayList<String> fileNames = new ArrayList<>();
        assert listOfFiles != null;
        for (File f: listOfFiles)
            if (f.isFile()) {
                fileNames.add(f.getName());
            }
        return fileNames;
    }

    // 获取绝对路径下的文件名和内容，并按行拆分，形成Tuple2<单词，文件路径>的形式
    public List<Tuple2<String,String>> getFileContentList(String filePath){
        List <String> fileList = this.getFileNamesFromDirectory(filePath);
        ArrayList <Tuple2<String, String>> fileContentList = new ArrayList<>();
        for (String s: fileList){
            try (Stream<String> lines = Files.lines(Paths.get(filePath + File.separator + s))) {
                lines.forEach(str-> Arrays.asList(str.split(" ")).forEach(str2->fileContentList.add(new Tuple2<>(str2,s))));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fileContentList;
    }
}