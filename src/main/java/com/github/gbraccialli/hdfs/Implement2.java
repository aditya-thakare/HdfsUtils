package com.github.gbraccialli.hdfs;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;

public class Implement2 {
    public static void main(String args[]){
        System.out.println("IMP2: From Main: WELCOMEEEEEE" );
        implementCall();
    }
    public static String implementCall(){
        BasicConfigurator.configure();
        System.out.println("From IMP2: implementCall" );
        String json = "";
        try {

            FileSystem hdfs = HDFSConfigUtils.loadConfigsAndGetFileSystem("/tmp/zeppline/","hdfs");

            int currentLevel = 0; //current level, default = 0
            int maxLevelThreshold = 7; //max number of directories do drill down. -1 means no limit. for example: maxLevelThreshold=3 means drill down will stop after 3 levels of subdirectories
            int minSizeThreshold = -1; //min number of bytes in a directory to continue drill down. -1 means no limit. minSizeThreshold=1000000 means only directories greater > 1000000 bytes will be drilled down
            boolean showFiles = false; //whether to show information about files. showFiles=false will show summary information about files in each directory/subdirectory.
            String excludeList = ""; //directories to exclude from drill down, for example: /tmp/,/user/ won't present information about those directories.
            boolean verbose = false; //when true print processing info into System.err (not applied for zeppelin)

            Path hdfsPath = new Path("/dl"); //path to start analysis
            PathInfo dirInfo = DirectoryContentsUtils.listContents(hdfs,hdfsPath,currentLevel,maxLevelThreshold,minSizeThreshold,showFiles,verbose,excludeList);
             json = DirectoryContentsUtils.directoryInfoToJson(dirInfo);

            System.out.println("JSON: " + json);
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
        }
        return json;
    }
}
