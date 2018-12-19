package com.godepth.apache.spark.kafkastreaming.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HdfsUtils implements Serializable {

    public boolean exists(
        String location
    ) throws IOException {

        Configuration hadoopConfiguration = new Configuration();
        FileSystem hadoopFileSystem = FileSystem.get(hadoopConfiguration);

        Path path = new Path(location);
        return hadoopFileSystem.exists(path);
    }

    public Set<String> listDirectory(
        String directoryLocation
    ) throws IOException {

        Configuration hadoopConfiguration = new Configuration();
        FileSystem hadoopFileSystem = FileSystem.get(hadoopConfiguration);

        Path directoryPath = new Path(directoryLocation);
        FileStatus[] fileStatuses = hadoopFileSystem.listStatus(directoryPath);

        Set<String> entries = new HashSet<>();

        for (FileStatus fileStatus : fileStatuses) {
            entries.add(
                fileStatus
                    .getPath()
                    .getName()
            );
        }

        return entries;
    }

    public void remove(
        String location
    ) throws IOException {

        Configuration hadoopConfiguration = new Configuration();
        FileSystem hadoopFileSystem = FileSystem.get(hadoopConfiguration);
        Path path = new Path(location);

        hadoopFileSystem.delete(path, true);
    }

    public List<String> readLines(
        String fileLocation
    ) throws IOException {

        Configuration hadoopConfiguration = new Configuration();
        FileSystem hadoopFileSystem = FileSystem.get(hadoopConfiguration);

        Path filePath = new Path(fileLocation);
        FSDataInputStream fileStream = hadoopFileSystem.open(filePath);

        try {

            return IOUtils.readLines(fileStream);

        } finally {
            fileStream.close();
        }
    }

    public String readText(
        String fileLocation
    ) throws IOException {

        Configuration hadoopConfiguration = new Configuration();
        FileSystem hadoopFileSystem = FileSystem.get(hadoopConfiguration);

        Path filePath = new Path(fileLocation);
        FSDataInputStream fileStream = hadoopFileSystem.open(filePath);

        try {

            return IOUtils.toString(fileStream);

        } finally {
            fileStream.close();
        }
    }

    public void writeText(
        String fileLocation,
        String text,
        boolean overwrite
    ) throws IOException {

        Configuration hadoopConfiguration = new Configuration();
        FileSystem hadoopFileSystem = FileSystem.get(hadoopConfiguration);

        Path filePath = new Path(fileLocation);
        FSDataOutputStream fileStream = hadoopFileSystem.create(filePath, overwrite);

        try {

            IOUtils.write(text, fileStream);

        } finally {
            fileStream.close();
        }
    }
}
