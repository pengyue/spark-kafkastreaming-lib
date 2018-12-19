package integration.helper;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public final class FilesystemHelper {

    public static void createTmp(
        String subdirectory
    ) {
        deleteTmp(subdirectory);

        File absoluteTmpDirectoryFile = getTmpFile(subdirectory);

        absoluteTmpDirectoryFile.mkdir();
    }

    public static void deleteTmp(
        String subdirectory
    ) {
        File absoluteTmpDirectoryFile = getTmpFile(subdirectory);

        if (absoluteTmpDirectoryFile.exists()) {
            try {
                FileUtils.deleteDirectory(absoluteTmpDirectoryFile);
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public static String getTmpDirectory(
        String subdirectory
    ) {
        File absoluteTmpDirectoryFile = getTmpFile(subdirectory);
        return absoluteTmpDirectoryFile.getAbsolutePath();
    }

    private static File getTmpFile(
        String subdirectory
    ) {
        String tmpDirectory = resolveAbsoluteTmpDirectory(subdirectory);
        return new File(tmpDirectory);
    }

    private static String resolveAbsoluteTmpDirectory(
        String subdirectory
    ) {
        String tmpDirectory = subdirectory;

        if (!subdirectory.startsWith("/")
            && subdirectory.charAt(1) != ':') {
            tmpDirectory = System.getProperty("java.io.tmpdir") + "/" + tmpDirectory;
        }

        return tmpDirectory;
    }
}
