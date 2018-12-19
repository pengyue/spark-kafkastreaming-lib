package integration.helper;

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;

import java.io.FileReader;

public class ArtifactName {

    public static String getArtifactName() {

        try {
            
            MavenXpp3Reader reader = new MavenXpp3Reader();
            Model model = reader.read(new FileReader("pom.xml"));

            return model.getArtifactId();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
