package integration.helper;

import com.godepth.apache.spark.kafkastreaming.hbase.SingleKeyTableMetadata;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression;

public class TestHbaseTableMetadata implements SingleKeyTableMetadata {

    public TableName getTableName() {
        return TableName.valueOf("test_offsets");
    }

    public String getFamily() {
        return "o";
    }

    public String getQualifier() {
        throw new UnsupportedOperationException("Table 'test_offsets' has no fixed qualifier");
    }

    public byte[][] getSplits() {
        return new byte[0][];
    }

    public Compression.Algorithm getCompressionAlgorithm() {
        return Compression.Algorithm.NONE;
    }

    public int getMaxVersions() {
        return 1;
    }

    public boolean hasTimeToLive() {
        return false;
    }

    public int getTimeToLive() {
        return 0;
    }
}
