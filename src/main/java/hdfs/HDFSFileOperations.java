package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * hdfs demo
 */
public class HDFSFileOperations {

    private static final Logger logger = LoggerFactory.getLogger(HDFSFileOperations.class);

    public static final String FS_DEFAULT_NAME_KEY = CommonConfigurationKeys.FS_DEFAULT_NAME_KEY;
    public static final String HDFS_STORE_DIR_PATH_PREFIX = "/test";
    public static final String HDFS_STORE_DIR_PATH = "";
    public static final String CLIENT_USER = "bigdata";

    public static BufferedReader readFileStream(String lastPath) throws Exception{
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hadfs-site.xml");

        String dest = HDFS_STORE_DIR_PATH_PREFIX + "/" + HDFS_STORE_DIR_PATH + lastPath;
        Path path = new Path(dest);

        FileSystem fs = null;
        FSDataInputStream in = null;
        BufferedReader reader = null;

        try {
            fs = FileSystem.get(FileSystem.getDefaultUri(conf), conf, CLIENT_USER);
            FileStatus[] fileStatuses = fs.listStatus(path);

            for(int i = 0; i < fileStatuses.length; i++) {
                String pathname = fileStatuses[i].getPath().getName();
                FSDataInputStream inputStream = fs.open(fileStatuses[i].getPath());
                reader = new BufferedReader(new InputStreamReader(inputStream));
                String line = null;
                String returnStr = null;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
                inputStream.close();
                reader.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (in != null) {
                in.close();
            }
            if (fs != null) {
                fs.close();
            }
        }

        return null;
    }

    public static void addFileStream(final InputStream inputStream, boolean overwrite, Configuration conf, String lastpath) throws Exception{
        if(conf == null) {
            conf = new Configuration();
            conf.addResource("core-site.xml");
            conf.addResource("hadfs-core.xml");
        }

        String dest = HDFS_STORE_DIR_PATH_PREFIX + "/" + HDFS_STORE_DIR_PATH + lastpath;
        Path path = new Path(dest);

        FileSystem fs = null;
        FSDataOutputStream out = null;
        InputStream in = null;

        try{
            fs = FileSystem.get(FileSystem.getDefaultUri(conf), conf, CLIENT_USER);

            System.out.println(fs.exists(path));

            if(!fs.exists(path)) {
                out = fs.create(path, overwrite);
            } else {
                out = fs.append(path);
            }

            in = new BufferedInputStream(inputStream);

            byte[] buf = new byte[1024];
            int bytes = 0;
            while ((bytes = in.read(buf)) > 0) {
                out.write(buf, 0, bytes);
            }
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
            if(fs != null) {
                fs.close();
            }
        }

    }

    public static void main(String[] args) {
        try{
            //readFileStream("");
            String str = "长城外，古道边，荒草碧连天；问君此去何时还，来时莫徘徊";
            InputStream inputStream = new ByteArrayInputStream(str.getBytes());
            addFileStream(inputStream, true, null, "second.txt");
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
