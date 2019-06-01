package util;

//import com.intellif.search.center.utils.CommonUtil;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
/**
 * @author L@NG
 */
public class GuidIndexCache {

    public static final String GUID_INDEX = "/Users/chenjiaqi/mapdb";

    protected final DB db;


    /**
     * 用于存放业务amount与topic和index的映射关系
     */
    protected volatile HTreeMap<String,String> indexHashMap = null;


    /**
     * 计数器
     */
    protected volatile org.mapdb.Atomic.Integer count = null;

    /**
     * 创建缓存
     *
     * @param db 数据库
     */
    protected GuidIndexCache(DB db) {
        super();
        this.db = db;
        this.count = db.atomicInteger("count", 0).createOrOpen();
        this.db.commit();
    }

    /**
     * 创建堆外缓存
     *
     * @return 堆外缓存
     */
    public static GuidIndexCache createGuidIndexInstance() {
        //CommonUtil.formatFileName(GUID_INDEX)
        return createInstance(GUID_INDEX);
    }

    /**
     * 创建缓存
     *
     * @param fileName
     * @return 缓存
     */
    public static GuidIndexCache createInstance(String fileName) {
        DB db = DBMaker.fileDB(fileName)
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .cleanerHackEnable()
                .closeOnJvmShutdownWeakReference()
                .closeOnJvmShutdown()
                .transactionEnable()
                .concurrencyScale(128)
                .make();
        return new GuidIndexCache(db);
    }


    /**
     * 获取indexHashMap
     */
    public synchronized HTreeMap<String,String> getIndexHashMap() {
        if (null == indexHashMap) {
            indexHashMap = db.hashMap("indexHashMap", Serializer.STRING, Serializer.STRING).createOrOpen();
        }
        return indexHashMap;
    }


    /**
     * 是否空闲
     */
    public synchronized boolean isFree() {
        return null == count || count.get() <= 0;
    }


    /**
     * 是否已经关闭
     *
     * @return 是否已经关闭
     */
    public boolean isClosed() {
        return db.isClosed();
    }
}
