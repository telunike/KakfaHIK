package util;

import org.mapdb.Atomic.Var;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DBMaker.Maker;
import org.mapdb.HTreeMap;
import org.mapdb.IndexTreeList;
import org.mapdb.Serializer;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author administrator
 */
public class UUIdCache {

    /**
     * 使用堆内存，有点像JDK自带容器，性能比其它两种方式高一个数量级，但是大对象会对GC照成较大压力，适合小对象
     */
    public static final String TYPE_HEAP = "heap";

    /**
     * 使用JVM老年代，会将内容存入以1M为单位的内存块中，该块交由MapDB自己管理，可以减轻GC的压力，适合不怎么修改的大对象
     */
    public static final String TYPE_MEMORY = "memory";

    /**
     * 使用堆外内存，不受JVM内存管理约束，性能较TYPE_MEMORY稍差，不会照成任何GC，适合经常变化的大对象
     */
    public static final String TYPE_MEMORY_DIRECT = "memoryDirect";

    /**
     * 数据库
     */
    protected final DB db;

    /**
     * 用于存放业务时间与docId的映射关系
     */
    protected volatile BTreeMap<String, String> treeMap = null;

    /**
     * 用于存放业务时间与docId的映射关系
     */
    protected volatile HTreeMap<String, String> hashMap = null;

    /**
     * 用于存放docId的列表
     */
    protected volatile IndexTreeList<Integer> list = null;

    /**
     * 用于存放docId的变量
     */
    protected volatile Var<int[]> var = null;

    /**
     * 计数器
     */
    protected volatile org.mapdb.Atomic.Integer count = null;

    /**
     * 创建缓存
     *
     * @param db 数据库
     */
    protected UUIdCache(DB db) {
        super();
        this.db = db;
        this.count = db.atomicInteger("count", 0).createOrOpen();
    }

    /**
     * 创建堆外缓存
     *
     * @return 堆外缓存
     */
    public static UUIdCache createInstance() {
        return createInstance(TYPE_MEMORY_DIRECT);
    }

    /**
     * 创建缓存
     *
     * @param type 缓存类型
     * @return 缓存
     */
    public static UUIdCache createInstance(String type) {
        Maker maker;
        switch (type) {
            case TYPE_HEAP:
                maker = DBMaker.heapDB();
                break;
            case TYPE_MEMORY:
                maker = DBMaker.memoryDB();
                break;
            default:
                maker = DBMaker.memoryDirectDB();
                break;
        }
        DB db = maker.cleanerHackEnable().closeOnJvmShutdownWeakReference().closeOnJvmShutdown().make();
        return new UUIdCache(db);
    }


    /**
     * 获取treeMap
     */
    public synchronized BTreeMap<String, String> getTreeMap() {
        if (null == treeMap) {
            treeMap = db.treeMap("map", Serializer.STRING, Serializer.STRING).createOrOpen();
        }
        return treeMap;
    }

    /**
     * 获取hashMap
     */
    public synchronized HTreeMap<String, String> getHashMap() {
        if (null == hashMap) {
            hashMap = db.hashMap("hashMap", Serializer.STRING, Serializer.STRING).
                    expireAfterUpdate(10, TimeUnit.MINUTES).
                    expireAfterCreate(10, TimeUnit.MINUTES).
                    expireAfterGet(1, TimeUnit.MINUTES).
                    createOrOpen();
        }
        return hashMap;
    }

    /**
     * 获取var
     */
    public synchronized Var<int[]> getVar() {
        if (null == var) {
            var = db.atomicVar("var", Serializer.INT_ARRAY).createOrOpen();
        }
        return var;
    }

    /**
     * 获取list
     */
    public synchronized List<Integer> getList() {
        if (null == list) {
            list = db.indexTreeList("list", Serializer.INTEGER).createOrOpen();
        }
        return list;
    }

    /**
     * 借用缓存
     */
    public synchronized void borrow() {
        if (null != count) {
            count.incrementAndGet();
        }
    }

    /**
     * 归还缓存
     */
    public synchronized void giveBack() {
        if (null != count) {
            count.decrementAndGet();
        }
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
