package mapDB;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import util.GuidIndexCache;

import java.util.HashMap;

public class MapDBDemo {

    public static void main(String[] args) {
        /*GuidIndexCache guidIndexCache = GuidIndexCache.createGuidIndexInstance();
        HTreeMap<String, String> hTreeMap = guidIndexCache.getIndexHashMap();
        hTreeMap.put("tom", "24");
        hTreeMap.put("jerry", "23");
        hTreeMap.put("jack", "25");
        hTreeMap.forEach((key, value) -> {
            System.out.println(key + "---" + value);
        });*/

        DB db = DBMaker.memoryDB().make();
        HTreeMap<String, String> map = db.hashMap("test", Serializer.STRING, Serializer.STRING).createOrOpen();
        map.put("test1", "haha1");
        map.put("test2", "haha2");
        map.put("test3", "haha3");
        map.forEach((key, value) -> {
            System.out.println(key + "---" + value);
        });
    }

}
