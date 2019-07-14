package avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import com.bigdata.entity.product;

public class ArvoTest {

    public static void main(String[] args) throws IOException {
        //avro序列化
        product pro = product.newBuilder().build();
        pro.setProductId("2");
        pro.setCompanyName("htcl");
        pro.setProductInfo("we will become more stronger");
        pro.setDirection("6");
        //序列化数据并保存在本地
        File file = new File("/Users/test/Downloads/user.avsc");
        DatumWriter<product> productDataFileWriter = new SpecificDatumWriter<>(product.class);
        DataFileWriter<product> dataFileWriter = new DataFileWriter<>(productDataFileWriter);
        dataFileWriter.create(product.getClassSchema(), file);
        dataFileWriter.append(pro);
        dataFileWriter.close();
        System.out.println("序列化完毕");

        //arvo反序列化
        DatumReader<product> productDatumReader = new SpecificDatumReader<>(product.class);
        DataFileReader<product> dataFileReader = new DataFileReader<>(file, productDatumReader);
        product pro1 = null;

        while (dataFileReader.hasNext()) {
            pro1 = dataFileReader.next();
            System.out.println(pro1.toString());
        }

    }

}
