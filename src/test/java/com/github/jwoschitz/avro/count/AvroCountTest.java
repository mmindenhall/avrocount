package com.github.jwoschitz.avro.count;

import com.github.jwoschitz.avro.count.utils.FileTestUtil;
import org.apache.avro.file.CodecFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static com.github.jwoschitz.avro.count.utils.AvroDataFileGenerator.intRecordGenerator;
import static org.junit.Assert.assertEquals;

public class AvroCountTest {

    @Rule
    public TestName testName = new TestName();

    @Test
    public void testCountOneFileNoCodec() throws Exception {
        testCountOneFileWithCodec(null, 1000);
    }

    @Test
    public void testCountOneFileNullCodec() throws Exception {
        testCountOneFileWithCodec(CodecFactory.nullCodec(), 1000);
    }

    @Test
    public void testCountOneFileSnappy() throws Exception {
        testCountOneFileWithCodec(CodecFactory.snappyCodec(), 1000);
    }

    @Test
    public void testCountOneFileDeflate() throws Exception {
        testCountOneFileWithCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL), 1000);
    }

    @Test
    public void testCountOneFileBzip2() throws Exception {
        testCountOneFileWithCodec(CodecFactory.bzip2Codec(), 1000);
    }

    @Test
    public void testCountOneFileXz() throws Exception {
        testCountOneFileWithCodec(CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL), 1000);
    }

    @Test
    public void testCountSmallFileNullCodec() throws Exception {
        testCountOneFileWithCodec(CodecFactory.nullCodec(), 1);
    }

    @Test
    public void testCountSmallFileSnappyCodec() throws Exception {
        testCountOneFileWithCodec(CodecFactory.snappyCodec(), 1);
    }

    private void testCountOneFileWithCodec(CodecFactory codec, long recordCount) throws Exception {
        File avroFile = intRecordGenerator(getClass(), codec)
                .createAvroFile(String.format("%s.avro", testName.getMethodName()), recordCount);

        AvroCount fileCounter = new AvroCount(avroFile.getPath());
        long fileCount = fileCounter.count();

        AvroCount isCounter = new AvroCount(new FileInputStream(avroFile));
        long isCount = isCounter.count();

        assertEquals(recordCount, fileCount);
        assertEquals(recordCount, isCount);
    }

    @Test(expected = IOException.class)
    public void testIgnoreNonAvroSuffixedFile() throws Exception {
        File someFile = FileTestUtil.createNewFile(getClass(), "not_an_avro.file");

        AvroCount fileCounter = new AvroCount(someFile.getPath());
        fileCounter.count();
    }

    @Test(expected = Exception.class)
    public void testRaiseExceptionIfFileIsNotAvro() throws Exception {
        File someFile = FileTestUtil.createNewFile(getClass(), "not_an_avro.avro");

        AvroCount fileCounter = new AvroCount(someFile.getPath());
        fileCounter.count();
    }
}
