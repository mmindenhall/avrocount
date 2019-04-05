Avrocount
========================
This project was forked from [jwoschitz/avrocount](https://github.com/jwoschitz/avrocount), and modified
to be usable from within Java code without any dependency on hadoop or avro-tools.  The only dependency is on the 
avro core components (`org.apache.avro:avro`), which is declared as `compileOnly` in the build.gradle (i.e., the
client must provide avro on its classpath).

Currently there's just a single jar that is created from the build (under `build/libs`).  This must be manually
included in a project, or uploaded to a repository (e.g., artifactory).

Usage
------------------------
The constructor accepts either a filesystem (local) path, or an `InputStream`.

```java
    // file path
    String path = "/tmp/myFile.avro";
    long count = new AvroCount(path).count();

    // input stream
    InputStream inStream = ...;
    long count = new AvroCount(inStream).count();

```