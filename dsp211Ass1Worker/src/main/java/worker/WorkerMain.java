package worker;

import com.sun.corba.se.spi.orbutil.threadpool.Work;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class WorkerMain {


    public static void main(String[] args) {
        Worker worker = new Worker();
        worker.run();

    }

}
