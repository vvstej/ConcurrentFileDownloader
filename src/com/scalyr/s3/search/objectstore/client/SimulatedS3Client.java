package com.scalyr.s3.search.objectstore.client;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.scalyr.s3search.s3simulation.NetworkSimulator;
import com.scalyr.s3search.utilities.FastRandom;

/**
 * Implements a simulated version of Amazon S3.
 */
public class SimulatedS3Client implements ObjectStoreClient {
    /**
     * Root of the local filesystem tree containing simulated S3 objects.
     */
    private final File rootDirectory;

    /**
     * Simulation of the network through which we read from S3.
     */
    private final NetworkSimulator networkSimulator;

    /**
     * Fraction of `readFileFromS3` calls that should throw `FlakyNetworkException`.
     */
    private final double exceptionRate;


    /** Models a transient network error. */
    public static final class FlakyNetworkException extends IOException {
        public FlakyNetworkException(String msg) {
            super(msg);
        }
    }



    /**
     * Random number generator used to generate simulated queue delays. We use the FastRandom class because it is
     * threadsafe.
     */
    private final FastRandom rng = new FastRandom();

    /**
     * Maps bucketName/objectName to the contents of the corresponding simulated S3 object. Acts as a cache of the local
     * filesystem. Populated lazily / on demand.
     */
    private final Map<String, byte[]> fileCache = new ConcurrentHashMap<String, byte[]>();

    /*
     * Fully defaulted constructor, convenience for `this(new File("."), new NetworkSimulator())`;
     */
    public SimulatedS3Client() {
        this(new File("."), new NetworkSimulator());
    }

    /**
     * Construct a SimulatedS3Client to read files from the specified local disk directory, with a default exception
     * rate. We expect files to appear at ROOTDIRECTORY/bucketName/objectName.
     *
     * @param rootDirectory Root of the local filesystem tree containing simulated S3 objects. Each bucketName
     *        corresponds to a subdirectory of this directory.
     * @param networkSimulator
     */
    public SimulatedS3Client(File rootDirectory, NetworkSimulator networkSimulator) {
        this(rootDirectory, networkSimulator, 0.0025); // default exception rate
    }

    SimulatedS3Client(File rootDirectory, NetworkSimulator networkSimulator, double exceptionRate) {
        this.rootDirectory = rootDirectory;
        this.networkSimulator = networkSimulator;
        this.exceptionRate = exceptionRate;
    }

    /**
     * Return the contents of a specified S3 object.
     *
     * This method will take some time to return, reflecting simulated delays for disk and network access.
     *
     * As currently implemented, we return objects from a local cache. Hence, this method generally won't allocate
     * memory, helping to minimize garbage collection as a factor in simulations.
     */
    public byte[] readFileFromS3(String bucketName, String objectName) throws FlakyNetworkException {
        if (rng.nextDouble() < exceptionRate)
            throw new FlakyNetworkException("transient network error, please retry");

        // Sleep for the simulated queuing and disk delay.
        try {
            Thread.sleep(simulatedDiskReadTime());
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        byte[] result = getFileContentsWithCaching(bucketName, objectName);
        if (result != null) {
            networkSimulator.waitForTraffic(result.length);
        }

        return result;
    }

    /**
     * Return the contents of the local disk file corresponding to bucketName/objectName, using (and populating)
     * fileCache. If the file does not exist, we throw a RuntimeException.
     */
    private byte[] getFileContentsWithCaching(String bucketName, String objectName) {
        String cacheKey = bucketName + "/" + objectName;
        byte[] result = fileCache.get(cacheKey);
        if (result == null) {
            result = readFileUncached(new File(rootDirectory, cacheKey));
            fileCache.put(cacheKey, result);
        }

        return result;
    }

    /**
     * Return the contents of the given file. If the file does not exist, throw a RuntimeException.
     */
    private static byte[] readFileUncached(File file) {
        try {
            FileInputStream stream = new FileInputStream(file);
            try {
                ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
                byte[] buffer = new byte[4096];
                while (true) {
                    int count = stream.read(buffer, 0, buffer.length);
                    if (count <= 0) {
                        break;
                    }
                    bufferStream.write(buffer, 0, count);
                }
                return bufferStream.toByteArray();
            } finally {
                try {
                    stream.close();
                } catch (IOException ex) {
                    // ignore
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Generate a simulated delay to read data from disk. This is intended to model one component of S3 read time -- the
     * actual disk seek, plus any queuing delays. Network transfer time is modeled separately. We assume that disk
     * transfer time is buried in the network transfer time.
     *
     * @return Simulated read delay, in milliseconds.
     */
    private int simulatedDiskReadTime() {
        // Based on the following real-world measurements for reading 256KB of data from S3 in a single
        // thread on a fast instance:
        //
        // Minimum time: 13 ms
        // 10th percentile: 38 ms
        // 50th percentile: 58 ms
        // 90th percentile: 78 ms
        // 99th percentile: 216 ms
        // 99.9th percentile: 527 ms
        // Maximum time: 3737 ms

        int percentile = rng.nextInt(1000);
        if (percentile < 100) {
            // Below 10th percentile.
            return randomValueInRange(13, 38);
        } else if (percentile < 500) {
            // Between 10th and 50th percentile.
            return randomValueInRange(38, 58);
        } else if (percentile < 900) {
            // Between 50th and 90th percentile.
            return randomValueInRange(58, 78);
        } else if (percentile < 990) {
            // Between 90th and 99th percentile.
            return randomValueInRange(78, 216);
        } else if (percentile < 999) {
            // Between 99th and 99.9th percentile.
            return randomValueInRange(216, 527);
        } else {
            // Above the 99.9th percentile.
            return randomValueInRange(527, 3737);
        }
    }

    /**
     * Return a random number in the range [low, high).
     */
    private int randomValueInRange(int low, int high) {
        return low + rng.nextInt(high - low);
    }

    @Override
    public byte[] downloadFile(String bucketName, String fileName) throws FlakyNetworkException {
        return readFileFromS3(bucketName, fileName);
    }
}
