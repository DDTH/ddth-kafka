package com.github.ddth.kafka.internal;

import java.util.Random;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import com.github.ddth.commons.utils.HashUtils;

/**
 * A simple random {@link Partitioner} implementation.
 * 
 * <ul>
 * <li>Message with {@code null} or empty key will be put into a random
 * partition.</li>
 * <li>Message with non-empty key will be put into partition based on it's key's
 * hash value.</li>
 * </ul>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.1
 * @deprecated since 1.2.0 as of Kafka 0.8.2 the Java producer does not support
 *             custom partitioner yet, and the default one just works in most
 *             (if not all) cases!
 */
public class RandomPartitioner implements Partitioner {

    private static Random random = new Random(System.currentTimeMillis());

    public RandomPartitioner() {
    }

    public RandomPartitioner(VerifiableProperties props) {
    }

    public void init(VerifiableProperties props) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int partition(Object key, int numPartitions) {
        System.out.println("DEBUG : " + key + " | " + numPartitions);
        if (key == null || "".equals(key)) {
            return random.nextInt() % numPartitions;
        }
        return (int) HashUtils.linearHashingMap(key, numPartitions);
    }

}
