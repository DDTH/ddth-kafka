package com.github.ddth.kafka.internal;

import java.util.Random;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * A simple random {@link Partitioner} implementation.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.1
 */
public class RandomPartitioner implements Partitioner {

    private Random random = new Random(System.currentTimeMillis());

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
        return random.nextInt(Integer.MAX_VALUE) % numPartitions;
    }

}
