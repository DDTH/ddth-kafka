package com.github.ddth.kafka.internal;

import java.util.concurrent.atomic.AtomicInteger;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * A round-robin {@link Partitioner} implementation.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.1
 */
public class RoundRobinPartitioner implements Partitioner {

    private AtomicInteger counter = new AtomicInteger(0);

    public RoundRobinPartitioner() {
    }

    public RoundRobinPartitioner(VerifiableProperties props) {
    }

    public void init(VerifiableProperties props) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int partition(Object key, int numPartitions) {
        int result = counter.incrementAndGet() % numPartitions;
        if (counter.get() > 65536) {
            counter.set(0);
        }
        return result;
    }
}
