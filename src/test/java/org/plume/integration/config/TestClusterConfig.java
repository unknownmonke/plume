package org.plume.integration.config;

import org.testcontainers.utility.DockerImageName;

/**
 * Basic configuration for integration test cluster.
 */
public final class TestClusterConfig {

    private static final String KAFKA_VERSION = "4.2.0";
    public static final DockerImageName KAFKA_IMAGE_NAME = DockerImageName
        .parse("apache/kafka")
        .withTag(KAFKA_VERSION);

    public static final Integer CLUSTER_PORT = 9092;
}
