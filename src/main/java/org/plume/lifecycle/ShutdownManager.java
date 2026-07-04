package org.plume.lifecycle;

/**
 * Allows to control consumer shutdown from outside and not rely on JVM default hook.
 *
 * <p> Useful for applicative frameworks (Spring, Quarkus...) which handle their own lifecycle.
 *
 * <p> <i>Ex :</i>
 * <pre> {@code
 * @Bean
 * public ShutdownManager shutdownManager(ApplicationContext context) {
 *     return hook -> context.publishEvent(new ContextClosedEvent(context));
 * }}
 *
 * <p> Avoid joining the main / test thread because joining threads from within the shutdown hook can deadlock the JVM shutdown
 * (the hook runs concurrently with other non-daemon threads).
 *
 * <p> This interface is named specifically, instead of a generic {@code LifecycleHookManager}
 * that could handle either startup or shutdown to provide clarity about purpose.
 */
@FunctionalInterface
public interface ShutdownManager {
    void registerShutdownHook(Runnable shutdown);
}
