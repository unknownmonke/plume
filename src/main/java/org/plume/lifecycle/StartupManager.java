package org.plume.lifecycle;

/**
 * Allows to delay consumer startup.
 * Consumer will defer polling from its {@code run} method, until this hook is executed.
 *
 * <p> Useful for applicative frameworks (Spring, Quarkus...) which handle their own lifecycle.
 *
 * <p> <i>Ex :</i>
 * <pre> {@code
 * @Bean
 * public StartupManager startupManager(TaskExecutor executor) {
 *     return startup -> executor.execute(startup);
 * }}
 *
 * <p> This interface is named specifically, instead of a generic {@code LifecycleHookManager}
 * that could handle either startup or shutdown to provide clarity about purpose.
 */
@FunctionalInterface
public interface StartupManager {
    void registerStartupHook(Runnable startup);
}
