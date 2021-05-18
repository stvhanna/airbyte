/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.workers.temporal;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.functional.CheckedConsumer;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.scheduler.models.JobRunConfig;
import io.airbyte.workers.Worker;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This class allow a worker to run multiple times. In addition to the functionality in
 * TemporalAttemptExecution it takes a predicate to determine if the output of a worker constitutes
 * a complete success or a partial one. It also takes a function that takes in the input of the
 * previous run of the worker and the output of the last worker in order to generate a new input for
 * that worker.
 */
public class PartialSuccessTemporalAttemptExecution<INPUT, OUTPUT> implements Supplier<List<OUTPUT>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartialSuccessTemporalAttemptExecution.class);

  private final CheckedSupplier<Worker<INPUT, OUTPUT>, Exception> workerSupplier;
  private final Supplier<INPUT> inputSupplier;
  private final CancellationHandler cancellationHandler;

  private final Predicate<OUTPUT> shouldAttemptAgainPredicate;
  private final BiFunction<INPUT, OUTPUT, INPUT> computeNextAttemptInputFunction;
  private final int maxRetriesCount;
  private final TemporalAttemptExecutionFactory<INPUT, OUTPUT> temporalAttemptExecutionFactory;
  private final Path workspaceRoot;
  private final JobRunConfig jobRunConfig;

  public PartialSuccessTemporalAttemptExecution(Path workspaceRoot,
                                                JobRunConfig jobRunConfig,
                                                CheckedSupplier<Worker<INPUT, OUTPUT>, Exception> workerSupplier,
                                                Supplier<INPUT> initialInputSupplier,
                                                CancellationHandler cancellationHandler,
                                                Predicate<OUTPUT> shouldAttemptAgainPredicate,
                                                BiFunction<INPUT, OUTPUT, INPUT> computeNextAttemptInputFunction,
                                                int maxRetriesCount) {
    this(
        workspaceRoot,
        jobRunConfig,
        workerSupplier,
        initialInputSupplier,
        cancellationHandler,
        shouldAttemptAgainPredicate,
        computeNextAttemptInputFunction,
        maxRetriesCount,
        TemporalAttemptExecution::new);
  }

  @VisibleForTesting
  PartialSuccessTemporalAttemptExecution(Path workspaceRoot,
                                         JobRunConfig jobRunConfig,
                                         CheckedSupplier<Worker<INPUT, OUTPUT>, Exception> workerSupplier,
                                         Supplier<INPUT> initialInputSupplier,
                                         BiConsumer<Path, String> mdcSetter,
                                         CheckedConsumer<Path, IOException> jobRootDirCreator,
                                         CancellationHandler cancellationHandler,
                                         Supplier<String> workflowIdProvider,
                                         Predicate<OUTPUT> shouldAttemptAgainPredicate,
                                         BiFunction<INPUT, OUTPUT, INPUT> computeNextAttemptInputFunction,
                                         int maxRetriesCount) {
    this(
        workspaceRoot,
        jobRunConfig,
        workerSupplier,
        initialInputSupplier,
        cancellationHandler,
        shouldAttemptAgainPredicate,
        computeNextAttemptInputFunction,
        maxRetriesCount,
        (Path workspaceRootArg,
         JobRunConfig jobRunConfigArg,
         CheckedSupplier<Worker<INPUT, OUTPUT>, Exception> workerSupplierArg,
         Supplier<INPUT> initialInputSupplierArg,
         // for testing.
         CancellationHandler e) -> new TestTemporalAttemptExecutionFactory<INPUT, OUTPUT>(mdcSetter, jobRootDirCreator, workflowIdProvider)
             .create(workspaceRootArg, jobRunConfigArg, workerSupplierArg, initialInputSupplierArg, e));
  }

  @VisibleForTesting
  PartialSuccessTemporalAttemptExecution(Path workspaceRoot,
                                         JobRunConfig jobRunConfig,
                                         CheckedSupplier<Worker<INPUT, OUTPUT>, Exception> workerSupplier,
                                         Supplier<INPUT> initialInputSupplier,
                                         CancellationHandler cancellationHandler,
                                         Predicate<OUTPUT> shouldAttemptAgainPredicate,
                                         BiFunction<INPUT, OUTPUT, INPUT> computeNextAttemptInputFunction,
                                         int maxRetriesCount,
                                         TemporalAttemptExecutionFactory<INPUT, OUTPUT> temporalAttemptExecutionFactory) {
    this.workspaceRoot = workspaceRoot;
    this.jobRunConfig = jobRunConfig;
    this.workerSupplier = workerSupplier;
    this.inputSupplier = initialInputSupplier;
    this.cancellationHandler = cancellationHandler;
    this.shouldAttemptAgainPredicate = shouldAttemptAgainPredicate;
    this.computeNextAttemptInputFunction = computeNextAttemptInputFunction;
    this.maxRetriesCount = maxRetriesCount;
    this.temporalAttemptExecutionFactory = temporalAttemptExecutionFactory;
  }

  @Override
  public List<OUTPUT> get() {
    INPUT input = inputSupplier.get();
    final AtomicReference<OUTPUT> lastOutput = new AtomicReference<>();
    List<OUTPUT> outputCollector = new ArrayList<>();

    int i = 0;
    while (true) {
      if (i >= maxRetriesCount) {
        LOGGER.info("Max retries reached: {}", i);
        break;
      }

      final boolean hasLastOutput = lastOutput.get() != null;
      final boolean shouldAttemptAgain = !hasLastOutput || shouldAttemptAgainPredicate.test(lastOutput.get());
      LOGGER.info("Last output present: {}. Should attempt again: {}", lastOutput.get() != null, shouldAttemptAgain);
      if (hasLastOutput && !shouldAttemptAgain) {
        break;
      }

      LOGGER.info("Starting attempt: {} of {}", i, maxRetriesCount);

      Supplier<INPUT> resolvedInputSupplier = !hasLastOutput ? inputSupplier : () -> computeNextAttemptInputFunction.apply(input, lastOutput.get());

      final TemporalAttemptExecution<INPUT, OUTPUT> temporalAttemptExecution = temporalAttemptExecutionFactory.create(
          workspaceRoot,
          jobRunConfig,
          workerSupplier,
          resolvedInputSupplier,
          cancellationHandler);
      lastOutput.set(temporalAttemptExecution.get());
      outputCollector.add(lastOutput.get());
      i++;
    }

    return outputCollector;
  }

  // interface to make testing easier.
  @FunctionalInterface
  interface TemporalAttemptExecutionFactory<INPUT, OUTPUT> {

    TemporalAttemptExecution<INPUT, OUTPUT> create(Path workspaceRoot,
                                                   JobRunConfig jobRunConfig,
                                                   CheckedSupplier<Worker<INPUT, OUTPUT>, Exception> workerSupplier,
                                                   Supplier<INPUT> inputSupplier,
                                                   CancellationHandler cancellationHandler);

  }

  private static class TestTemporalAttemptExecutionFactory<INPUT, OUTPUT> implements TemporalAttemptExecutionFactory<INPUT, OUTPUT> {

    private final BiConsumer<Path, String> mdcSetter;
    private final CheckedConsumer<Path, IOException> jobRootDirCreator;
    private final Supplier<String> workflowIdProvider;

    public TestTemporalAttemptExecutionFactory(
                                               BiConsumer<Path, String> mdcSetter,
                                               CheckedConsumer<Path, IOException> jobRootDirCreator,
                                               Supplier<String> workflowIdProvider) {
      this.mdcSetter = mdcSetter;
      this.jobRootDirCreator = jobRootDirCreator;
      this.workflowIdProvider = workflowIdProvider;
    }

    @Override
    public TemporalAttemptExecution<INPUT, OUTPUT> create(final Path workspaceRoot,
                                                          final JobRunConfig jobRunConfig,
                                                          final CheckedSupplier<Worker<INPUT, OUTPUT>, Exception> workerSupplier,
                                                          final Supplier<INPUT> inputSupplier,
                                                          final CancellationHandler cancellationHandler) {
      return new TemporalAttemptExecution<>(
          workspaceRoot,
          jobRunConfig,
          workerSupplier,
          inputSupplier,
          mdcSetter,
          jobRootDirCreator,
          cancellationHandler,
          workflowIdProvider);
    }

  }

}
