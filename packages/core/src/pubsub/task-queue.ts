import PQueue from 'p-queue';

const noop = () => {
  // Do Nothing
};

type Task<TaskResultType> = (() => PromiseLike<TaskResultType>) | (() => TaskResultType)

/**
 * PQueue with synchronous `add` and a common error-handler.
 */
export class TaskQueue {
  #pq = new PQueue({ concurrency: 1 });

  /**
   * @param onError - Common error handler.
   */
  constructor(private readonly onError: (error: Error) => void = noop) {}

  /**
   * Add task to queue.
   */
  add(f: Task<void>): void {
    this.#pq.add(f).catch((error) => this.onError(error));
  }

  /**
   * Wait till all the tasks are completed.
   */
  onIdle(): Promise<void> {
    return this.#pq.onIdle();
  }

  /**
   * Clear the queue.
   */
  clear() {
    this.#pq.clear();
  }
}
