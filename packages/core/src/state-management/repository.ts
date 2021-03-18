import DocID from '@ceramicnetwork/docid';
import { DocumentFactory } from './document-factory';
import { DocOpts, DocState, DocStateHolder } from '@ceramicnetwork/common';
import { PinStore } from '../store/pin-store';
import { NetworkLoad } from './network-load';
import { NamedTaskQueue } from './named-task-queue';
import { DiagnosticsLogger } from '@ceramicnetwork/logger';
import { ExecutionQueue } from './execution-queue';
import { RunningState } from './running-state';
import { LRUMap } from 'lru_map';
import { Subject } from 'rxjs';
import { StateManager } from './state-manager';

export class Repository {
  readonly loadingQ: NamedTaskQueue;
  readonly executionQ: ExecutionQueue;

  readonly feed$: Subject<DocState>;

  readonly #map: LRUMap<string, RunningState>;
  #documentFactory?: DocumentFactory;
  stateManager: StateManager;
  pinStore?: PinStore;
  #networkLoad?: NetworkLoad;

  constructor(limit: number, logger: DiagnosticsLogger) {
    this.loadingQ = new NamedTaskQueue(error => {
      logger.err(error)
    })
    this.executionQ = new ExecutionQueue(logger, (docId) => this.get(docId));
    this.#map = new LRUMap(limit);
    this.#map.shift = function () {
      const entry = LRUMap.prototype.shift.call(this);
      entry[1].complete();
      return entry;
    };
    this.feed$ = new Subject();
  }

  // Ideally this would be provided in the constructor, but circular dependencies in our initialization process make this necessary for now
  setDocumentFactory(documentFactory: DocumentFactory): void {
    this.#documentFactory = documentFactory;
    this.stateManager = new StateManager(
      this.#documentFactory.dispatcher,
      this.#documentFactory.pinStore,
      this.executionQ,
      this.#documentFactory.context.anchorService,
      this.#documentFactory.conflictResolution,
    );
  }

  // Ideally this would be provided in the constructor, but circular dependencies in our initialization process make this necessary for now
  setPinStore(pinStore: PinStore) {
    this.pinStore = pinStore;
  }

  setNetworkLoad(networkLoad: NetworkLoad) {
    this.#networkLoad = networkLoad;
  }

  fromMemory(docId: DocID): RunningState | undefined {
    return this.#map.get(docId.toString());
  }

  async fromStateStore(docId: DocID): Promise<RunningState | undefined> {
    if (this.pinStore && this.#documentFactory) {
      const docState = await this.pinStore.stateStore.load(docId);
      if (docState) {
        const runningState = new RunningState(docState);
        await this.add(runningState);
        return runningState;
      } else {
        return undefined;
      }
    }
  }

  async fromNetwork(docId: DocID, opts: DocOpts = {}): Promise<RunningState> {
    const state$ = await this.#networkLoad.load(docId);
    await this.add(state$);
    await this.stateManager.syncGenesis(state$, opts);
    return state$;
  }

  /**
   * Returns a document from wherever we can get information about it.
   * Starts by checking if the document state is present in the in-memory cache, if not then then checks the state store, and finally loads the document from pubsub.
   */
  async load(docId: DocID, opts: DocOpts = {}): Promise<RunningState> {
    return this.loadingQ.run(docId.toString(), async () => {
      const fromMemory = this.fromMemory(docId);
      if (fromMemory) return fromMemory;
      const fromStateStore = await this.fromStateStore(docId);
      if (fromStateStore) return fromStateStore;
      return this.fromNetwork(docId, opts);
    });
  }

  /**
   * Checks if we can get the document state without having to load it via pubsub (i.e. we have the document state in our in-memory cache or in the state store)
   */
  async has(docId: DocID): Promise<boolean> {
    const fromMemory = this.fromMemory(docId);
    if (fromMemory) return true;
    const fromState = await this.pinStore.stateStore.load(docId);
    return Boolean(fromState);
  }

  /**
   * Return a document, either from cache or re-constructed from state store, but will not load from the network.
   * Adds the document to cache.
   */
  async get(docId: DocID): Promise<RunningState | undefined> {
    return this.loadingQ.run(docId.toString(), async () => {
      const fromMemory = this.fromMemory(docId);
      if (fromMemory) return fromMemory;
      return this.fromStateStore(docId);
    });
  }

  /**
   * Return a document state, either from cache or from state store.
   */
  async docState(docId: DocID): Promise<DocState | undefined> {
    const fromMemory = this.#map.get(docId.toString());
    if (fromMemory) {
      return fromMemory.state;
    } else {
      if (this.pinStore) {
        return this.pinStore.stateStore.load(docId);
      }
    }
  }

  /**
   * Stub for adding the document.
   */
  add(state: RunningState): void {
    this.#map.set(state.id.toString(), state);
  }

  pin(docStateHolder: DocStateHolder): Promise<void> {
    return this.pinStore.add(docStateHolder);
  }

  unpin(docId: DocID): Promise<void> {
    return this.pinStore.rm(docId);
  }

  /**
   * List pinned documents as array of DocID strings.
   * If `docId` is passed, indicate if it is pinned.
   */
  async listPinned(docId?: DocID): Promise<string[]> {
    if (this.pinStore) {
      return this.pinStore.stateStore.list(docId);
    } else {
      return [];
    }
  }

  async close(): Promise<void> {
    await this.loadingQ.close();
    await this.executionQ.close();
    Array.from(this.#map).forEach(([id, document]) => {
      this.#map.delete(id);
      document.complete();
    });
    await this.pinStore.close();
  }
}
