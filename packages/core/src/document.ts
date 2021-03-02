import { Dispatcher } from './dispatcher'
import CID from 'cids'
import { EventEmitter } from 'events'
import PQueue from 'p-queue'
import cloneDeep from 'lodash.clonedeep'
import Utils from './utils'
import {
  AnchorStatus,
  DocState,
  Doctype,
  DoctypeHandler,
  DocOpts,
  Context,
  DoctypeUtils,
  DocStateHolder,
  UnreachableCaseError,
} from '@ceramicnetwork/common';
import DocID, { CommitID } from '@ceramicnetwork/docid';
import { PinStore } from './store/pin-store';
import { SubscriptionSet } from "./subscription-set";
import { concatMap } from "rxjs/operators";
import { DiagnosticsLogger } from "@ceramicnetwork/logger";
import { BehaviorSubject } from 'rxjs'
import { ConflictResolution } from './conflict-resolution';
import { DEFAULT_WRITE_DOCOPTS } from './loading-queue';


/**
 * Document handles the update logic of the Doctype instance
 */
export class Document extends EventEmitter implements DocStateHolder {
  readonly id: DocID
  private readonly state$: BehaviorSubject<DocState>
  private readonly subscriptionSet = new SubscriptionSet();

  private _applyQueue: PQueue
  private _logger: DiagnosticsLogger
  private _isProcessing: boolean
  _doctype: Doctype

  private readonly conflictResolution: ConflictResolution;

  constructor (initialState: DocState,
               readonly dispatcher: Dispatcher,
               readonly pinStore: PinStore,
               private readonly _validate: boolean,
               private readonly _context: Context,
               private readonly handler: DoctypeHandler<Doctype>,
               readonly = false) {
    super()
    const doctype = new handler.doctype(initialState, _context)
    this._doctype = readonly ? DoctypeUtils.makeReadOnly(doctype) : doctype // FIXME NEXT Snapshot vs Running
    this.id = new DocID(handler.name, initialState.log[0].cid)
    this.state$ = new BehaviorSubject(initialState)
    // FIXME NEXT distinct only
    this.state$.subscribe(state => {
      this._doctype.state = state
      this._doctype.emit('change')
    })

    this._logger = _context.loggerProvider.getDiagnosticsLogger()

    this._applyQueue = new PQueue({ concurrency: 1 })
    this.conflictResolution = new ConflictResolution(_context, dispatcher, handler, _validate)
  }

  /**
   * Takes a document containing only the genesis commit and kicks off the process to load and apply
   * the most recent Tip to it.
   * @private
   */
  async _syncDocumentToCurrent(pinStore: PinStore, opts: DocOpts): Promise<void> {
    // TODO: Assert that doc contains only the genesis commit
    // Update document state to cached state if any
    const pinnedState = await pinStore.stateStore.load(this.id)
    if (pinnedState) {
      this.state$.next(pinnedState)
    }

    this.on('update', this._update)

    await this.dispatcher.register(this)

    await this._applyOpts(opts)
  }

  /**
   * Applies commit to the existing Doctype
   *
   * @param commit - Commit data
   * @param opts - Document initialization options (request anchor, wait, etc.)
   */
  async applyCommit (commit: any, opts: DocOpts = {}): Promise<void> {
    // Fill 'opts' with default values for any missing fields
    opts = {...DEFAULT_WRITE_DOCOPTS, ...opts}

    const cid = await this.dispatcher.storeCommit(commit)

    await this._handleTip(cid)
    await this._updateStateIfPinned()
    await this._applyOpts(opts)
  }

  /**
   * Apply initialization options
   *
   * @param opts - Initialization options (request anchor, wait, etc.)
   * @private
   */
  async _applyOpts(opts: DocOpts): Promise<void> {
    const anchor = opts.anchor ?? true
    const publish = opts.publish ?? true
    const sync = opts.sync ?? true
    if (anchor) {
      this.anchor();
    }
    if (publish) {
      this._publishTip()
    }
    if (sync) {
      await this._wait()
    }
  }

  /**
   * Updates document state if the document is pinned locally
   *
   * @private
   */
  async _updateStateIfPinned(): Promise<void> {
    const isPinned = Boolean(await this.pinStore.stateStore.load(this.id))
    if (isPinned) {
      await this.pinStore.add(this)
    }
  }

  /**
   * Handles update from the PubSub topic
   *
   * @param cid - Document Tip CID
   * @private
   */
  async _update(cid: CID): Promise<void> {
    try {
      await this._handleTip(cid)
    } catch (e) {
      this._logger.err(e)
    } finally {
      this._isProcessing = false
    }
  }

  /**
   * Handles Tip from the PubSub topic
   *
   * @param cid - Document Tip CID
   * @private
   */
  async _handleTip(cid: CID): Promise<void> {
    try {
      this._isProcessing = true;
      await this._applyQueue.add(async () => {
        const next = await this.conflictResolution.applyTip(this.state$.value, cid);
        if (next) {
          this.state$.next(next);
        }
      });
    } finally {
      this._isProcessing = false;
    }
  }

  /**
   * Takes the most recent known-about version of a document and a specific commit and returns a new
   * Document instance representing the same document but set to the state of the document at the
   * requested commit.  If the requested commit is for a branch of history that conflicts with the
   * known current version of the document, throws an error. Intentionally does not register the new
   * document so that it does not get notifications about newer commits, since we want it tied to a
   * specific commit.
   * @param commitId - DocID of the document including the requested commit
   */
  async rewind(commitId: CommitID) {
    const resetState = await this.conflictResolution.rewind(this.state$.value, commitId)
    return new Document(resetState, this.dispatcher, this.pinStore, this._validate, this._context, this.handler, true)
  }

  /**
   * Publishes Tip commit to the pub/sub
   *
   * @private
   */
  _publishTip (): void {
    this.dispatcher.publishTip(this.id, this.tip)
  }

  /**
   * Request anchor for the latest document state
   */
  anchor(): void {
    const anchorStatus$ = this._context.anchorService.requestAnchor(this.id.baseID, this.tip);
    const subscription = anchorStatus$
        .pipe(
            concatMap(async (asr) => {
              switch (asr.status) {
                case AnchorStatus.PENDING: {
                  const next = { ...this.state, anchorStatus: AnchorStatus.PENDING }
                  if (asr.anchorScheduledFor) next.anchorScheduledFor = asr.anchorScheduledFor
                  this.state$.next(next)
                  await this._updateStateIfPinned();
                  return;
                }
                case AnchorStatus.PROCESSING: {
                  this.state$.next({ ...this.state, anchorStatus: AnchorStatus.PROCESSING })
                  await this._updateStateIfPinned();
                  return;
                }
                case AnchorStatus.ANCHORED: {
                  await this._handleTip(asr.anchorRecord);
                  await this._updateStateIfPinned();
                  this._publishTip();
                  subscription.unsubscribe();
                  return;
                }
                case AnchorStatus.FAILED: {
                  if (!asr.cid.equals(this.tip)) {
                    return;
                  }
                  console.log('failed', asr.message)
                  this.state$.next({ ...this.state, anchorStatus: AnchorStatus.FAILED })
                  subscription.unsubscribe();
                  return;
                }
                default:
                  throw new UnreachableCaseError(asr, 'Unknown anchoring state')
              }
            })
        )
        .subscribe();
    this.subscriptionSet.add(subscription);
  }

  /**
   * Gets document content
   */
  get content (): any {
    const { next, content } = this.state
    return next?.content ?? content
  }

  /**
   * Gets document state
   */
  get state (): DocState {
    return this.state$.value
  }

  /**
   * Gets document doctype name
   */
  get doctype (): Doctype {
    return this._doctype
  }

  /**
   * Gets document Tip commit CID
   */
  get tip (): CID {
    return this.state.log[this.state.log.length - 1].cid
  }

  /**
   * Gets document controllers
   */
  get controllers (): string[] {
    const { next, metadata: currentMetadata } = this.state
    const metadata = cloneDeep(next?.metadata ?? currentMetadata)
    return metadata.controllers
  }

  /**
   * Waits for some time in order to propagate
   *
   * @private
   */
  async _wait(): Promise<void> {
    // add response timeout for network change
    return new Promise(resolve => {
      let tid: any // eslint-disable-line prefer-const
      const clear = async (): Promise<void> => {
        clearTimeout(tid)
        this._doctype.off('change', clear)
        await this._applyQueue.onEmpty()
        resolve()
      }
      tid = setTimeout(clear, 3000)
      this._doctype.on('change', clear)
    })
  }

  /**
   * Gracefully closes the document instance.
   */
  async close (): Promise<void> {
    this.subscriptionSet.close();
    this.off('update', this._update)

    await this.dispatcher.unregister(this.id)

    await this._applyQueue.onEmpty()

    await Utils.awaitCondition(() => this._isProcessing, () => false, 500)
  }

  /**
   * Serializes the document content
   */
  toString (): string {
    return JSON.stringify(this.state.content)
  }
}
