import { Dispatcher } from './dispatcher'
import CID from 'cids'
import { EventEmitter } from 'events'
import PQueue from 'p-queue'
import cloneDeep from 'lodash.clonedeep'
import Utils from './utils'
import {
  AnchorProof,
  AnchorCommit,
  AnchorStatus,
  DocState,
  LogEntry,
  Doctype,
  DoctypeHandler,
  DocOpts,
  Context,
  DoctypeUtils,
  DocMetadata,
  DocStateHolder,
  UnreachableCaseError, CommitType,
} from '@ceramicnetwork/common';
import DocID, { CommitID } from '@ceramicnetwork/docid';
import { PinStore } from './store/pin-store';
import { SubscriptionSet } from "./subscription-set";
import { concatMap } from "rxjs/operators";
import { DiagnosticsLogger } from "@ceramicnetwork/logger";
import { BehaviorSubject } from 'rxjs'

// DocOpts defaults for document load operations
const DEFAULT_LOAD_DOCOPTS = {anchor: false, publish: false, sync: true}
// DocOpts defaults for document write operations
const DEFAULT_WRITE_DOCOPTS = {anchor: true, publish: true, sync: false}

type RetrieveCommitFunc = (cid: CID | string) => any

/**
 * Loads schema by ID
 *
 * @param context - Ceramic context
 * @param schemaDocId - Schema document ID
 */
async function loadSchemaById<T extends any>(context: Context, schemaDocId: string): Promise<T | null> {
  let commitId: CommitID;
  try {
    commitId = CommitID.fromString(schemaDocId);
  } catch {
    throw new Error('Commit missing when loading schema document');
  }
  const schemaDoc = await context.api.loadDocument(commitId);
  return schemaDoc.content;
}

/**
 * Loads schema for the Doctype
 */
async function loadSchema<T extends any>(context: Context, state: DocState): Promise<T | null> {
  const schemaId = state.metadata?.schema
  if (schemaId) {
    return loadSchemaById(context, schemaId)
  } else {
    return null
  }
}

/**
 * Validate Document against its schema.
 */
async function validateDocument(document: Document, context: Context) {
  const schema = await loadSchema(context, document.state)
  if (schema) {
    Utils.validate(document.content, schema)
  }
}

/**
 * Find index of the commit in the array. If the commit is signed, fetch the payload
 *
 * @param retrieveCommit - Get commit from IPFS
 * @param cid - CID value
 * @param log - Log array
 * @private
 */
async function findIndex(retrieveCommit: RetrieveCommitFunc, cid: CID, log: Array<LogEntry>): Promise<number> {
  for (let index = 0; index < log.length; index++) {
    const c = log[index].cid;
    if (c.equals(cid)) {
      return index;
    }
    const commit = await retrieveCommit(c);
    if (DoctypeUtils.isSignedCommit(commit) && commit.link.equals(cid)) {
      return index;
    }
  }
  return -1;
}

/**
 * Is CID included in the log. If the commit is signed, fetch the payload
 *
 * @param retrieveCommit - Get commit from IPFS
 * @param cid - CID value
 * @param log - Log array
 * @private
 */
async function isCidIncluded(retrieveCommit: RetrieveCommitFunc, cid: CID, log: Array<LogEntry>): Promise<boolean> {
  return findIndex(retrieveCommit, cid, log).then(index => index !== -1)
}

/**
 * Fetch log to find a connection for the given CID
 *
 * @param retrieveCommit - Get commit from IPFS
 * @param cid - Commit CID
 * @param state - Current Document State
 * @param log - Found log so far
 * @private
 */
async function fetchLog(retrieveCommit: RetrieveCommitFunc, cid: CID, state: DocState, log: Array<CID> = []): Promise<Array<CID>> {
  if (await isCidIncluded(retrieveCommit, cid, state.log)) { // already processed
    return [];
  }
  const commit = await retrieveCommit(cid);
  if (commit == null) {
    throw new Error(`No commit found for CID ${cid.toString()}`);
  }

  let payload = commit;
  if (DoctypeUtils.isSignedCommit(commit)) {
    payload = await retrieveCommit(commit.link);
    if (payload == null) {
      throw new Error(`No commit found for CID ${commit.link.toString()}`);
    }
  }
  const prevCid: CID = payload.prev;
  if (!prevCid) { // this is a fake log
    return [];
  }
  log.unshift(cid);
  if (await isCidIncluded(retrieveCommit, prevCid, state.log)) {
    // we found the connection to the canonical log
    return log;
  }
  return fetchLog(retrieveCommit, prevCid, state, log);
}

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

  private readonly retrieveCommit: RetrieveCommitFunc

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
    })

    this._logger = _context.loggerProvider.getDiagnosticsLogger()

    this._applyQueue = new PQueue({ concurrency: 1 })
    this.retrieveCommit = this.dispatcher.retrieveCommit.bind(this.dispatcher)
  }

  /**
   * Creates new Doctype with params
   * @param docId - Document ID
   * @param handler - DoctypeHandler instance
   * @param dispatcher - Dispatcher instance
   * @param pinStore - PinStore instance
   * @param context - Ceramic context
   * @param opts - Initialization options
   * @param validate - Validate content against schema
   */
  static async create (
      docId: DocID,
      handler: DoctypeHandler<Doctype>,
      dispatcher: Dispatcher,
      pinStore: PinStore,
      context: Context,
      opts: DocOpts = {},
      validate = true,
  ): Promise<Document> {
    // Fill 'opts' with default values for any missing fields
    opts = {...DEFAULT_WRITE_DOCOPTS, ...opts}
    return this.load(docId, handler, dispatcher, pinStore, context, opts, validate)
  }

  /**
   * Loads the Doctype by id
   * @param docId - Document ID
   * @param handler - find handler
   * @param dispatcher - Dispatcher instance
   * @param pinStore - PinStore instance
   * @param context - Ceramic context
   * @param opts - Initialization options
   * @param validate - Validate content against schema
   */
  static async load<T extends Doctype> (
      docId: DocID,
      handler: DoctypeHandler<T>,
      dispatcher: Dispatcher,
      pinStore: PinStore,
      context: Context,
      opts: DocOpts = {},
      validate = true): Promise<Document> {
    // Fill 'opts' with default values for any missing fields
    opts = {...DEFAULT_LOAD_DOCOPTS, ...opts}

    const genesisCid = docId.cid
    const commit = await dispatcher.retrieveCommit(genesisCid)
    if (commit == null) {
      throw new Error(`No genesis commit found with CID ${genesisCid.toString()}`)
    }
    const state = await handler.applyCommit(commit, docId.cid, context)
    const document = new Document(state, dispatcher, pinStore, validate, context, handler)

    if (validate) {
      await validateDocument(document, context)
    }

    await document._syncDocumentToCurrent(pinStore, opts)
    return document
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
   * Takes the most recent known-about version of a document and a specific commit and returns a new
   * Document instance representing the same document but set to the state of the document at the
   * requested commit.  If the requested commit is for a branch of history that conflicts with the
   * known current version of the document, throws an error. Intentionally does not register the new
   * document so that it does not get notifications about newer commits, since we want it tied to a
   * specific commit.
   * @param id - DocID of the document including the requested commit
   * @param doc - Most current version of the document that we know about
   */
  static async loadAtCommit (
      id: CommitID,
      doc: Document): Promise<Document> {

    // If 'commit' is ahead of 'doc', sync doc up to 'commit'
    await doc._handleTip(id.commit)

    // If 'commit' is not included in doc's log at this point, that means that conflict resolution
    // rejected it.
    // const commitIndex = await doc._findIndex(id.commit, doc.state.log)
    const commitIndex = await findIndex(doc.retrieveCommit, id.commit, doc.state.log) // FIXME NEXT Rewind
    if (commitIndex < 0) {
      throw new Error(`Requested commit CID ${id.commit.toString()} not found in the log for document ${id.baseID.toString()}`)
    }

    // If the requested commit is included in the log, but isn't the most recent commit, we need
    // to reset the state to the state at the requested commit.
    const resetLog = doc.state.log.slice(0, commitIndex + 1)
    const resetState = await doc._applyLogToState(resetLog.map((logEntry) => logEntry.cid))
    return new Document(resetState, doc.dispatcher, doc.pinStore, doc._validate, doc._context, doc.handler, true)
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
      this._isProcessing = true
      await this._applyQueue.add(async () => {
        const log = await fetchLog(this.retrieveCommit, cid, this.state$.value)
        if (log.length) {
          const next = await this._applyLog(log)
          if (next) {
            this.state$.next(next)
            this._doctype.emit('change') // FIXME NEXT
          }
        }
      })
    } finally {
      this._isProcessing = false
    }
  }

  /**
   * Given two different DocStates representing two different conflicting histories of the same
   * document, pick which commit to accept, in accordance with our conflict resolution strategy
   * @param state1 - first log's state
   * @param state2 - second log's state
   * @returns the DocState containing the log that is selected
   * @private
   */
  static async _pickLogToAccept(state1: DocState, state2: DocState): Promise<DocState> {
    const isState1Anchored = state1.anchorStatus === AnchorStatus.ANCHORED
    const isState2Anchored = state2.anchorStatus === AnchorStatus.ANCHORED

    if (isState1Anchored != isState2Anchored) {
      // When one of the logs is anchored but not the other, take the one that is anchored
      return isState1Anchored ? state1 : state2
    }

    if (isState1Anchored && isState2Anchored) {
      // compare anchor proofs if both states are anchored
      const { anchorProof: proof1 } = state1
      const { anchorProof: proof2 } = state2

      if (proof1.chainId != proof2.chainId) {
        // TODO: Add logic to handle conflicting updates anchored on different chains
        throw new Error("Conflicting logs on the same document are anchored on different chains. Chain1: " +
            proof1.chainId + ", chain2: " + proof2.chainId)
      }

      // Compare block heights to decide which to take
      if (proof1.blockNumber < proof2.blockNumber) {
        return state1
      } else if (proof2.blockNumber < proof1.blockNumber) {
        return state2
      }
      // If they have the same block number fall through to fallback mechanism
    }

    // The anchor states are the same for both logs. Compare log lengths and choose the one with longer length.
    if (state1.log.length > state2.log.length) {
      return state1
    } else if (state1.log.length < state2.log.length) {
      return state2
    }

    // If we got this far, that means that we don't have sufficient information to make a good
    // decision about which log to choose.  The most common way this can happen is that neither log
    // is anchored, although it can also happen if both are anchored but in the same blockNumber or
    // blockTimestamp. At this point, the decision of which log to take is arbitrary, but we want it
    // to still be deterministic. Therefore, we take the log whose last entry has the lowest CID.
    return state1.log[state1.log.length - 1].cid.bytes < state2.log[state2.log.length - 1].cid.bytes ? state1 : state2
  }

  /**
   * Applies the log to the document
   *
   * @param log - Log of commit CIDs
   * @return true if the log resulted in an update to this document's state, false if not
   * @private
   */
  async _applyLog (log: Array<CID>): Promise<DocState | null> {
    if (log[log.length - 1].equals(this.tip)) {
      // log already applied
      return null
    }
    const cid = log[0]
    const commit = await this.retrieveCommit(cid)
    let payload = commit
    if (DoctypeUtils.isSignedCommit(commit)) {
      payload = await this.retrieveCommit(commit.link)
    }
    if (payload.prev.equals(this.tip)) {
      // the new log starts where the previous one ended
      return this._applyLogToState(log, cloneDeep(this.state))
    }

    // we have a conflict since prev is in the log of the local state, but isn't the tip
    // BEGIN CONFLICT RESOLUTION
    const conflictIdx = await findIndex(this.retrieveCommit, payload.prev, this.state.log) + 1
    const canonicalLog = this.state.log.map(({cid}) => cid) // copy log
    const localLog = canonicalLog.splice(conflictIdx)
    // Compute state up till conflictIdx
    const state: DocState = await this._applyLogToState(canonicalLog)
    // Compute next transition in parallel
    const localState = await this._applyLogToState(localLog, cloneDeep(state), true)
    const remoteState = await this._applyLogToState(log, cloneDeep(state), true)

    const selectedState = await Document._pickLogToAccept(localState, remoteState)
    if (selectedState === localState) {
      return null
    }

    return this._applyLogToState(log, cloneDeep(state))
  }

  /**
   * Applies the log to the document and updates the state.
   * TODO: make this static so it's immediately obvious that this doesn't mutate the document
   *
   * @param log - Log of commit CIDs
   * @param state - Document state
   * @param breakOnAnchor - Should break apply on anchor commits?
   * @private
   */
  async _applyLogToState (log: Array<CID>, state?: DocState, breakOnAnchor?: boolean): Promise<DocState> {
    const itr = log.entries()
    let entry = itr.next()
    while(!entry.done) {
      const cid = entry.value[1]
      const commit = await this.retrieveCommit(cid)
      // TODO - should catch potential thrown error here

      let payload = commit
      if (DoctypeUtils.isSignedCommit(commit)) {
        payload = await this.retrieveCommit(commit.link)
      }

      if (payload.proof) {
        // it's an anchor commit
        await this._verifyAnchorCommit(commit)
        state = await this.handler.applyCommit(commit, cid, this._context, state)
      } else {
        // it's a signed commit
        const tmpState = await this.handler.applyCommit(commit, cid, this._context, state)
        const isGenesis = !payload.prev
        const effectiveState = isGenesis ? tmpState : tmpState.next
        if (this._validate) {
          const schemaId = effectiveState.metadata.schema
          if (schemaId) {
            const schema = await loadSchemaById(this._context, schemaId)
            if (schema) {
              Utils.validate(effectiveState.content, schema)
            }
          }
        }
        state = tmpState // if validation is successful
      }

      if (breakOnAnchor && AnchorStatus.ANCHORED === state.anchorStatus) {
        return state
      }
      entry = itr.next()
    }
    return state
  }

  /**
   * Verifies anchor commit structure
   *
   * @param commit - Anchor commit
   * @private
   */
  async _verifyAnchorCommit (commit: AnchorCommit): Promise<AnchorProof> {
    const proofCID = commit.proof
    const proof =  await this.dispatcher.retrieveFromIPFS(proofCID)

    let prevCIDViaMerkleTree
    try {
      // optimize verification by using ipfs.dag.tree for fetching the nested CID
      if (commit.path.length === 0) {
        prevCIDViaMerkleTree = proof.root
      } else {
        const merkleTreeParentRecordPath = '/root/' + commit.path.substr(0, commit.path.lastIndexOf('/'))
        const last: string = commit.path.substr(commit.path.lastIndexOf('/') + 1)

        const merkleTreeParentRecord = await this.dispatcher.retrieveFromIPFS(proofCID, merkleTreeParentRecordPath)
        prevCIDViaMerkleTree = merkleTreeParentRecord[last]
      }
    } catch (e) {
      throw new Error(`The anchor commit couldn't be verified. Reason ${e.message}`)
    }

    if (commit.prev.toString() !== prevCIDViaMerkleTree.toString()) {
      throw new Error(`The anchor commit proof ${commit.proof.toString()} with path ${commit.path} points to invalid 'prev' commit`)
    }

    await this._context.anchorService.validateChainInclusion(proof)
    return proof
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
    return this.metadata.controllers
  }

  /**
   * Gets document metadata
   */
  get metadata (): DocMetadata {
    const { next, metadata } = this.state
    return cloneDeep(next?.metadata ?? metadata)
  }

  get commitId(): CommitID {
    return this.id.atCommit(this.tip)
  }

  get allCommitIds(): Array<CommitID> {
    return this.state.log.map(({ cid }) => this.id.atCommit(cid))
  }

  get anchorCommitIds(): Array<CommitID> {
    return this.state.log
      .filter(({ type }) => type === CommitType.ANCHOR)
      .map(({ cid }) => this.id.atCommit(cid))
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

    this.dispatcher.unregister(this.id)

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
