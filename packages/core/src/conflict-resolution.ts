import CID from 'cids';
import {
  AnchorCommit,
  AnchorProof,
  AnchorService,
  AnchorStatus,
  Context,
  DocState,
  Doctype,
  DoctypeHandler,
  DoctypeUtils,
  LogEntry,
} from '@ceramicnetwork/common';
import { validateState } from './validate-state';
import { Dispatcher } from './dispatcher';
import cloneDeep from 'lodash.clonedeep';
import { findIndex } from './document';

type RetrieveCommitFunc = (cid: CID | string, path?: string) => any;

/**
 * Verifies anchor commit structure
 *
 * @param dispatcher - To get raw blob from IPFS
 * @param anchorService - AnchorService to verify chain inclusion
 * @param commit - Anchor commit
 */
async function verifyAnchorCommit(
  dispatcher: Dispatcher,
  anchorService: AnchorService,
  commit: AnchorCommit,
): Promise<AnchorProof> {
  const proofCID = commit.proof;
  const proof = await dispatcher.retrieveFromIPFS(proofCID);

  let prevCIDViaMerkleTree;
  try {
    // optimize verification by using ipfs.dag.tree for fetching the nested CID
    if (commit.path.length === 0) {
      prevCIDViaMerkleTree = proof.root;
    } else {
      const merkleTreeParentRecordPath = '/root/' + commit.path.substr(0, commit.path.lastIndexOf('/'));
      const last: string = commit.path.substr(commit.path.lastIndexOf('/') + 1);

      const merkleTreeParentRecord = await dispatcher.retrieveFromIPFS(proofCID, merkleTreeParentRecordPath);
      prevCIDViaMerkleTree = merkleTreeParentRecord[last];
    }
  } catch (e) {
    throw new Error(`The anchor commit couldn't be verified. Reason ${e.message}`);
  }

  if (commit.prev.toString() !== prevCIDViaMerkleTree.toString()) {
    throw new Error(
      `The anchor commit proof ${commit.proof.toString()} with path ${commit.path} points to invalid 'prev' commit`,
    );
  }

  await anchorService.validateChainInclusion(proof);
  return proof;
}

/**
 * Given two different DocStates representing two different conflicting histories of the same
 * document, pick which commit to accept, in accordance with our conflict resolution strategy
 * @param state1 - first log's state
 * @param state2 - second log's state
 * @returns the DocState containing the log that is selected
 */
export async function pickLogToAccept(state1: DocState, state2: DocState): Promise<DocState> {
  const isState1Anchored = state1.anchorStatus === AnchorStatus.ANCHORED;
  const isState2Anchored = state2.anchorStatus === AnchorStatus.ANCHORED;

  if (isState1Anchored != isState2Anchored) {
    // When one of the logs is anchored but not the other, take the one that is anchored
    return isState1Anchored ? state1 : state2;
  }

  if (isState1Anchored && isState2Anchored) {
    // compare anchor proofs if both states are anchored
    const { anchorProof: proof1 } = state1;
    const { anchorProof: proof2 } = state2;

    if (proof1.chainId != proof2.chainId) {
      // TODO: Add logic to handle conflicting updates anchored on different chains
      throw new Error(
        'Conflicting logs on the same document are anchored on different chains. Chain1: ' +
          proof1.chainId +
          ', chain2: ' +
          proof2.chainId,
      );
    }

    // Compare block heights to decide which to take
    if (proof1.blockNumber < proof2.blockNumber) {
      return state1;
    } else if (proof2.blockNumber < proof1.blockNumber) {
      return state2;
    }
    // If they have the same block number fall through to fallback mechanism
  }

  // The anchor states are the same for both logs. Compare log lengths and choose the one with longer length.
  if (state1.log.length > state2.log.length) {
    return state1;
  } else if (state1.log.length < state2.log.length) {
    return state2;
  }

  // If we got this far, that means that we don't have sufficient information to make a good
  // decision about which log to choose.  The most common way this can happen is that neither log
  // is anchored, although it can also happen if both are anchored but in the same blockNumber or
  // blockTimestamp. At this point, the decision of which log to take is arbitrary, but we want it
  // to still be deterministic. Therefore, we take the log whose last entry has the lowest CID.
  return state1.log[state1.log.length - 1].cid.bytes < state2.log[state2.log.length - 1].cid.bytes ? state1 : state2;
}

export class HistoryLog {
  constructor(private readonly dispatcher: Dispatcher, private readonly entries: LogEntry[]) {}

  /**
   * Find index of the commit in the array. If the commit is signed, fetch the payload
   *
   * @param cid - CID value
   * @private
   */
  async findIndex(cid: CID): Promise<number> {
    for (let index = 0; index < this.entries.length; index++) {
      const current = this.entries[index].cid;
      if (current.equals(cid)) {
        return index;
      }
      const commit = await this.dispatcher.retrieveCommit(current);
      if (DoctypeUtils.isSignedCommit(commit) && commit.link.equals(cid)) {
        return index;
      }
    }
    return -1;
  }

  /**
   * Return CIDs of the log entries
   */
  get cids(): CID[] {
    return this.entries.map((_) => _.cid);
  }
}

/**
 * Is CID included in the log. If the commit is signed, fetch the payload
 *
 * @param retrieveCommit - Get commit from IPFS
 * @param cid - CID value
 * @param log - Log array
 * @private
 */
export async function isCidIncluded(retrieveCommit: RetrieveCommitFunc, cid: CID, log: Array<LogEntry>): Promise<boolean> {
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
export async function fetchLog(retrieveCommit: RetrieveCommitFunc, cid: CID, state: DocState, log: Array<CID> = []): Promise<Array<CID>> {
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

export class ConflictResolution {
  constructor(
    private readonly context: Context,
    private readonly dispatcher: Dispatcher,
    private readonly handler: DoctypeHandler<Doctype>,
    private readonly isValidationEnabled: boolean,
  ) {}

  /**
   * Applies the log to the document and updates the state.
   */
  async applyLogToState(log: Array<CID>, state?: DocState, breakOnAnchor?: boolean): Promise<DocState> {
    const itr = log.entries();
    let entry = itr.next();
    while (!entry.done) {
      const cid = entry.value[1];
      const commit = await this.dispatcher.retrieveCommit(cid);
      // TODO - should catch potential thrown error here

      let payload = commit;
      if (DoctypeUtils.isSignedCommit(commit)) {
        payload = await this.dispatcher.retrieveCommit(commit.link);
      }

      if (payload.proof) {
        // it's an anchor commit
        await verifyAnchorCommit(this.dispatcher, this.context.anchorService, commit);
        state = await this.handler.applyCommit(commit, cid, this.context, state);
      } else {
        // it's a signed commit
        const tmpState = await this.handler.applyCommit(commit, cid, this.context, state);
        const isGenesis = !payload.prev;
        const effectiveState = isGenesis ? tmpState : tmpState.next;
        if (this.isValidationEnabled) {
          await validateState(effectiveState, effectiveState.content, this.context.api);
        }
        state = tmpState; // if validation is successful
      }

      if (breakOnAnchor && AnchorStatus.ANCHORED === state.anchorStatus) {
        return state;
      }
      entry = itr.next();
    }
    return state;
  }

  /**
   * Applies the log to the document
   *
   * @param initialState
   * @param log - Log of commit CIDs
   * @return true if the log resulted in an update to this document's state, false if not
   * @private
   */
  async applyLog(initialState: DocState, log: Array<CID>): Promise<DocState | null> {
    const tip = initialState.log[initialState.log.length - 1].cid;
    if (log[log.length - 1].equals(tip)) {
      // log already applied
      return null;
    }
    const cid = log[0];
    const commit = await this.dispatcher.retrieveCommit(cid);
    let payload = commit;
    if (DoctypeUtils.isSignedCommit(commit)) {
      payload = await this.dispatcher.retrieveCommit(commit.link);
    }
    if (payload.prev.equals(tip)) {
      // the new log starts where the previous one ended
      return this.applyLogToState(log, cloneDeep(initialState));
    }

    // we have a conflict since prev is in the log of the local state, but isn't the tip
    // BEGIN CONFLICT RESOLUTION
    const conflictIdx = (await this.findIndex(payload.prev, initialState.log)) + 1; // FIXME NOW
    const canonicalLog = initialState.log.map(({ cid }) => cid); // copy log
    const localLog = canonicalLog.splice(conflictIdx);
    // Compute state up till conflictIdx
    const state: DocState = await this.applyLogToState(canonicalLog);
    // Compute next transition in parallel
    const localState = await this.applyLogToState(localLog, cloneDeep(state), true);
    const remoteState = await this.applyLogToState(log, cloneDeep(state), true);

    const selectedState = await pickLogToAccept(localState, remoteState);
    if (selectedState === localState) {
      return null;
    }

    return this.applyLogToState(log, cloneDeep(state));
  }

  async applyTip(initialState: DocState, tip: CID): Promise<DocState | null> {
    const log = await fetchLog(this.dispatcher.retrieveCommit.bind(this.dispatcher), tip, initialState);
    if (log.length) {
      return this.applyLog(initialState, log);
    }
  }

  /**
   * Find index of the commit in the array. If the commit is signed, fetch the payload
   *
   * @param cid - CID value
   * @param log - Log array
   * @private
   */
  async findIndex(cid: CID, log: Array<LogEntry>): Promise<number> {
    for (let index = 0; index < log.length; index++) {
      const c = log[index].cid;
      if (c.equals(cid)) {
        return index;
      }
      const commit = await this.dispatcher.retrieveCommit(c);
      if (DoctypeUtils.isSignedCommit(commit) && commit.link.equals(cid)) {
        return index;
      }
    }
    return -1;
  }
}
