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
import { pickLogToAccept } from './document';

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
