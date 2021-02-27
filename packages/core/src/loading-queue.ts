import { Repository } from './repository';
import { CommitID, DocID, DocRef } from '@ceramicnetwork/docid';
import { Document } from './document';
import { HandlersMap } from './handlers-map';
import { Context, DocOpts } from '@ceramicnetwork/common';
import { validateState } from './validate-state';
import { Dispatcher } from './dispatcher';
import { PinStore } from './store/pin-store';

const DEFAULT_LOAD_DOCOPTS = { anchor: false, publish: false, sync: true };

export class LoadingQueue {
  constructor(
    private readonly repository: Repository,
    private readonly dispatcher: Dispatcher,
    private readonly handlers: HandlersMap,
    private readonly context: Context,
    private readonly pinStore: PinStore,
  ) {}

  async load(id: DocID | CommitID, opts: DocOpts = {}) {
    const docId = DocRef.from(id).baseID
    if (await this.repository.has(docId)) {
      return this.repository.get(docId);
    } else {
      // Load the current version of the document
      const handler = this.handlers.get(docId.typeName);
      opts = { ...DEFAULT_LOAD_DOCOPTS, ...opts };
      const genesisCid = docId.cid;
      const commit = await this.dispatcher.retrieveCommit(genesisCid);
      if (commit == null) {
        throw new Error(`No genesis commit found with CID ${genesisCid.toString()}`);
      }
      const state = await handler.applyCommit(commit, docId.cid, this.context);
      const validate = true;
      const document = new Document(state, this.dispatcher, this.pinStore, validate, this.context, handler);

      if (validate) {
        await validateState(document.state, document.content, this.context.api);
      }

      await document._syncDocumentToCurrent(this.pinStore, opts);
      this.repository.add(document);
      return document;
    }
  }
}
