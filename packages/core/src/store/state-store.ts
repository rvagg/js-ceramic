import { DocState, DocStateHolder } from '@ceramicnetwork/common';
import DocID from '@ceramicnetwork/docid'

export interface StateStore {
    open(networkName: string): void;
    close(): Promise<void>;
    save(document: DocStateHolder): Promise<void>;
    load(docId: DocID): Promise<DocState | null>;
    list(docId?: DocID): Promise<string[]>;
    remove(docId: DocID): Promise<void>;
}
