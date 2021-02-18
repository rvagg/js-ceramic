import { Document } from './document';
import DocID from '@ceramicnetwork/docid';

export class Repository {
  readonly #map: Map<string, Document> = new Map();

  get(docId: DocID) {
    const found = this.#map.get(docId.toString());
    if (found) {
      return found;
    } else {
      throw new Error(`No document found for id ${docId}`);
    }
  }

  add(document: Document) {
    this.#map.set(document.id.toString(), document);
  }

  has(docId: DocID) {
    return this.#map.has(docId.toString());
  }

  delete(docId: DocID) {
    this.#map.delete(docId.toString());
  }

  async close() {
    for (const document of this.#map.values()) {
      await document.close();
    }
  }
}
