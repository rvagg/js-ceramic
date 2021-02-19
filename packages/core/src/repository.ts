import { Document } from './document';
import DocID from '@ceramicnetwork/docid';
import { LRUMap } from 'lru_map';

export class Repository {
  readonly #map: LRUMap<string, Document> = new LRUMap(this.limit);

  constructor(private readonly limit: number) {}

  /**
   * Stub for async loading of the document.
   */
  get(docId: DocID): Document {
    const found = this.#map.get(docId.toString());
    if (found) {
      return found;
    } else {
      throw new Error(`No document found for id ${docId}`);
    }
  }

  /**
   * Stub for adding the document.
   */
  add(document: Document): void {
    this.#map.set(document.id.toString(), document);
  }

  /**
   * The document is in memory or on disk to load.
   */
  has(docId: DocID) {
    return this.#map.has(docId.toString());
  }

  /**
   * Remove from memory. Stub.
   * @param docId
   */
  delete(docId: DocID) {
    this.#map.delete(docId.toString());
  }

  async close() {
    for (const document of this.#map.values()) {
      await document.close();
    }
  }
}
