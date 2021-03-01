import { Document } from './document';
import DocID from '@ceramicnetwork/docid';

export class Repository {
  readonly #map: Map<string, Document> = new Map();

  /**
   * Stub for async loading of the document.
   */
  async get(docId: DocID): Promise<Document> {
    return this.#map.get(docId.toString());
  }

  /**
   * Stub for adding the document.
   */
  async add(document: Document): Promise<void> {
    this.#map.set(document.id.toString(), document);
  }

  /**
   * Remove from memory. Stub.
   * @param docId
   */
  delete(docId: DocID) {
    this.#map.delete(docId.toString());
  }

  async close(): Promise<void> {
    for (const document of this.#map.values()) {
      await document.close();
    }
  }
}
