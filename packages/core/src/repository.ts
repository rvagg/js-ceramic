import { Document } from './document';
import DocID from '@ceramicnetwork/docid';
import { AsyncLruMap } from './async-lru-map';

export class Repository {
  readonly #map: AsyncLruMap<Document>;

  constructor(limit: number) {
    this.#map = new AsyncLruMap(limit, async (entry) => {
      await entry.value.close();
    });
  }

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
    await this.#map.set(document.id.toString(), document);
  }

  /**
   * Remove from memory. Stub.
   * @param docId
   */
  async delete(docId: DocID): Promise<void> {
    await this.#map.delete(docId.toString());
  }

  async close(): Promise<void> {
    for (const [, document] of this.#map) {
      await document.close();
    }
  }
}
