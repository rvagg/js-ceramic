import Utils from './utils';
import { CommitID } from '@ceramicnetwork/docid';
import { CeramicApi, DocNext, DocState } from '@ceramicnetwork/common';

/**
 * Loads schema by ID
 *
 * @param ceramic - Ceramic API to load document
 * @param schemaDocId - Schema document ID
 */
async function loadSchemaById<T extends any>(ceramic: CeramicApi, schemaDocId: string): Promise<T | null> {
  let commitId: CommitID;
  try {
    commitId = CommitID.fromString(schemaDocId);
  } catch {
    throw new Error('Commit missing when loading schema document');
  }
  const schemaDoc = await ceramic.loadDocument(commitId);
  return schemaDoc.content;
}

/**
 * Loads schema for the Doctype
 */
async function loadSchema<T extends any>(ceramic: CeramicApi, state: DocState | DocNext): Promise<T | null> {
  const schemaId = state.metadata?.schema
  if (schemaId) {
    return loadSchemaById(ceramic, schemaId)
  } else {
    return null
  }
}

/**
 * Validate document state against its schema.
 */
export async function validateState(state: DocState | DocNext, content: any, ceramic: CeramicApi): Promise<void> {
  const schema = await loadSchema(ceramic, state)
  if (schema) {
    Utils.validate(content, schema)
  }
}
