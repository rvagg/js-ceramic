import { DocRef } from '../doc-ref';
import { DocID } from '../doc-id';
import { CommitID } from '../commit-id';

describe('.build', () => {
  const DOC_ID_STRING = 'kjzl6cwe1jw147dvq16zluojmraqvwdmbh61dx9e0c59i344lcrsgqfohexp60s';
  const docId = DocID.fromString(DOC_ID_STRING);
  const DOC_ID_WITH_COMMIT =
    'k1dpgaqe3i64kjqcp801r3sn7ysi5i0k7nxvs7j351s7kewfzr3l7mdxnj7szwo4kr9mn2qki5nnj0cv836ythy1t1gya9s25cn1nexst3jxi5o3h6qprfyju';
  const commitId = CommitID.fromString(DOC_ID_WITH_COMMIT);

  test('DocID', () => {
    const result = DocRef.from(docId);
    expect(result).toBe(docId);
  });
  test('valid DocID string', () => {
    const result = DocRef.from(DOC_ID_STRING);
    expect(result).toBeInstanceOf(DocID);
    expect(result).toEqual(docId);
  });
  test('valid DocID bytes', () => {
    const result = DocRef.from(docId.bytes);
    expect(result).toBeInstanceOf(DocID);
    expect(result).toEqual(docId);
  });
  test('CommitID', () => {
    const result = DocRef.from(commitId);
    expect(result).toBeInstanceOf(CommitID);
    expect(result).toEqual(commitId);
  });
  test('valid CommitID string', () => {
    const result = DocRef.from(DOC_ID_WITH_COMMIT);
    expect(result).toBeInstanceOf(CommitID);
    expect(result).toEqual(commitId);
  });
  test('valid CommitID bytes', () => {
    const result = DocRef.from(commitId.bytes);
    expect(result).toBeInstanceOf(CommitID);
    expect(result).toEqual(commitId);
  });
  test('invalid string', () => {
    expect(() => DocRef.from('garbage')).toThrow();
  });
  test('invalid bytes', () => {
    expect(() => DocRef.from(new Uint8Array([1, 2, 3]))).toThrow();
  });
});
