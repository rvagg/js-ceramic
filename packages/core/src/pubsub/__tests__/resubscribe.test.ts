import { IpfsApi, LoggerProvider } from '@ceramicnetwork/common';
import { createIPFS } from '../../__tests__/ipfs-util';
import { IPFSPubsubMessage, Resubscribe } from '../resubscribe';
import { TaskQueue } from '../task-queue';
import { delay } from './delay';
import { asIpfsMessage } from './as-ipfs-message';
import { MsgType } from '../pubsub-message';
import { DocID } from '@ceramicnetwork/docid';
import { Subject } from 'rxjs';

const FAKE_DOC_ID = DocID.fromString('kjzl6cwe1jw147dvq16zluojmraqvwdmbh61dx9e0c59i344lcrsgqfohexp60s');

let ipfs: IpfsApi;

beforeEach(async () => {
  ipfs = await createIPFS();
});

afterEach(async () => {
  await ipfs.stop();
});

const TOPIC = '/test';
const loggerProvider = new LoggerProvider();
const pubsubLogger = loggerProvider.makeServiceLogger('pubsub');
const diagnosticsLogger = loggerProvider.getDiagnosticsLogger();
const PEER_ID = 'FAKE_PEER_ID';

test('subscribe and unsubscribe', async () => {
  const taskQueue = new TaskQueue();
  const incoming = new Resubscribe(ipfs, TOPIC, 30000, PEER_ID, pubsubLogger, diagnosticsLogger, taskQueue);
  const subscribeSpy = jest.spyOn(ipfs.pubsub, 'subscribe');
  const unsubscribeSpy = jest.spyOn(ipfs.pubsub, 'unsubscribe');
  const subscription = incoming.subscribe();
  await taskQueue.onIdle();
  expect(await ipfs.pubsub.ls()).toEqual([TOPIC]);
  subscription.unsubscribe();
  await taskQueue.onIdle();
  expect(await ipfs.pubsub.ls()).toEqual([]);
  expect(subscribeSpy).toBeCalledTimes(1);
  expect(unsubscribeSpy).toBeCalledTimes(2); // 1 on initial subscription attempt, +1 on final unsubscribe
});

test('resubscribe', async () => {
  const taskQueue = new TaskQueue();
  const resubscribePeriod = 200;
  const incoming$ = new Resubscribe(
    ipfs,
    TOPIC,
    resubscribePeriod,
    PEER_ID,
    pubsubLogger,
    diagnosticsLogger,
    taskQueue,
  );
  const subscribeSpy = jest.spyOn(ipfs.pubsub, 'subscribe');
  const unsubscribeSpy = jest.spyOn(ipfs.pubsub, 'unsubscribe');
  const subscription = incoming$.subscribe();
  await taskQueue.onIdle();
  expect(subscribeSpy).toBeCalledTimes(1); // Initial pubsub.subscribe
  expect(unsubscribeSpy).toBeCalledTimes(1); // Resubscribe attempt: _unsubscribe_ then subscribe
  await ipfs.pubsub.unsubscribe(TOPIC);
  await delay(resubscribePeriod * 3);
  expect(unsubscribeSpy).toBeCalledTimes(3); // +1 on resubscribe, +1 on force-unsubscribe few lines above
  expect(subscribeSpy).toBeCalledTimes(2); // +1 on resubscribe
  expect(await ipfs.pubsub.ls()).toEqual([TOPIC]); // And now we subscribed
  subscription.unsubscribe();
});

test('pass incoming message', async () => {
  const taskQueue = new TaskQueue();
  const length = 10;
  const messages = Array.from({ length }).map((_, index) => {
    return asIpfsMessage({
      typ: MsgType.QUERY,
      id: index.toString(),
      doc: FAKE_DOC_ID,
    });
  });
  const feed$ = new Subject<IPFSPubsubMessage>();
  const ipfsA = {
    pubsub: {
      subscribe: async (_, handler) => {
        feed$.subscribe(handler);
      },
      unsubscribe: jest.fn(),
      ls: jest.fn(() => []),
    },
  };
  const incoming$ = new Resubscribe(ipfsA, TOPIC, 30000, PEER_ID, pubsubLogger, diagnosticsLogger, taskQueue);
  const result: any[] = [];
  const subscription = incoming$.subscribe((message) => {
    result.push(message);
  });
  await taskQueue.onIdle(); // Wait till fully subscribed
  messages.forEach((m) => feed$.next(m));
  expect(result).toEqual(messages);
  feed$.complete();
  subscription.unsubscribe();
});
