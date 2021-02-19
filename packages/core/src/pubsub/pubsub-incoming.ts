import { Observable } from 'rxjs';
import { IpfsApi } from '@ceramicnetwork/common';
import { TaskQueue } from './task-queue';

// Typestub for pubsub message.
// At some future time this type definition should be provided by IPFS.
export type PubsubMessage = {
  from: string;
  seqno: Uint8Array;
  data: Uint8Array;
  topicIDs: string[];
};

async function resubscribe(ipfs: IpfsApi, topic: string, handler: (message: PubsubMessage) => void) {
  const listeningTopics = await ipfs.pubsub.ls();
  const isSubscribed = listeningTopics.includes(topic);
  if (!isSubscribed) {
    await ipfs.pubsub.unsubscribe(topic, handler);
    await ipfs.pubsub.subscribe(topic, handler);
  }
}

/**
 * Incoming IPFS PubSub message stream as Observable.
 * Ensures that ipfs subscription to a topic is established by periodically re-subscribing.
 */
export class PubsubIncoming extends Observable<PubsubMessage> {
  constructor(ipfs: IpfsApi, topic: string, resubscribeEvery: number, taskQueue?: TaskQueue) {
    super((subscriber) => {
      // We want all the tasks added to execute in FIFO order.
      // Subscription attempts must be sequential.
      // Last call to unsubscribe must execute after all the attempts are done,
      // and all the attempts yet inactive are cleared.
      const tasks =
        taskQueue ||
        new TaskQueue((error) => {
          console.error(`Having trouble subscribing to ${topic}:`, error.message); // FIXME Replace with proper logging
        });

      const handler = (message: PubsubMessage) => {
        subscriber.next(message);
      };

      tasks.add(() => ipfs.pubsub.subscribe(topic, handler));

      const ensureSubscribed = setInterval(async () => {
        tasks.add(() => resubscribe(ipfs, topic, handler));
      }, resubscribeEvery);

      return () => {
        clearInterval(ensureSubscribed);
        tasks.clear();
        // Unsubscribe only after a currently running task is finished.
        tasks.add(() => {
          return ipfs.pubsub.unsubscribe(topic, handler);
        });
      };
    });
  }
}
