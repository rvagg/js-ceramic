import { TaskQueue } from '../task-queue';

async function delay(ms: number) {
  return new Promise<void>((resolve) => {
    setTimeout(() => resolve(), ms);
  });
}

test('add synchronously', async () => {
  const tasks = new TaskQueue();
  let n = 0;
  tasks.add(async () => {
    await delay(100);
    n++;
  });
  await tasks.onIdle();
  expect(n).toEqual(1);
});

test('common error handler', async () => {
  const onError = jest.fn();
  const tasks = new TaskQueue(onError);
  const error = new Error(`Horror #${Math.random()}`);
  const accumulator = [];
  tasks.add(async () => {
    throw error;
  });
  tasks.add(async () => {
    await delay(300);
    accumulator.push(1);
  });
  tasks.add(async () => {
    await delay(200);
    throw error;
  });
  await tasks.onIdle();
  expect(onError).toBeCalledTimes(2);
  expect(onError).toBeCalledWith(error);
});

test('sequential execution', async () => {
  const tasks = new TaskQueue();
  const accumulator = [];
  tasks.add(async () => {
    await delay(300);
    accumulator.push(1);
  });
  tasks.add(async () => {
    await delay(200);
    accumulator.push(2);
  });
  tasks.add(async () => {
    accumulator.push(3);
  });
  await tasks.onIdle();
  expect(accumulator).toEqual([1, 2, 3]);
});

test('clear', async () => {
  const tasks = new TaskQueue();
  const accumulator = [];
  tasks.add(async () => {
    await delay(300);
    accumulator.push(1);
  });
  tasks.add(async () => {
    await delay(200);
    accumulator.push(2);
  });
  tasks.add(async () => {
    accumulator.push(3);
  });
  await delay(200);
  tasks.clear();
  await tasks.onIdle();
  expect(accumulator).toEqual([1]);
});
