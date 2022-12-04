from collections import deque
from typing import List

from utils import Settings


class BucketQueue(deque):
    def __init__(self, settings: Settings, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.settings = settings
        self.task = None
        self.max_bucket_deque_size = 1e6

    def set_task(self, task) -> None:
        self.task = task

    def append(self, *args, **kwargs) -> None:
        if super(BucketQueue, self).__len__() > self.max_bucket_deque_size:
            return None
        return super().append(*args, **kwargs)

    def __iter__(self):
        return self

    def __next__(self) -> List:
        if self.task is None:
            raise Exception("Task of BucketQueue is None")

        base_deque = super(BucketQueue, self)
        bucket: List = []
        processed_in_batch: int = 0
        bound: int = min(base_deque.__len__(), self.settings.batch_size)

        while processed_in_batch < bound:
            url = self.popleft()
            bucket.append(self.task(url))
            processed_in_batch += 1

        return bucket
