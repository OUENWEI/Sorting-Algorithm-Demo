import random
import time

random.seed(time.time())
numbers = [random.randint(1, 100) for _ in range(10)]
print(numbers)

K = 5
buckets = [[] for _ in range(K)]
for value in numbers:
    buckets[(value - 1) * K // 100].append(value)
    flat = [x for bucket in buckets for x in bucket]
    print("桶数:", K, "分桶:", flat)

for bucket in buckets:
    for i in range(1, len(bucket)):
        key = bucket[i]
        j = i - 1
        while j >= 0 and bucket[j] > key:
            bucket[j + 1] = bucket[j]
            j -= 1
        bucket[j + 1] = key
        flat = [x for b in buckets for x in b]
        print("桶数:", K, "桶内排序:", flat)

numbers = [x for bucket in buckets for x in bucket]
print("升序排序后的数组是:", numbers)
