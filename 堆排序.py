import random
import time

random.seed(time.time())
numbers = [random.randint(1, 100) for _ in range(10)]
print(numbers)

def sift_down(size, i):
    while True:
        largest = i
        left = 2 * i + 1
        right = 2 * i + 2
        if left < size and numbers[left] > numbers[largest]:
            largest = left
        if right < size and numbers[right] > numbers[largest]:
            largest = right
        if largest == i:
            return
        numbers[i], numbers[largest] = numbers[largest], numbers[i]
        i = largest
        print("堆大小:", size, "当前结点:", i, numbers)

n = len(numbers)
for i in range(n // 2 - 1, -1, -1):
    sift_down(n, i)
for end in range(n - 1, 0, -1):
    numbers[0], numbers[end] = numbers[end], numbers[0]
    print("堆大小:", end, "当前结点:", 0, numbers)
    sift_down(end, 0)

print("升序排序后的数组是:", numbers)
