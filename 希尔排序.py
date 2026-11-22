import random
import time

random.seed(time.time())
numbers = [random.randint(1, 100) for _ in range(10)]
print(numbers)

gap = len(numbers) // 2
while gap > 0:
    for i in range(gap, len(numbers)):
        j = i
        while j >= gap and numbers[j - gap] > numbers[j]:
            numbers[j], numbers[j - gap] = numbers[j - gap], numbers[j]
            print("间隔:", gap, numbers)
            j -= gap
    gap //= 2

print("升序排序后的数组是:", numbers)
