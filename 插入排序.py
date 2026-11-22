import random
import time

random.seed(time.time()) #使用时间戳，避免系统缓存影响随机数生成结果
numbers = [random.randint(1, 100) for _ in range(10)]
print(numbers)
for i in range(1, len(numbers)):
    key = numbers[i]
    print(key)
    j = i - 1
    while j >= 0 and numbers[j] > key:
        numbers[j + 1] = numbers[j]
        j -= 1
    numbers[j + 1] = key
    print(numbers)
print(numbers)
   
    
   