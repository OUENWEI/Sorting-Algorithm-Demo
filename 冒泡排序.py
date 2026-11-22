import random
import time

random.seed(time.time()) #使用时间戳，避免系统缓存影响随机数生成结果
numbers = [random.randint(1, 100) for _ in range(10)] #初始化列表
print(numbers)
for i in range(len(numbers)):
    for j in range(0, len(numbers)-i-1):
        if numbers[j] > numbers[j+1]:
            numbers[j], numbers[j+1] = numbers[j+1], numbers[j]
            print(numbers)


print("升序排序后的数组是:", numbers)
