import random
import time

def quick_sort(arr):
    if len(arr) <= 1:
        return arr
    pivot = arr[random.randint(0,len(arr)-1)]
    left = [] 
    equal = []  
    right = [] 
    for i in arr:
        if i < pivot:
            left.append(i)
        elif i > pivot:
            right.append(i)
        else:
            equal.append(i)
    print("左数组:", left, "基准元素:", pivot, "等于基准:", equal, "右数组:", right)

    return quick_sort(left) + equal + quick_sort(right) #快排需要使用递归，故使用函数封装
random.seed(time.time()) #使用时间戳，避免系统缓存影响随机数生成结果
numbers = [random.randint(0,100) for _ in range(0,10)]
result = quick_sort(numbers)
print("升序排序后的数组是:", result)
