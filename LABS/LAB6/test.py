# def new_func():
#     x = [1,2,3,4]
#     for i in range(len(x)):
#         for j in range(i+1, len(x)):
#                 print(x[i], x[j])
data = [
    (('B1', 'B3'), 1),
    (('B1', 'B5'), 1),
    (('B3', 'B5'), 1),
    (('B1', 'B3'), 1),
    (('B1', 'B4'), 1),
    (('B1', 'B5'), 1),
    (('B3', 'B4'), 1),
    (('B3', 'B5'), 1),
    (('B4', 'B5'), 1),
    (('B5', 'B1'), 1),
    (('B5', 'B3'), 1),
    (('B1', 'B3'), 1),
]
sorted_tuple = sorted(data, key=lambda x: x[0])
print("------->>>>>>>",sorted_tuple)  # Output: ['B1', 'B3']

# Convert back to a tuple
normalized_tuple = tuple(sorted_tuple)
print("---------//////>>>>",normalized_tuple)  # Output: ('B1', 'B3')
