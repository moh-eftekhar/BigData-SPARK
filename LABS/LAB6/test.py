
x = [['B1', 'B3', 'B5'],
    ['B1', 'B3', 'B4', 'B5'],
    ['B3'],
    ['B2'],
    ['B5', 'B1', 'B3']]
def get_combinations(x):
    products = list(x)
    print("products------->>>>>>>",products)
    result = []
    for product1 in products:
        for product2 in products:
            if product1 < product2:
                result.append(((product1, product2),1))
    return result

print("Result//////------>>>>>",get_combinations(x))
# data = [
#     (('B1', 'B3'), 1),
#     (('B1', 'B5'), 1),
#     (('B3', 'B5'), 1),
#     (('B1', 'B3'), 1),
#     (('B1', 'B4'), 1),
#     (('B1', 'B5'), 1),
#     (('B3', 'B4'), 1),
#     (('B3', 'B5'), 1),
#     (('B4', 'B5'), 1),
#     (('B5', 'B1'), 1),
#     (('B5', 'B3'), 1),
#     (('B1', 'B3'), 1),
# ]
# sorted_tuple = sorted(data, key=lambda x: x[0])
# print("------->>>>>>>",sorted_tuple)  # Output: ['B1', 'B3']

# # Convert back to a tuple
# normalized_tuple = tuple(sorted_tuple)
# print("---------//////>>>>",normalized_tuple)  # Output: ('B1', 'B3')
