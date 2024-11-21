x = [1,2,3,4]
for i in range(len(x)):
    for j in range(i+1, len(x)):
        if (x[i], x[j]) != (x[0], x[1]):
            print(x[i], x[j])



.reduceByKey(lambda x, y: x + y)