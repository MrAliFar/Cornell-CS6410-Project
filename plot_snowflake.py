from matplotlib import pyplot  as plt

f = open("round_queries.txt", "r")

data = f.read()

data_lines = data.split("\n")

for line in data_lines[0:len(data_lines) - 1]:
    line_data = []
    vals = line.split(" ")
    for val in vals:
        line_data.append(int(val))
    plt.plot(vals, "-o")
    print(line)
plt.show()

#print(data)