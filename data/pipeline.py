# 4b Pipeline Dependancy

# the question:
# - make directed unweighted graph from relation.txt (edges) and task_ids.txt (vertices)
# - given a starting task (vertex), use a shortest path algo to find goal task
# - return list of vertices to reach goal task
from collections import defaultdict

def addEdge(graph, u, v):
    graph[u].append(v)


with open('relation.txt') as f:
    edges = [tuple(map(int, i.split('->'))) for i in f]

with open('task_ids.txt') as f:
    vertices = f.read().split(',')
    j = 0
    for i in vertices:
        vertices[j] = int(i)
        j += 1


# using defaultdict to declare a dictionary of lists 
tasks = defaultdict(list)

for i in edges:
    addEdge(tasks, i[0], i[1])



# modified DFS for topological sort

def topologicalHelper(graph, numV, visited, sorted):
    visited[numV] = True

    for i in graph[numV]:
        i -= 1
        if visited[i] == False:
            topologicalHelper(graph, i, visited, sorted)
    sorted.insert(0, numV+1)

def topologicalSort(graph, vertices):
    visited = [False] * max(vertices)
    sorted = []
    for i in vertices:
        i -= 1
        if visited[i] == False:
            topologicalHelper(graph, i, visited, sorted)

    return sorted

print(topologicalSort(tasks, vertices))
# def shortestPath(graph, src, goal):
# solution requires a modification of bellman-ford's algorithm, 
# inputting a sub list from the initial to the goal task
# and building the shortest path to there.

# Can also be done with a modified DFS to end when reaching the goal task
# and printing the path to the node.
