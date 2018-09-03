# consistent-hash
One possible implementation of consistent hash and simulation on add/remove machine from a cluster.
# Assumption and goal
Assumption:
* In a cluster, data is partitioned into multiple columns for write and size scalability.
* Data is targeted to a column by hashing an unique identifier associated with the data.
* Columns can be backed by several rows of replicas for read scalability.

Goal:
Minimize data needs to be moved when adding or removing a column
# Algorithm
### Hashing
The hashing algorithm is usually presented as "points on a circle" 
![alt text](https://github.com/ruiyangio/consistent-hash/blob/master/images/points_on_the_circle.png "consistent hash")
1. Randomly create amount of points using an hash algorithm such as sha256 and sort them ascend. The number of points would stay constant unless the consistent hash is remade
2. Randomly and evenly assign points to the columns. By doing so, HashMap<point, column> is established
3. When data comes, make a hash key based on its identifier
   * find first point larger than the hash key and target the data to the corresponding column
   * if the hash key is larger than largest points, then target the data to the column associated with first point
   * Stamp the data with the point

### Add Machine
![alt text](https://github.com/ruiyangio/consistent-hash/blob/master/images/add_node.png "add node")
Current System:
* N points in the hash circle
* n columns

Add machine:
1. Add one column which will assigned to the new machine
2. Take N/n*(n+1) points randomly from each of the n column and assign to the new column
3. Move data associated with the moved points from original columns

### Remove Machine
Remove machine is a similar fashion
1. Randomly partition points in the column will be removed into n-1 parts.
2. Assign each parts to the remaining columns
3. Move data associated with the moved points from the original column

# Simulation
To prove the above mentioned algorithm works, a [Python implementation](https://github.com/ruiyangio/consistent-hash) is done.

Metrics:
* Item count distribution across nodes. The uniformality is validated with p-value hypothesis testing.
* Coefficient of variation of item count distribution. This is calculated using std/mean. The metric tells us dispersion of items distribution

Method:
1. Start the cluster with 5 nodes and 500K points for each node. Making a total of 2500K points for the consistent hash
2. Add 100K items. Test uniformality and report metrics
3. Scale out the cluster 5 times. Each time adding one node and 100K items
4. Randomly take one of the following operations ["add_node", "delete_node", "add_items", "get_items", "remove_items"]
5. Repeat #4 100 times

Report:

| Operations    | Uniformality test passed | Max coefficient of variation |
| ------------- | ------------------------ | ---------------------------- |
| 100           | 100                      | 1.4069%                      |
 
# Handling state of system during transition
This is complex to simulate. Here are some thoughts:
1. During removing/adding node, keep both original hash meta data and the rebalanced hash
2. Target data to column using both hash
3. After all data  is moved, remove old hash
