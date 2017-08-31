# Detecting-Tight-Communities-In-Facebook

The identification of tight communities in a network is very important to understand the structure of the Network. This is helpful in many areas like Medical science, Defense teams. In Medical Science this analysis can be used for detecting Cancer causing genes, in Defense for terrorist community detection. We can find the like-mindedness among the people in Social Networks.

### Dataset Statistics
Dataset Link: http://snap.stanford.edu/data/egonets-Facebook.html  
The following table describes the SNAP Facebook dataset as in the Dataset description:  
![dataset](https://user-images.githubusercontent.com/31011479/29914759-8e2f9ea2-8dee-11e7-8aa9-5c696682f9d4.png)  

The Facebook dataset resembles an Undirected Graph. This is because two Users in Facebook are mutually connected if they are friends unlike in Twitter or Google+ where a User follows another User which signifies a directed graph. There is no difference between weakly and strongly connected components in an undirected graph. This is the reason the sub graph of largest WCC and SCC shows same number of nodes in the above table. The Average clustering coefficient of the whole signifies the overall tightness of the network. As all nodes in the SCC are equal to the total number of nodes in the graph, there is only one connected component in the given data. Since it is an undirected graph, SCC and WCC are same. So, detecting strongly connected components in the graph is same as detecting connected components in this graph. Also, since we have only one connected component, we are increasing our project’s scope to finding cliques in the graph. A completely connected sub-graph in the given network is called a clique.  
The Clique (Completely connected sub-component) is different from a strongly connected sub-component in the following way. In a completely connected sub-component, there is an edge between all the distinct nodes in the sub-component whereas in strongly connected sub-component, a path between any two distinct nodes in the sub-component exists.

### Project Approach
* All the analysis requires constructing and analyzing a network which can be modeled as a Graph. Apache Spark has provided an API for graphs and parallel computation in graphs which is GraphX. GraphX API is used in this project for constructing the graph and for analyzing the graph. In addition to this, JGrapht API is used. JGrapht has additional functionalities for graph processing. Here we use JGrapht for finding the cliques across the graph.

1. First step is to build a graph using the Facebook dataset with Users as the vertices and the relationship among them as edges.
2. The second step is finding the strongly connected components and cliques (completely connected sub-graph) in the network-graph. To find this, we are using Bron-Kerbosch algorithm which is in Jgrapht.
3. Now we find the clustering coefficient of all vertices in the graph using the triangle count.
4. Now tightness of each clique is determined using the clustering coefficient.
5. The last step is determining the tightness for each clique in the overall network and ordering the cliques based on the tightness.

### Execution & Explanation
The source code is present in DetectingTightCommunities.scala  
Databricks software is used to complete the project.  

**Execution instructions:**  

1. The input is read from 10 different files. The path is given in code with lines like

```
data = sc.textFile("../Assignment/facebook.tar/facebook/facebook/107.edges");
```

Please keep the files in required paths or change the path.

2. Intermediate output of all the cliques in the graph is generated to the file: **sample1.txt**

3. The final output of the cliques in the graph in the decreasing order of the avg. co-efficient of tightness is generated to file : **result.txt**

### References
1. http://spark.apache.org/graphx/
2. http://jgrapht.org/javadoc/org/jgrapht/alg/BronKerboschCliqueFinder.html
3. http://stackoverflow.com/questions/31217642/finding-cliques-or-strongly-connected-components-in-apache-spark-using-graphx
4. https://en.wikipedia.org/wiki/Bron%E2%80%93Kerbosch_algorithm
