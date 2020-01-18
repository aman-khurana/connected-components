### Efficient python code using Apache Spark to find connected components in a graph.

This code was written as part of the requirement for CSE 570 Introduction to Parallel and Distributed Programming at State University of New York at Buffalo. In this code Map Reduce paradigm is used to solve the problem of finding connected components in massive graphs with the capability to solve the problem at scale.

**Input:** 

List of edges

**Output**: 

Connected Components of the graph in the form of list of edges, with all the nodes attached to the parent node(can also be interpreted as the id of the connected component ).

This code was tested and benchmarked on UB's HPC center, [Center for Computational Research.](http://www.buffalo.edu/ccr.html)

**References**

- <https://research.google/pubs/pub43122/>
- <http://mmds-data.org/presentations/2014/vassilvitskii_mmds14.pdf>