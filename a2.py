#### AMAN
#### KHURANA
#### AMANKHUR



from pyspark import SparkContext
import sys, os

### function performs the large star operation
def large_star(rdd):
    

    rdd = rdd.flatMap(lambda x: [(x[0],x[1]), (x[1],x[0])])
    
    ### getting adjacency list for all the vertices
    rdd = rdd.groupByKey().mapValues(list)

    def reduce_large_star(x):
        
        u = x[0]       ## vertex 
        L = x[1]       ## list of vertices which have edge with u
        L = L + [u]
        m = min(L)      
        out_list = []
        for v in L:
            if u < v:
                out_list.append((v,m))
        return out_list

    ### for each vertex and its adjacency list, performing the reduce_large_star function
    rdd = rdd.flatMap(reduce_large_star)

    return rdd
 
def small_star(rdd):
    
    ### returning (u,v) with v <= u
    rdd = rdd.map(lambda x: (x[1],x[0]) if x[0] <= x[1] else x )
    
    ### getting adjacency list for all the vertices
    rdd = rdd.groupByKey().mapValues(list)

    def reduce_small_star(x):
            
            u = x[0]     ## vertex 
            L = x[1]     ## list of vertices which have edge with u
            L = L + [u]
            m = min(L)
            out_list = []
            for v in L:
                out_list.append((v,m))
            return out_list



    rdd = rdd.flatMap(reduce_small_star)
    return rdd.distinct()


sc = SparkContext()
## input path/file
in_path = sys.argv[1]
## output path/file
out_path = sys.argv[2]

rdd = sc.textFile(in_path)

### converting string from the text file to int, and storing them as tuple
rdd = rdd.map(lambda x: (int(x.split(' ')[0]),int(x.split(' ')[1])) )

last_sum_small_star = 0  ## to check for termination condition of the outer loop

while True:

    last_sum_large_star = 0 ## to check for termination condition of the inner loop
    while True:
        rdd = large_star(rdd)   ## running the large star operation 
        
        ## comparing sum of the second vertices of the tuples with the sum in the last iteration
        ## break if sum same, which implies no change 
        current_sum_large_star = rdd.map(lambda x:x[1]).sum()
        if current_sum_large_star == last_sum_large_star:
            break

        last_sum_large_star = current_sum_large_star
    
    rdd = small_star(rdd) ## applying the small star operation
    
    ## comparing sum of the second vertices of the tuples with the sum in the last iteration
    ## break if sum same, which implies no change
    current_sum_small_star = rdd.map(lambda x:x[1]).sum()
    if current_sum_small_star == last_sum_small_star:
        break
    
    last_sum_small_star =  current_sum_small_star



    
rdd.saveAsTextFile(out_path)
