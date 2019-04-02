# GenericUnsupervisedMachineLearning
This application provides a Generic Clustering algorithm for Apache Spark deployment

## Concept 
This application allows to quickly deploy an unsupervised machine learning algorithm based on Kmeans clustering method. 
The main objective of the algorithm is the adaptation to the input, and the computation of the optimal number of cluster for the application of the clustering. 

The application is divided into three distinct parts :

- The first part is the data import and preprocessing :
The algorithm applies a transformation on the features, to obtain vectors that will be the input of the clustering algorithm.
The choosen clustering algorithm is the Kmeans.

- The second part is the application of the Silhouette method : 
The goal is to compute the optimal cluster number (optimalK) based on input data. The silhouette value is a measure of how similar an object is to its own cluster (cohesion) compared to other clusters (separation). The silhouette ranges from −1 to +1, where a high value indicates that the object is well matched to its own cluster and poorly matched to neighboring clusters. If most objects have a high value, then the clustering configuration is appropriate. If many points have a low or negative value, then the clustering configuration may have too many or too few clusters.
The algorithm test compute the Silouhette score for each K between 2 and MAX_K (choosen value in config file, default = 20) and selects the optimal by comparing the scores between them. 

- The third part is kmeans clustering application with optimalK : 
The output dataframe is saved with a new column "Cluster" to indicate the cluster of each record.
Moreover a report that describe the different clusters and their characteristics is provided thank's to GenerateReport object
The report compute the pourcentage of the categorical features in each cluster, and the median of numeric features in each cluster.

## How to run 
- Set variables in config.yml file
- Run :
	- `spark-submit --class main.Main --master local[*] /PathToJAR/.../GenericUnsupervisedMachineLearning-assembly-0.1.jar /pathToConfig/config.yml` 
  
 
## How to build 
### sbt 
The build.sbt file is provided. 
You need to create under project repo : 
- build.properties file with sbt version
- assembly.sbt file with sbt-assembly version associated to sbt version 

To build and package, inside the sbt shell, call assembly command. 

  
