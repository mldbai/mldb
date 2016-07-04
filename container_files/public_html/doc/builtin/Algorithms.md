# Algorithm Support

MLDB [Procedures](procedures/Procedures.md) are used to train models, which can then be applied via [Functions](functions/Functions.md).

## Supervised Machine Learning

* The ![](%%doclink classifier.train procedure) can train:
    * [Generalized Linear Models](https://en.wikipedia.org/wiki/Generalized_linear_model) 
    * [Logistic Regressions](https://en.wikipedia.org/wiki/Logistic_regression)
    * [Decision Trees](https://en.wikipedia.org/wiki/Decision_tree_learning) (including [Random Forests](https://en.wikipedia.org/wiki/Random_forest))
    * [Neural Networks](https://en.wikipedia.org/wiki/Artificial_neural_network)
    * [Naive Bayes models](https://en.wikipedia.org/wiki/Naive_Bayes_classifier)
    * with [Bagging](https://en.wikipedia.org/wiki/Bootstrap_aggregating)
    * with [Boosting](https://en.wikipedia.org/wiki/Boosting_(machine_learning))
* The ![](%%doclink svm.train procedure) can train [Support Vector Machines (SVM)](https://en.wikipedia.org/wiki/Support_vector_machine)
* The ![](%%doclink probabilizer.train procedure) can calibrate classifiers

## Deep Learning

* The ![](%%doclink tensorflow.graph function) can execute [TensorFlow](https://www.tensorflow.org/) models

## Clustering 

* The ![](%%doclink kmeans.train procedure) can train [K-Means models](https://en.wikipedia.org/wiki/K-means_clustering) 

## Dimensionality Reduction, Visualization & Manifold Learning

* The ![](%%doclink svd.train procedure) can perform [Truncated Singular Value Decompositions (SVD)](https://en.wikipedia.org/wiki/Singular_value_decomposition)
* The ![](%%doclink tsne.train procedure) can perform [t-distributed Stochastic Neighbor Embedding (t-SNE)](https://en.wikipedia.org/wiki/T-distributed_stochastic_neighbor_embedding)

## Feature Engineering

* The ![](%%doclink import.sentiwordnet procedure) can import [SentiWordNet](http://sentiwordnet.isti.cnr.it/) models 
* The ![](%%doclink import.word2vec procedure) can import [Word2Vec](https://code.google.com/p/word2vec/) embeddings 
* The ![](%%doclink tfidf.train procedure) can train [Term-Frequency/Inverse-Document-Frequency (TF-IDF) models](https://en.wikipedia.org/wiki/Tf%E2%80%93idf)
* The ![](%%doclink statsTable.train procedure) can assemble tables of counts to assemble [count-based features](https://www.youtube.com/watch?v=b7OSggJUVPY)
* The ![](%%doclink feature_hasher function) can be used to do [feature hashing](https://en.wikipedia.org/wiki/Feature_hashing), which is a way to vectorize features

