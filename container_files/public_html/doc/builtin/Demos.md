# Demos & Tutorials

MLDB ships with some demo and tutorial [Notebooks](Notebooks.md). 

## How-to Tutorials

### First steps with MLDB

* ![](%%nblink _tutorials/Using pymldb Tutorial)
* ![](%%nblink _tutorials/Loading Data Tutorial) 
* ![](%%nblink _tutorials/Querying Data Tutorial) 
* ![](%%nblink _tutorials/SELECT Tutorial) 
* ![](%%nblink _tutorials/Procedures and Functions Tutorial)
* ![](%%nblink _tutorials/Using pymldb Progress Bar and Cancel Button Tutorial)

### Intermediate
* ![](%%nblink _tutorials/Loading Data From An HTTP Server Tutorial)
* ![](%%nblink _tutorials/Virtual Manipulation of Datasets Tutorial) 
* ![](%%nblink _tutorials/Executing JavaScript Code Directly in SQL Queries Using the jseval Function Tutorial)
* ![](%%nblink _tutorials/Selecting Columns Programmatically Using Column Expressions Tutorial)
* ![](%%nblink _tutorials/Identifying Biased Features Tutorial)
* ![](%%nblink _tutorials/Tensorflow Image Recognition Tutorial) 

## Benchmarks
The ![](%%nblink _demos/Benchmarking MLDB) notebook shows how to run the [The Absolute Minimal Machine Learning Benchmark](https://github.com/szilard/benchm-ml/tree/master/z-other-tools) with MLDB.

## Transfer Learning with Tensorflow
The ![](%%nblink _demos/Transfer Learning with Tensorflow) demo demonstrates how to do transfer learning to leverage the power of a deep convolutional neural network without having to train one yourself. Most people do not train those types of networks from scratch because of the large data and computational power requirements. What is more common is to train the network on a large dataset (unrelated to our task) and then leverage the representation it learned.

## Image Processing and Image Recognition Basics
This topic is broken down in two demos. First, the ![](%%nblink _demos/Image Processing with Convolutions) demo explains convolutions and shows different ways to do them with MLDB. Second, the ![](%%nblink _demos/Real-Time Digits Recognizer) demo goes though the machine learning concepts necessary to build the [MLPaint plugin](https://github.com/mldbai/mlpaint).

## Predicting Titanic Survival
The ![](%%nblink _demos/Predicting Titanic Survival) demo shows a classification workflow:

* importing data with the ![](%%doclink import.text procedure)
* to training and evaluating a classifier with the ![](%%doclink classifier.experiment procedure) 
* calling a classifier with the ![](%%doclink classifier function)
* understanding it with the ![](%%doclink classifier.explain function)

## Mapping Reddit and Visualizing StackOverflow tags
The ![](%%nblink _demos/Mapping Reddit) and  ![](%%nblink _demos/Visualizing StackOverflow Tags)  demos show how to use MLDB to visualize high-dimensional datasets:

* doing dimensionality reduction with the ![](%%doclink svd.train procedure)
* clustering with the ![](%%doclink kmeans.train procedure)
* visualizting with the ![](%%doclink tsne.train procedure).

## Recommending Movies
The ![](%%nblink _demos/Recommending Movies) demo shows how to use MLDB to do recommendation with the ![](%%doclink svd.train procedure).

## Exploring Favourite Recipes
The ![](%%nblink _demos/Exploring Favourite Recipes) demo shows how to use MLDB to do clustering/topic extraction among recipes with the ![](%%doclink kmeans.train procedure).

## Spam Filtering
The ![](%%nblink _demos/Enron Spam Filtering) demo uses MLDB to show the perils of over-reliance on Area Under the Curve as a metric for evaluating classifiers.

## Natural Language Processing with Word2Vec
The ![](%%nblink _demos/Mapping Election Press Releases) demo uses MLDB to visualize the relationships between texts with Word2Vec using the ![](%%doclink import.word2vec procedure).

## Using SQL to explore the Panama Papers
The ![](%%nblink _demos/Investigating the Panama Papers) demo shows off MLDB's SQL engine by exploring the raw data from the *Offshore Leaks Database*.

