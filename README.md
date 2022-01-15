# Finding Similar Items 

<p align="center">
  <img alt="KTH" src="https://img.shields.io/badge/-KTH-%231954a6?style=flat-square" />
</p>

Lab 1 for the ID2222 Data Mining course at [KTH](https://www.kth.se/en).

## Principle

The project explores different ways to find similar items from a dataset using [Apache Spark](https://spark.apache.org/).

## Tasks

You are to implement the stages of finding textually similar documents based on Jaccard similarity using the shingling, minhashing, and locality-sensitive hashing (LSH) techniques and corresponding algorithms. The implementation can be done using any big data processing framework, such as Apache Spark, Apache Flink, or no framework, e.g., in Java, Python, etc. To test and evaluate your implementation, write a program that uses your implementation to find similar documents in a corpus of 5-10 or more documents such as web pages or emails.

The stages should be implemented as a collection of classes, modules, functions or procedures depending the framework and the language of your choice. Below, we give a description of sample classes that implement different stages of finding textually similar documents. You do not have to develop the exact same classes and data types as described below. Feel free to use data structures that suit you best.

1 - A class Shingling that constructs k–shingles of a given length k (e.g., 10) from a given document, computes a hash value for each unique shingle, and represents the document in the form of an ordered set of its hashed k-shingles.
2 - A class CompareSets that computes the Jaccard similarity of two sets of integers – two sets of hashed shingles.
3 - A class MinHashing that builds a minHash signature (in the form of a vector or a set) of a given length n from a given set of integers (a set of hashed shingles).
4 - A class CompareSignatures that estimates similarity of two integer vectors – minhash signatures – as a fraction of components, in which they agree.
5 - (Optional task for extra 2 bonus) A class LSH that implements the LSH technique: given a collection of minhash signatures (integer vectors) and a similarity threshold t, the LSH class (using banding and hashing) finds all candidate pairs of signatures that agree on at least fraction t of their components.
To test and evaluate scalability (the execution time versus the size of input dataset) of your implementation, write a program that uses your classes to find similar documents in a corpus of 5-10 documents. Choose a similarity threshold s (e.g., 0,8) that states that two documents are similar if the Jaccard similarity of their shingle sets is at least s.  

## Authors

* [Nathan Labbe](https://github.com/NathanLabbe)
* [Guillaume Longrais](https://github.com/glongrais)
