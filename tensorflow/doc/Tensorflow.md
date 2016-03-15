# Tensorflow documentation

MLDB incorporates the
[Tensorflow deep learning framework](https://www.tensorflow.org/).
This allows MLDB to apply and train deep learning models.

The following functionality is currently exposed in MLDB:

* Application of serialized Tensorflow graphs as a function
  (see the [Tensorflow Graph Function](TensorflowGraph.md)
* Access to the full set of Python wrappers for tensorflow.  This
  allows the examples in Tensorflow to be applied unmodified from within
  a notebook, and for models to be trained or fine-tuned.
* Tensorflow Python examples/tutorials that are packaged with Tensorflow.  These
  can all be accessed by importing `tensorflow.examples.<example>`.

The following functionality is currently not exposed in MLDB:

* The Tensorboard
* The Tensorflow documentation (it is available [online](https://www.tensorflow.org/)

# Tensorflow modifications

The base distribution of Tensorflow has been modified to include a
fine-grained thread scheduler.  This typically reduces CPU-only
training and prediction times by a factor of 2 to 3 on many core
CPUs, as it allows more jobs to run in parallel.



