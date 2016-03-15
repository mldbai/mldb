# Tensorflow documentation

MLDB incorporates the
[Tensorflow deep learning framework](https://www.tensorflow.org/).
This allows MLDB to apply and train deep learning models.

The following functionality is currently exposed in MLDB:

* Application of serialized Tensorflow graphs as a function
  (see the [Tensorflow Graph Function](TensorflowGraph.md)
* All Tensorflow built-in functions are callable from MLDB using the
  `tf_` prefix.  For example, `tf_EncodePng` for the `EncodePng`
  operation.  Each takes two arguments, the first being the input
  data (either a value, or a row with the names and values of
  each argument), and the second being a row with the options
  and arguments to the operation.
* Access to the full set of Python wrappers for tensorflow.  This
  allows the examples in Tensorflow to be applied unmodified from within
  a notebook, and for models to be trained or fine-tuned.
* Tensorflow Python examples/tutorials that are packaged with Tensorflow.  These
  can all be accessed by importing `tensorflow.examples.<example>`.

The following functionality is currently not exposed in MLDB:

* The Tensorboard
* The Tensorflow documentation (it is available [online](https://www.tensorflow.org/)

# Tensorflow Operations

![](%%tfOperation EncodePng)
![](%%tfOperation MatrixInverse)
