# Tensorflow documentation

MLDB incorporates the Tensorflow operation graph library in order to allow
for deep learning and gradient descent operations to be used.

Most tensorflow functions are available in MLDB queries and expressions with a 
`tf_` prefix. MLDB embeddings will be implicitly converted to tensorflow tensors 
of the same dimensions and closest matching type, and vice versa. In case of inconsistent 
type in a MLDB embedding, it will be converted to a 64-bit floating point tensor.

![](%%tfOperations)
