# Support Vector Machine Training Procedure

This procedure trains a Support Vector Machine (SVM) model and stores the model file to disk.
It is a wrapper around the popular open-source LIBSVM library.
For more information about LIBSVM: <http://www.csie.ntu.edu.tw/~cjlin/libsvm/>

## Configuration

![](%%config procedure svm.train)

### Type of SVM

There are 5 types of SVM that can be trained:

- `classification` will train a regular SVM for multi-class classification
- `nu-classification` will train the nu version of multi-class SVM classification
- `one class` will train a one-class SVM that will evaluate how alike a vector is to the training input
- `regression` will train a SVM for regression
- `nu-regression` will train the nu version of SVM for regression

In the nu version of the SVM, the nu parameter is used to control the number of support vectors. 

You can choose the type of SVM in the `svmType` parameter of the procedure training

### Label

You must set the `label` parameter of the procedure training to specify which column in the input
is to be used as label for classification, or as regression value. All other columns will be used 
as the feature vector.

### Configuration Contents

Here are the fields that you can specify in `configuration`:

- `kernel` specifies the type of SVM kernel to be used (see below). Default value is 'rbf'
- `degree` specifies the degree of polynome for polynomial kernels. Default value is '3'
- `coef0` specifies the coefficient of polynomial for sigmoid kernels. Default value is 0.
- `eps` specifies the stopping criteria for SVM training. Default value is 1e-3.
- `C` specifies the C parameter for various kernels. Default value is 1.
- `gamma` specifies gamma parameter for various kernels. Default value is 1 divided by the number of features.
- `nu` specifies the nu parameter for NU and one class SVM. Default value is 0.5.
- `p` specifies the p parameter for SVM regression. Default value is 0.1.
- `shrinking` specifies whether to use shrinking heuristics. Default is 1.
- `probability` specifies whether to perform probability estimates. Default is 0.


#### Kernels

The following type of kernels are supported, when applying feature vectors x and y:

- `linear` for a Linear kernel: x dot y
- `poly` for a Polynomial kernel: (gamma * (x dot y) + coef0)^degree
- `rbf` for an radial basis function (RBF) kernel: e^(-gamma*(x^2 +y^2 - 2*(x dot y))). This is the default kernel.
- `sigmoid` for a sigmoidal kernel : tanh(gamma * (x dot y) + coef0)

## See also

* The ![](%%doclink classifier.test procedure) allows the accuracy of a predictor to be tested against
held-out data.
* The ![](%%doclink svm function) applies a trained SVM to a feature vector, producing a classification score.
