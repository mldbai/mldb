# SVM Apply Function

This function applies a Support Vector Machine (SVM) model previously trained by a SVM procedure to a feature vector, 
predicting a label or returning a regression result.
It is a wrapper around the popular open-source LIBSVM library.
For more information about LIBSVM: <http://www.csie.ntu.edu.tw/~cjlin/libsvm/>

## Configuration

![](%%config function svm)

## Input and Output Values

The SVM function of this type has a single input value called `embedding` which is a feature vector corresponding to the
feature vector that was used for training the SVM model, without the label.

The function outputs a single value `output` which depend on the type of SVM trained:

* `classification` and `nu-classification` will return the predicted label of the feature vector
* `one class` will return a value corresponding to how alike the feature vector is to the training data
* `regression` and `nu-regression` will return the regression value