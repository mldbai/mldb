# Mutable Behaviour Dataset

**This feature is part of the [MLDB Pro Plugin](../../../../doc/builtin/ProPlugin.md) and so can only be used in compliance with the [trial license](../../../../doc/builtin/licenses.md) unless a commercial license has been purchased**

This is a mutable (writeable) version of the Behavior dataset.

It allows for one single commit, at which point a "behaviour" file will
be written to the location provided in the `address` field.  Afterwards
the produced file can be loaded by the ![](%%doclink beh dataset).  An
attempt to `commit()` more than once will lead to an exception; once
committed it's really committed.

## Configuration

![](%%config dataset beh.mutable)

# See Also

* The ![](%%doclink beh dataset) allows files
  of the given format to be loaded.

