import sys as _sys

_sys.oldStdOut = _sys.stdout
_sys.oldStdErr = _sys.stderr

class CatchOutContainer:
    def __init__(self):
        import json as _json
        self._json = _json

        import datetime as _datetime
        self._datetime = _datetime

        self.value = []
    def write(self, txt, method):
        if len(txt.strip()) == 0: return    # ignore whitespaces
        self.value.append(self._json.dumps(
                    [self._datetime.datetime.now().isoformat(), method, txt]))

class CatchOutErr:
    def __init__(self, catchOut, method):
        self.catchOut=catchOut
        self.method=method
    def write(self, txt):
        self.catchOut.write(txt, self.method)
    def flush(self):
        pass


catchOut = CatchOutContainer()
catchStdErr = CatchOutErr(catchOut, "stderr")
catchStdOut = CatchOutErr(catchOut, "stdout")

_sys.stdout = catchStdOut
_sys.stderr = catchStdErr
