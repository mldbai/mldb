function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

function assertInSet(expr, setofvals, msg)
{
    if (setofvals.has(expr))
        return;

    for ([k,v] of setofvals.entries()) {
        if (JSON.stringify(expr) == JSON.stringify(v))
        return;
    }

    plugin.log("expected", setofvals);
    plugin.log("received", expr);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not in value set " + JSON.stringify(setofvals);
}

assertEqual['__markdown'] = `
### \`assertEqual\` function

Asserts that the two arguments are equal.  If they are, then the function
will return.  If they are not, the arguments will be logged and an exception
will be thrown.

`;

assertEqual['__summary'] = "`assertEqual(received, expected, msg)`: Asserts that the two arguments are equal";


function appendPath(path, key)
{
    if (typeof key == 'string') {
        return path + "." + key;
    }
    else if (typeof key == 'number') {
        return path + '[' + key + ']';
    }
    throw new Error("can't append path");
}

function assertNearlyEqual(v1, v2, path, relativeError)
{
    if (v1 === v2)
        return true;

    //mldb.log("v1 = ", typeof v1, " v2 = ", typeof v2);
    
    if (typeof v1 != typeof v2) {
        mldb.log("objects differ in type at path " + path + ": "
                 + typeof v1 + " vs " + typeof v2);
        return false;
    }

    if (typeof v1 == 'number') {
        var av1 = Math.abs(v1);
        var av2 = Math.abs(v2);
        var smallest = av1 < av2 ? av1 : av2;
        var largest = av1 < av2 ? av2 : av2;

        var diff = Math.abs(v1 - v2);
        var relative = diff / largest;

        if (relative < relativeError)
            return true;
        
        var percent = relative * 100.0;

        mldb.log("numbers differ at path " + path + ": " + v1 + " vs " + v2
                 + " has " + percent + "% relative error");
        return false;
    }
    if (typeof v1 == 'string') {
        mldb.log("strings differ at path " + path + ": " + v1 + " vs " + v2);
        return false;
    }
    
    var k1 = Object.getOwnPropertyNames(v1);
    var k2 = Object.getOwnPropertyNames(v2);

    var all = {};
    for (var i = 0;  i < k1.length;  ++i) {
        var k = k1[i];
        all[k] = [v1[k], undefined ];
    }
    for (var i = 0;  i < k2.length;  ++i) {
        var k = k2[i];
        if (all.hasOwnProperty(k)) {
            all[k] = [ all[k][0], v2[k] ];
        }
        else {
            all[k] = [ undefined, v2[k] ];
        }
    }

    var result = true;
    
    for (k in all) {
        //mldb.log('key ', k, ' all[k]', all[k]);
        if (!assertNearlyEqual(all[k][0], all[k][1], appendPath(path, k),
                              relativeError)) {
            result = false;
        }
    }

    return result;
}

assertNearlyEqual['__summary'] = '`assertNearlyEqual(received, expected, path, relativeError)` : assert that two objects are equal, apart from numbers which are roughly equal to a tolerance';

assertNearlyEqual['__markdown'] = `
This function will compare two values or structured values (objects, arrays)
recursively for near-equality.  Near equality means:

* Strings or nulls must be identical
* Numbers may be up to relativeError different (default 1%)
* Structured values (objects, arrays) must have near equality for each element.

In the case of a difference, the function will throw an exception.
`;


module.exports = { assertEqual: assertEqual, assertNearlyEqual: assertNearlyEqual, assertInSet: assertInSet };

module.exports['__markdown'] = `
## \`mldb/unittest\` module

This module contains functionality used to write unit tests for MLDB code
and plugins.

It contains the following functions:

![](%%jsfunctions mldb/unittest)
`;
