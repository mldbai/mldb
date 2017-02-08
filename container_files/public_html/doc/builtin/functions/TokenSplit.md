# Token Split Function

The token split function is a utility function that will insert a separating character before and after specified tokens in a string.
This can be useful when preparing bags of words data for training. The function will add the specified split character only if there
is not one already.

## Configuration
![](%%config function tokensplit)

## Input and Output Values

The function takes a single input named `text` that contains the string to parse and returns a single input named `output` that contains
the input string with the split characters inserted.

## Example

As a example, consider a function of type `tokensplit` defined this way:

```python
mldb.put("/v1/functions/split_smiley", {
    "type": "tokensplit",
    "params": {
        "tokens": "select ':P', '(>_<)', ':-)'",
        "splitChars": " "
        "splitCharToInsert": " "
        }
})
```

Given this call

```python
mldb.get("/v1/query", 
    q="select split_smiley({text: ':PGreat day!!! (>_<)(>_<) :P :P :P:-)'}) as x"
)
```

the function `split_smiley` will add spaces before and after emojis matching the list above but leave unchanged the ones that are already separated by a space.

```python
":P Great day!!! (>_<) (>_<) :P :P :P :-)"
```

## See also

* The [tokenize built-in function](../sql/ValueExpression.md.html#importfunctions) can be applied on text to create a _bag of words_.
