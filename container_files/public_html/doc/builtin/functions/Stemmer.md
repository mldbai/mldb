# Stemming Functions

The following function types are used to apply a
[stemming algorithm](https://en.wikipedia.org/wiki/Stemming) to column names or strings.

There are two function types available, depending on the input to the function: `stemmer` for words
and `stemmerdoc` for documents. From an [NLP](https://en.wikipedia.org/wiki/Natural_language_processing)
perspective, a string made up of many words in a cell is a *document*. Another
way to represent a document is by transforming it in a
[bag of words](https://en.wikipedia.org/wiki/Bag-of-words_model), something that can be easily
accomplished using the `tokenize` function. Each *word* then becomes a column.

Both representations can be useful to solve NLP problems. Use the corresponding stemming
function type for the data representation:

- [stemmer](#stemmer) for column names (ie: output of the [`tokenize` built-in function](../sql/ValueExpression.md.html#importfunctions))
- [stemmerdoc](#stemmerdoc) for whole strings in cells

These functions are a wrapper around the Snowball open-source stemming library.
For more information about Snowball: <http://snowball.tartarus.org/index.php>


<br>

<a name="stemmer"></a>
## Stemmer Function

A function of this types creates a stemmer that can be used on column names.


### Configuration

![](%%config function stemmer)

The following values are valid for the `language` configuration field: danish, dutch,
english, finnish, french, german, hungarian, italian, norwegian, porter, portuguese,
romanian, russian, spanish, swedish, turkish.

More information about the stemming algorithms is available on the
[Snowball page](http://snowball.tartarus.org/texts/stemmersoverview.html).

### Input and Output Values

Functions of this type have a single input value named `words` which is a row, and 
a single output value also named `words`.

### Example

After having created the following stemmer:

```python
mldb.put("/v1/functions/my_stemmer", {
    "type": "stemmer",
    "params": {
        "language": "english"
    }
})
```

and this dataset:

| rowName | potato | potatoes | carrot | carrots |
|-----------|----------|------------|----------|-----------|
|   row_0   |     1    |      2     |     3    |    None   |
|   row_1   | 'crips'  |  'chips'   |     0    | 'hi mom'  |


The following query will merge the stemmed columns, summing their content:

```sql
SELECT my_stemmer({words:{*}})[words] as * FROM our_dataset
```

and return:


| rowName | potato | carrot |
|-----------|----------|----------|
|   row_0   |     3    |     3    |
|   row_1   |     2    |     1    |


Note that strings are coerced to the integer value 1.


We can also nicely use it in conjunction with the tokenize function:

```sql
SELECT my_stemmer({words: {tokenize('I have liked having carrots', {splitchars:' '}) as *}}) as *
```

This returns:

|words.I|words.carrot|words.have|words.like|
|---------|--------------|------------|------------|
|    1    |     1        |     2      |      1     |

<br>
<a name="stemmerdoc"></a>
## Stemmer on documents Function

A function of this type creates a stemmer that can be used on whole strings.
It works in a way similar to the [Stemmer Function](#stemmer) and has the same configuration.
However, the input and output formats are different. It will also stem each word in the string, 
using spaces as the separator.


### Input and Output Values

Functions of this type have a single input value named `document` that is a string, and 
a single output value named `stemmed document`.

### Example

After having created the following stemmer:

```python
mldb.put("/v1/functions/my_stemmer", {
    "type": "stemmerdoc",
    "params": {
        "language": "english"
    }
})
```

we can apply the stemming algorithm on the provided string:

```sql
SELECT my_stemmer({document: 'I like having carrots'})
```

This returns:

```
"I like have carrot"
```

## See also

* [Stemming](https://en.wikipedia.org/wiki/Stemming) on Wikipedia
* The [Snowball library](http://snowball.tartarus.org/index.php)
