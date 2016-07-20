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

![](%%notebookexample function stemmer)


<br>
<a name="stemmerdoc"></a>
## Stemmer on documents Function

A function of this type creates a stemmer that can be used on whole strings.
It works in a way similar to the [Stemmer Function](#stemmer) and has the same configuration.
However, its input and output formats are different. It will also stem each word in the string, 
using spaces as the separator.


### Input and Output Values

Functions of this type have a single input value named `document` that is a string, and 
a single output value named `stemmed document`.

### Example

![](%%notebookexample function stemmerdoc)

## See also

* [Stemming](https://en.wikipedia.org/wiki/Stemming) on Wikipedia
* The [Snowball library](http://snowball.tartarus.org/index.php)
