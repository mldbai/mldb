# Stemmer Function

A function of this types creates a stemmer that can be used on column names.

It is a wrapper around the Snowball open-source stemming library.
For more information about Snowball: <http://snowball.tartarus.org/index.php>

## Configuration

![](%%config function stemmer)

The following values are valid for the `language` configuration field: danish, dutch,
english, finnish, french, german, hungarian, italian, norwegian, porter, portuguese,
romanian, russian, spanish, swedish, turkish.

More information about the stemming
algorithms is available on the 
[Snowball page](http://snowball.tartarus.org/texts/stemmersoverview.html).

## Input and Output Values

Functions of this type have a single input value named `words` which is a row, and 
a single output value also named `words`.

## Example

After having created the following stemmer:

```
PUT /v1/functions/my_stemmer
{
    "type": "stemmer",
    "params": {
        "language": "english"
    }
}
```

we can tokenize a sentence and apply the stemming algorithm on the
resulting tokens:

```
SELECT my_stemmer({words: {tokenize('I like having lots', {splitchars:' '}) as *}}) as *
```

This returns:

| *rowName* | *count* |
|-----------|---------|
| words.I | 1 |
| words.have | 1 |
| words.like | 1 |
| words.lot | 1 |

# Stemmer on documents Function

A function of this types creates a stemmer that can be used on whole strings.
It works in a way similar to the Stemmer Function above and has the same configuration, but
the input and output format are different. It will treat words separated by spaces.

## Input and Output Values

Functions of this type have a single input value named `document` which is a string, and 
a single output value named `stemmed document`.

## Example

After having created the following stemmer:

```
PUT /v1/functions/my_stemmer
{
    "type": "stemmerdoc",
    "params": {
        "language": "english"
    }
}
```

we can apply the stemming algorithm on the provided string:

`SELECT my_stemmer({document: 'I like having lots'})`

This returns:

"I like have lot"

## See also

* [Stemming](https://en.wikipedia.org/wiki/Stemming) on Wikipedia

