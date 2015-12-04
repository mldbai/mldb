# Documentation Serving

MLDB contains support for serving up documentation that is delivered
along with a entity type. When a plugin defines a new entity type (only possible at build-time for the moment), it can register the documentation for that type with the Documentation Browser.

In addition, a plugin can register its own documentation, which is served under `/v1/plugins/<id>/doc`, and will be rendered with Markdown support as explained below.

## <a name="linking"></a>The Documentation Browser and Linking to Documentation

If you see a sidebar on the left, this document is being served within the Documentation Browser under `/doc`.

If you want to link to a specific piece of documentation through the Documentation Browser, you can deep-link all the way to an anchor inside a document like this: `/doc#builtin/DocumentationServing.md.html#linking`. You can create an anchor anywhere with `<a name="anchor"></a>`.

If you want to deep-link to a specific REST route within the Interactive Documentation, you can use something like `/doc#/doc/rest.html#GET:/v1/query`.

### Documentation in Notebooks

Sometimes it's more convenient to show how to do something in a Notebook. You can link to a specific notebook that ships with MLDB like this `![](%%nblink _demos/Predicting Titanic Survival)` which gives this: ![](%%nblink _demos/Predicting Titanic Survival).

## Writing Documentation

Documentation is delivered as HTML and Markdown files, with support for
arbitrary other assets in the documentation (png files, css files, js files,
etc).

The Markdown format is documented [here](Markdown.md).

### Markdown Support

Markdown files (files whose name ends in `.md`) will be transformed into html files with a built-in Markdown processor at serve-time if you append `.html` to their name when requesting them over HTTP.

### Fenced code functions

You can put your code in functions delimited with three backticks:

```
   ```
   var code = "Code goes here"
   ```
```

which produces the following output:

```
var code = "Code goes here"
```

### Syntax highlighting

Syntax highlighting should work with any code in code functions.  It's implemented
using [highlight.js] (https://highlightjs.org/).

#### Markdown Math

Math is supported as $LaTeX$ expressions within `\\( ... \\)` characters (inline),
or within broken-out `$$` character pairs out of line.  For example,

    $$
    e^{i\pi}-1=0
    $$

renders as

$$
e^{i\pi}-1=0
$$


#### Markdown Macros

It also accepts macros registered with the following syntax

    ![](%%macroname param1 param2 ...)

These macros allow access to extensions to the Markdown syntax.

The extensions currently supported are:

* `config <kind> <type>`
* `codeexample <file> <lang>`
* `availabletypes <entity> <format>`
* `jmlclassifier <cls>`
* `type <type>`
* `doclink <type> <kind>`
* `nblink <notebook name>`

