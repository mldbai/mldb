# Parse User-Agent Function

Functions of this type parse a [user agent string](https://en.wikipedia.org/wiki/User_agent) into its components.

It is a wrapper around the [BrowserScope](http://www.browserscope.org)'s user agent string parser.

## Configuration

![](%%config function http.useragent)

## Input and Output Values

Functions of this type have a single input value named `ua` which is a string.

The output values of functions of this type are:

- `os`: Operating system with two sub fields: `family` and `version`.
- `browser`: Browser with two sub fields: `family` and `version`.
- `device`: Device with two sub fields: `brand` and `model`.
- `isSpider`: Boolean representing if the user agent is a spider. 

## Example

Assume we have created a function of this type called `ua_parser`, the following call:

```sql
SELECT ua_parser({ua: 'Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3'}) as *
```

will return:

| rowName | browser.family | browser.version | device.brand | device.model | isSpider | os.family | os.version |
|-----------|----------|----------|-----------|----------|----------|----------|----------|
| result | Mobile Safari | 5.1.0 | Apple | iPhone | 0 | iOS | 5.1.1 |

## See also

* [BrowserScope](http://www.browserscope.org) website
* [uap-core](https://github.com/ua-parser/uap-core) project

