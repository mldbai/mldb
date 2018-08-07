//var fn = mldb.createFunction('htmlparser', 'html.parse', {});
var html = '<html><body><a href=\"link.txt\">Link</a></body></html>';

var html = mldb.get('/doc/builtin/functions/FunctionConfig.md.html').response;

mldb.log(html);

var res = mldb.query("select extract_text(" + mldb.sqlEscape(html) + ") as txt",
                  { format: 'table'});

mldb.log(res);

"success"
