//var fn = mldb.createFunction('htmlparser', 'html.parse', {});
var html = '<html><body><a href=\"link.txt\">Link</a></body></html>';

var html = mldb.get('/doc/builtin/functions/FunctionConfig.md.html').response;

mldb.log(html);



var fn = mldb.createFunction({id: 'htmlparser', type: 'html.extractLinks', params: {}});

var html = mldb.get('/doc/builtin/functions/FunctionConfig.md.html').response;

var res = mldb.query("select htmlparser({ text: " + mldb.sqlEscape(html) + "}) as *",
                  { format: 'table'});

mldb.log(res);

"success"
