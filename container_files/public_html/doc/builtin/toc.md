
<script>
$("body").css({margin: "20px"});
$.getJSON("{{HTTP_BASE_URL}}/version.json?t="+Date.now(), function(v){ $("#version").text("version "+v.version) });
</script>
<style>
p, li {
    line-height: 1.4;
}
ul {
    padding-left: 25px;
}
</style>

<base target="rh">
<div align="center">
<a href="{{HTTP_BASE_URL}}/" target="_top"><img src="{{HTTP_BASE_URL}}/resources/images/mldb_ipython_logo.png" alt="MLDB Logo" /></a><br>
<span id="version" style="font-size: 12px;"></span>
</div>

<form action="{{HTTP_BASE_URL}}/doc/search.html">
<input style="
     font: 13px/1.6 'open sans', sans-serif;
    color: #333;
    padding: 12px 12px 12px 40px;
    width: 170px;
    border: 1px solid #e2e2e2;
    border-radius: 0;
    -moz-appearance: none;
    -webkit-appearance: none;
     box-shadow: none; 
    outline: 0;
    margin: 0;
    background: #fff url('/resources/js/tipuesearch/img/search.png') no-repeat 15px 15px;
" placeholder="search documentation" name="q" autocomplete="off" required>
</form>

### General Documentation

* [MLDB Overview](Overview.md)
* [Running MLDB](Running.md)
* [Working with the REST API](WorkingWithRest.md)
* [REST API Interactive Docs](../rest.html)
* [Notebooks and pymldb](Notebooks.md)
* [Demos & Tutorials](Demos.md)
* [Files and URLs](Url.md)
* [MLDB Pro](ProPlugin.md)
* [Algorithm Support](Algorithms.md)
* [Classifier configuration](ClassifierConf.md)
* [Scaling MLDB](Scaling.md)
* [Help and Feedback](help.md)
* [Licenses](licenses.md)

### SQL Support

* [SQL Queries](sql/Sql.md)
* [Query API](sql/QueryAPI.md)
* [Type System](sql/TypeSystem.md)
* [Value Expressions](sql/ValueExpression.md)
    * [Operators](sql/ValueExpression.md.html#operators)
    * [Built-in functions](sql/ValueExpression.md.html#builtinfunctions)
* [Select Expressions](sql/SelectExpression.md)
* [From Expressions](sql/FromExpression.md)
* [When Expressions](sql/WhenExpression.md)
* [Where Expressions](sql/WhereExpression.md)
* [Group-By Expressions](sql/GroupByExpression.md)
* [Order-By Expressions](sql/OrderByExpression.md)

### Datasets

* [Intro to Datasets](datasets/Datasets.md)
* [Dataset Configuration](datasets/DatasetConfig.md)
* [Data Persistence](datasets/Persistence.md)
* Available Dataset types: ![](%%availabletypes dataset list)

### Procedures

* [Intro to Procedures](procedures/Procedures.md)
* [Procedure Configuration](procedures/ProcedureConfig.md)
* [Input Queries](procedures/InputQuery.md)
* [Output Dataset Specification](procedures/OutputDatasetSpec.md)
* Available Procedure types: ![](%%availabletypes procedure list)

### Functions

* [Intro to Functions](functions/Functions.md)
* [Function Application via REST](functions/Application.md)
* [Function Configuration](functions/FunctionConfig.md)
* Available Function types: ![](%%availabletypes function list)

### Plugins

* [Intro to Plugins](plugins/Plugins.md)
* [Plugin Configuration](plugins/PluginConfig.md)
* [Example Plugins](plugins/ExamplePlugins.md)
* [Type Introspection](rest/Types.md)
* [Writing Documentation](DocumentationServing.md)
* Available Plugin types: ![](%%availabletypes plugin list)

