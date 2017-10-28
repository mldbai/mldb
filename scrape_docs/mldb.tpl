{%- extends 'basic.tpl' -%}
{% from 'mathjax.tpl' import mathjax %}


{%- block header -%}
<!DOCTYPE html>
<html>
<head>

<meta charset="utf-8" />
<title>{{resources['metadata']['name']}}</title>

<script src="https://cdnjs.cloudflare.com/ajax/libs/require.js/2.1.10/require.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/2.0.3/jquery.min.js"></script>

<script>
  (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
  (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
  m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
  })(window,document,'script','//www.google-analytics.com/analytics.js','ga');

  ga('create', 'UA-16909325-9', 'auto');
  ga('send', 'pageview');

</script>

{% for css in resources.inlining.css -%}
    <style type="text/css">
    {{ css }}
    </style>
{% endfor %}

<style type="text/css">
@import url(https://fonts.googleapis.com/css?family=Roboto:400,700|Roboto+Slab:400,700);
/* Overrides of notebook CSS for static HTML export */
body {
  overflow: visible;
  padding: 8px;
}

div#notebook {
  overflow: visible;
  border-top: none;
}

@media print {
  div.cell {
    display: block;
    page-break-inside: avoid;
  } 
  div.output_wrapper { 
    display: block;
    page-break-inside: avoid; 
  }
  div.output { 
    display: block;
    page-break-inside: avoid; 
  }
}

    .rendered_html p, .rendered_html li {
        font-family: 'Roboto'; 
        font-size: 18px;
        line-height: 1.6;
    }

    .rendered_html p {
        padding-bottom: 10px;
    }

    .rendered_html h1, .rendered_html h2, .rendered_html h3, .rendered_html h4 {
        font-family: 'Roboto Slab';
        padding-bottom: 10px;
    }

    a, .rendered_html a pre, .rendered_html a code  {
        color:#3A62AD;
    }
</style>


<!-- Loading mathjax macro -->
{{ mathjax() }}

</head>
{%- endblock header -%}

{% block body %}
<body>

  <div tabindex="-1" id="notebook" class="border-box-sizing">
    <div class="container" id="notebook-container">

    <div class="cell border-box-sizing text_cell rendered">
      <div class="prompt input_prompt"> </div>
      <div class="inner_cell">
        <div class="text_cell_render border-box-sizing rendered_html">
          <p style="text-align: center;"><a href="/"><img src="/resources/images/mldb_ipython_logo.png"></a></p>
          <p style="text-align: center;">This page is part of the documentation for the <a href="http://mldb.ai">Machine Learning Database</a>.</p>
          <p style="text-align: center;">It is a static snapshot of a Notebook which you can play with interactively by <a href="/doc/#builtin/Running.md.html">trying MLDB online now</a>.<br>It's free and takes 30 seconds to get going.</p>
        </div>
      </div>
    </div>

{{ super() }}
    </div>
  </div>
</body>
{%- endblock body %}

{% block footer %}
</html>
{% endblock footer %}
