<h1 id="plugin-gallery">Plugin Gallery</h1>

Here is a short list of open-source plugins for MLDB that you can make use of immediately or use as starting points to create new plugins.

<script src="/resources/js/jquery-1.11.2.min.js"></script>
<script>
$(function(){
    $.getJSON("https://plugins.mldb.ai/registry.json", function(plugins){
        $("#plugin-gallery").parent().append( 
            plugins.map(function(plugin){
                return $("<div>").append(
                    $("<h2>").text(plugin.name),
                    $("<p>").text(plugin.description),
                    $("<p>").append($("<a>", {href: plugin.website, target: "_blank"}).text(plugin.website))
                );
            })
        );
    });
});
</script>