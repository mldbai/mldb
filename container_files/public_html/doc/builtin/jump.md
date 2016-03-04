<style>ul {display: none}</style>

* datasets ![](%%availabletypes dataset list)
* procedures ![](%%availabletypes procedure list)
* functions ![](%%availabletypes function list)
* plugins ![](%%availabletypes plugin list)

<script src="/resources/js/jquery-1.11.2.min.js" type="text/javascript"></script>
<script>
$(function(){
    $("a").each(function(){ 
        var entity = $(this).parent().parent().parent().text().split(" ")[0];
        var entity_type = $(this).text();
        if( window.location.href.endsWith(entity+"/"+entity_type) ) { 
           window.location = $(this).attr("href");
        };
    });
});
</script>
