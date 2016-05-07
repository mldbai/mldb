function mldb_defer() {
    if (window.jQuery) {

        //munge the existing tabs
        $('#tabs a[href="#notebooks"]')
            .text("Notebooks & Files")
            .css({"font-size": "18px"})                                       
        $('#tabs a[href="#running"]')
            .text("Active Notebooks")
            .css({"font-size": "18px"});        

        //delete the clusters tab
        $('#tabs a[href="#clusters"]').parent().remove();   
        $('#clusters').remove();   

        //add the queries tab
        $('#tabs').append(
            $("<li>").append(
                $('<a href="#queries" data-toggle="tab" aria-expanded="false">')
                    .text("Query Runner")
                    .css({"font-size": "18px"})
            )
        );
        $('#running').after(
            $('<div id="queries" class="tab-pane">').append(
                $('<iframe width="100%" height="900" frameborder="0" '+
                        'src="{{HTTP_BASE_URL}}/resources/queries/index.html">')
            )
        );

        $("#header-container").append(
            $("<div>", {class:"pull-right", style:"padding: 10px;"}).append(
                $("<a>", {href:"{{HTTP_BASE_URL}}/doc", target:"_blank", style:"font-weight: bold; padding: 10px;"})
                    .text("MLDB Documentation")
            )
        );

        $.getJSON("{{HTTP_BASE_URL}}/version.json?t="+Date.now(), function(version){
            $("#ipython_notebook").append(
                $("<span>", {style: "font-size: 12px;"}).text("version "+version.version)
            );
        })

        if(window.location.pathname.endsWith("tree") &&
                document.cookie.indexOf("hidevideo=1") == -1){
            $("#tab_content").before(
                $("<div>", {
                        "id": "introvideo", "class":"panel panel-default",
                        style:"width: 560px; margin: 5px auto; text-align: center;"
                    })
                .append(
                    $('<button type="button" class="close">&times;</button>')
                        .on("click", function(){
                            $("#introvideo").hide();
                            document.cookie = "hidevideo=1;";
                        }),
                    $('<iframe width="530" height="300" '+
                        'style="border: 1px solid grey; margin: 10px;" '+
                        'src="https://www.youtube.com/embed/YR9tfxA0kH8" '+
                        'frameborder="0" allowfullscreen>')
                )
            );
        }

        $.getJSON("{{HTTP_BASE_URL}}/resources/expiration.json", function(data) {
            if(!data.expiration) return;
            
            var exp = Date.parse(data.expiration);
            var countDownSpan = $("<div>", {id:"countdown", 
                class: "notification_widget btn btn-xs navbar-btn pull-right"});
            if(window.location.pathname.indexOf("/ipy/tree") != -1) {
                $("#header-container").append(countDownSpan);
            }
            else {
                $("#notification_area").append(countDownSpan);
            }
            function updateCountdown(){
                var minLeft = Math.floor((exp-Date.now())/(60*1000));
                if(minLeft < 0) {
                    countDownSpan
                    .addClass("danger")
                    .html("MLDB session expired " + Math.abs(minLeft) + " minutes ago.");
                }
                else if(minLeft < 10) {
                    countDownSpan
                    .addClass("warning")
                    .html("MLDB session expires in " + minLeft + " minutes: export your data now!");
                }
                else {
                    countDownSpan.html("MLDB session expires in " + minLeft + " minutes");
                }
            }
            setInterval(updateCountdown, 10000); 
            updateCountdown();  
        })

        console.log("MLDB custom.js end");
    }
    else {
        console.log("MLDB custom.js defer");
        setTimeout(mldb_defer, 50);
    }
}

console.log("MLDB custom.js start");
mldb_defer();
