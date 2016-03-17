function mldb_defer() {
    if (window.jQuery) {
        $('#tabs a[href="#notebooks"]')
            .text("Notebooks & Files")
            .css({"font-size": "18px"})                                       
        $('#tabs a[href="#running"]')
            .text("Active Notebooks")
            .css({"font-size": "18px"});                                 
        $('#tabs a[href="#clusters"]').remove();    

        $("#header-container").append(
            $("<div>", {class:"pull-right", style:"padding: 10px;"}).append(
                $("<a>", {href:"{{HTTP_BASE_URL}}/doc", style:"font-weight: bold; padding: 10px;"}).text("MLDB Documentation")
            )
        );

        $.getJSON("{{HTTP_BASE_URL}}/resources/version.json?t="+Date.now(), function(version){
            $("#ipython_notebook").append(
                $("<span>", {style: "font-size: 12px;"}).text("version "+version.version)
            );
        })

        if(window.location.pathname.endsWith("tree")){
            $("#tab_content").before(
                $("<div align=center>").append(
                    $('<iframe width="530" height="300" '+
                        'style="border: 1px solid grey; margin: 10px;" '+
                        'src="https://www.youtube.com/embed/YR9tfxA0kH8" '+
                        'frameborder="0" allowfullscreen>')
                )
            );
        }

        $.getJSON("{{HTTP_BASE_URL}}/resources/expiration.json", function(data) {
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
                    .html("MLDB Container expired " + Math.abs(minLeft) + " minutes ago.");
                }
                else if(minLeft < 10) {
                    countDownSpan
                    .addClass("warning")
                    .html("MLDB Container expires in " + minLeft + " minutes: export your data now!");
                }
                else {
                    countDownSpan.html("MLDB Container expires in " + minLeft + " minutes");
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
