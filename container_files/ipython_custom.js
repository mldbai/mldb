function mldb_defer() {
    if (window.jQuery) {
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
                $("<div>", {style:"font-size: 18px; margin: 0 auto; width: 700px;"}).append(
                    $("<p>", {style:"margin: 10px; line-height: 1.6;"}).html("This is MLDB's <a href='http://jupyter.org' target='_blank'>Jupyter</a>-powered Notebook interface, which enables you to interact with our demos and tutorials below."),
                    $("<p>", {style:"margin: 10px; line-height: 1.6;"}).html("You can also check out our <a href='{{HTTP_BASE_URL}}/doc'>documentation</a> or get in touch with us at any time at <a href='mailto:mldb@datacratic.com'>mldb@datacratic.com</a> with questions or feedback.")
                )
            );
        }

        $.getJSON("{{HTTP_BASE_URL}}/resources/expiration.json", function(data) {
            var exp = Date.parse(data.expiration);
            var countDownSpan = $("<div>", {id:"countdown", 
                class: "notification_widget btn btn-xs navbar-btn pull-right"});
            if(window.location.pathname.endsWith("tree")) {
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
                    .html("MLDB Container expired " + Math.abs(minLeft) + " minutes ago: save your data now!");
                }
                else if(minLeft < 10) {
                    countDownSpan
                    .addClass("warning")
                    .html("MLDB Container expires in " + minLeft + " minutes: save your data now!");
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
