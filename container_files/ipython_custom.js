function mldb_defer() {
    if (window.jQuery) {
        $("#header-container").append(
            $("<div>", {class:"pull-right", style:"padding: 10px;"}).append(
                $("<a>", {href:"{{HTTP_BASE_URL}}/doc", style:"font-weight: bold; padding: 10px;"}).text("MLDB Documentation")
            )
        );

        $.getJSON("{{HTTP_BASE_URL}}/resources/version.json", function(version){
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

        $.getJSON("../../resources/expiration.json", function(data) {
            setExpiration(data.expiration);
        });

        function setExpiration() {
            $("#header-container").css("text-align", "center").append(
                $("<span>").html("Container Expiration<br/>" + (new Date(expiration).toLocaleString()))
            );
        }

        console.log("MLDB custom.js end");
    }
    else {
        console.log("MLDB custom.js defer");
        setTimeout(mldb_defer, 50);
    }
}

console.log("MLDB custom.js start");
mldb_defer();
