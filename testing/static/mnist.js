// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


var Row = React.createClass({
    render: function() {
        return <div className="row" onClick={this.handleClick}>{this.props.data.rowName}</div>;
    },
    handleClick: function (event) {
        console.log("selected row " + this.props.data.rowName);
        
        var element = document.getElementById('currentimage');

        var label;

        var pixels = this.props.data.columns.map(function(column) {
            if (column[0] == "label")
                label = column[1];
            var matches = column[0].match(/r([0-9]+)c([0-9]+)/);
            if (!matches)
                return null;
            var x = matches[2] * 10;
            var y = matches[1] * 10;
            var val = 255 - column[1];

            var color = "rgb(" + val + "," + val + "," + val + ")";

            return <rect width="10" height="10" x={x} y={y} style={{fill: color}} />;
        });



        React.render(      
                <div>
                <div style={{'font-size': 64, 'float': 'right'}}>{label}</div>
                <svg width="280" height="280" id="currentimage">{pixels}</svg>
                </div>
                , element);
    }
});

var Rows = React.createClass({
    render: function() {
        //console.log(this.props.data);
        var rowNodes = this.props.data.map(function(row) {
            return (
                    <Row data={row}/>
            );
        });
        return <div className="rows">{rowNodes}</div>;
    }
});

var RowList = React.createClass({
    loadRows: function() {
        $.ajax({
            url: this.props.url,
            dataType: 'json',
            success: function (data) {
                this.setState({data: data});
            }.bind(this),
            error: function (xhr, status, err) {
                console.error(this.props.url, status, err.toString());
            }.bind(this)
        });
    },
    getInitialState: function() {
        return {data: []};
    },
    componentDidMount: function() {
        this.loadRows();
        setInterval(this.loadRows, this.props.pollInterval);
    },
    render: function() {
        return (
            <div className="row">
                <h1>Rows</h1>
                <Rows data={this.state.data} />
            </div>
        );
    }
});


/*
var element = document.getElementById('example');

React.render(
        <RowList url="/v1/datasets/mnist_testing/query?query=*&where=rowHash%2510=0&limit=20" pollInterval={1000000} />
        , element
);
*/


var SvdRow = React.createClass({
    render: function() {
        return <div className="svdrow" onClick={this.handleClick}>{this.props.data}</div>;
    },
    handleClick: function (event) {
        //console.log(this.props);
        console.log("selected svd row " + this.props.data);
        
        var url = "/v1/datasets/svd_col_output/query?select=svd" + this.props.data;
        
        //console.log("fetching url " + url);

        $.ajax({
            url: url,
            dataType: 'json',
            success: function (data) {
                //console.log("got result back from query");
                //console.log(data);
                this.renderSvd(data);
            }.bind(this),
            error: function (xhr, status, err) {
                console.error(this.props.url, status, err.toString());
            }.bind(this)
        });
    },
    renderSvd: function(data) {
        var element = document.getElementById('currentsvd');

        var pixels = data.map(function(row) {
            var matches = row.rowName.match(/r([0-9]+)c([0-9]+)/);
            if (!matches)
                return null;
            var x = matches[2] * 10;
            var y = matches[1] * 10;

            var val = Math.round(Math.min(255, Math.max(0, 128 - row.columns[0][1] * 800)));

            var color = "rgb(" + val + "," + val + "," + val + ")";

            //console.log(column, color);

            return <rect width="10" height="10" x={x} y={y} style={{fill: color}} />;
        });

        React.render(      
                <div>
                <svg width="280" height="280" id="currentimage">{pixels}</svg>
                </div>
                , element);
    }
});

var SvdRows = React.createClass({
    render: function() {
        //console.log(this.props.data);
        var rowNodes = this.props.data.map(function(row) {
            return (
                    <SvdRow data={row}/>
            );
        });
        return <div className="rows">{rowNodes}</div>;
    }
});

function zeroFill( number, width )
{
  width -= number.toString().length;
  if ( width > 0 )
  {
    return new Array( width + (/\./.test( number ) ? 2 : 1) ).join( '0' ) + number;
  }
  return number + ""; // always return a string
}

var SvdList = React.createClass({
    getInitialState: function() {
        var elements = [];
        for (var i = 0;  i < 10;  ++i)
            elements.push(zeroFill(i, 4));
        return {data: elements};
    },
    render: function() {
        return (
            <div className="row">
                <h1>SVD Dimensions</h1>
                <SvdRows data={this.state.data} />
            </div>
        );
    }
});

var svdElement = document.getElementById('svd');

React.render(<SvdList />, svdElement);



var margin = {top: 20, right: 20, bottom: 30, left: 40},
    width = 960 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

var x = d3.scale.linear()
    .range([0, width]);

var y = d3.scale.linear()
    .range([height, 0]);

var color = d3.scale.category10();

var xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom");

var yAxis = d3.svg.axis()
    .scale(y)
    .orient("left");

var svg = d3.select("#tsne").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

function render(data)
{
    var element = document.getElementById('tsneimage');

    var label;

    var pixels = data.columns.map(function(column) {
        if (column[0] == "label")
            label = column[1];
        var matches = column[0].match(/r([0-9]+)c([0-9]+)/);
        if (!matches)
            return null;
        var x = matches[2] * 10;
        var y = matches[1] * 10;
        var val = 255 - column[1];

        var color = "rgb(" + val + "," + val + "," + val + ")";

        return <rect width="10" height="10" x={x} y={y} style={{fill: color}} />;
    });



    React.render(      
            <div>
            <div style={{'font-size': 64, 'float': 'right'}}>{label}</div>
            <svg width="280" height="280" id="currentimage">{pixels}</svg>
            </div>
            , element);

}

var current;

function display(rowName)
{
    var url = "/v1/datasets/cls_training/query?select=label,r*&where="
        + encodeURIComponent("rowName()='" + rowName + "'");
        
    console.log("fetching url " + url);

    //if (current)
    //    current.abort();

    current = $.ajax({
        url: url,
        dataType: 'json',
        success: function (data) {
            //console.log("got result back from query");
            //console.log(data);
            render(data[0]);
            //this.renderSvd(data);
        }.bind(this),
        error: function (xhr, status, err) {
            console.error(this.props.url, status, err.toString());
        }.bind(this)
    });
}


d3.json("/v1/datasets/cls_training/query?select=x,y,label&limit=10000", function(error, datain) {
    var data = datain.map(function (row) { res = {rowName: row.rowName};  row.columns.forEach(function(col) { res[col[0]] = col[1]; }); return res; });

    //console.log(data);

    x.domain(d3.extent(data, function(d) { return d.x; })).nice();
    y.domain(d3.extent(data, function(d) { return d.y; })).nice();

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis)
        .append("text")
        .attr("class", "label")
        .attr("x", width)
        .attr("y", -6)
        .style("text-anchor", "end");

    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis)
        .append("text")
        .attr("class", "label")
        .attr("transform", "rotate(-90)")
        .attr("y", 6)
        .attr("dy", ".71em")
        .style("text-anchor", "end");

    svg.selectAll(".dot")
        .data(data)
        .enter().append("circle")
        .attr("class", "dot")
        .attr("r", 3.5)
        .attr("cx", function(d) { return x(d.x); })
        .attr("cy", function(d) { return y(d.y); })
        .style("fill", function(d) { return color(d.label); })
        .on("mouseover", function (d) { display(d.rowName); });

    var legend = svg.selectAll(".legend")
        .data(color.domain())
        .enter().append("g")
        .attr("class", "legend")
        .attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

    legend.append("rect")
        .attr("x", width - 18)
        .attr("width", 18)
        .attr("height", 18)
        .style("fill", color);

    legend.append("text")
        .attr("x", width - 24)
        .attr("y", 9)
        .attr("dy", ".35em")
        .style("text-anchor", "end")
        .text(function(d) { return d; });
    
});

// ROC curve
{
    var margin = {top: 20, right: 20, bottom: 30, left: 40},
    width = 960 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

    var x = d3.scale.linear()
        .range([0, width]);

    var y = d3.scale.linear()
        .range([height, 0]);

    var color = d3.scale.category10();

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");

    var yAxis = d3.svg.axis()
        .scale(y)
        .orient("left");

    var svg = d3.select("#roc").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    d3.json("/v1/datasets/cls_test_results/query", function(error, datain) {
        return;

        var data = datain.map(function (row) { res = {rowName: row.rowName};  row.columns.forEach(function(col) { res[col[0]] = col[1]; }); return res; });

        //console.log(data);

        x.domain(d3.extent(data, function(d) { return d.falsePositiveRate; })).nice();
        y.domain(d3.extent(data, function(d) { return d.truePositiveRate; })).nice();

        svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis)
            .append("text")
            .attr("class", "label")
            .attr("x", width)
            .attr("y", -6)
            .style("text-anchor", "end");

        svg.append("g")
            .attr("class", "y axis")
            .call(yAxis)
            .append("text")
            .attr("class", "label")
            .attr("transform", "rotate(-90)")
            .attr("y", 6)
            .attr("dy", ".71em")
            .style("text-anchor", "end");

        //svg.append("path")
        //    .datum(data)
        //    .attr("class", "line")
        //    .attr("d", line);

        svg.selectAll(".dot")
            .data(data)
            .enter().append("circle")
            .attr("class", "dot")
            .attr("r", 3.5)
            .attr("cx", function(d) { return x(d.falsePositiveRate); })
            .attr("cy", function(d) { return y(d.truePositiveRate); })
            .style("fill", function(d) { return color(d.label); })
            .on("mouseover", function (d) { /*display(d.rowName);*/ });

        var legend = svg.selectAll(".legend")
            .data(color.domain())
            .enter().append("g")
            .attr("class", "legend")
            .attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

        legend.append("rect")
            .attr("x", width - 18)
            .attr("width", 18)
            .attr("height", 18)
            .style("fill", color);

        legend.append("text")
            .attr("x", width - 24)
            .attr("y", 9)
            .attr("dy", ".35em")
            .style("text-anchor", "end")
            .text(function(d) { return d; });
        
    });
}