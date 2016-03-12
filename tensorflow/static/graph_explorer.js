var GraphNode = React.createClass({
    loadNodes: function() {
        var url = "/v1/functions/incept/routes/graph/nodes/" + this.props.nodeName;
        $.ajax({
            url: url,
            dataType: 'json',
            success: function (nodeInfo) {
                this.setState({nodeInfo: nodeInfo, expanded: this.props.startExpanded});
            }.bind(this),
            error: function (xhr, status, err) {
                console.error(this.props.parent, status, err.toString());
            }.bind(this)
        });
    },
    getInitialState: function() {
        return {nodeInfo: {children: []}, expanded: this.props.startExpanded};
    },
    componentDidMount: function() {
        this.loadNodes();
    },
    handleExpand: function (event) {
        var newState = this.state;
        newState.expanded = true;
        this.setState(newState);
    },
    handleContract: function (event) {
        var newState = this.state;
        newState.expanded = false;
        this.setState(newState);
    },
    render: function() {
        var that = this;
        var shortName = " ";
        if (this.state.nodeInfo.name)
            shortName = this.state.nodeInfo.name.replace(this.state.nodeInfo.parent + "/", "");

        console.log(that.state.nodeInfo);

        function renderInfo(expandSpan)
        {
            return (
            <div className="info">
              <div className="node" style={{width: "300px", textIndent: 10 * that.props.level}}>
                <div className="nodeName" style={{display:"inline-block"}}>
                  {expandSpan}
                  <span className="nodeName">{shortName}</span>
                </div>
              </div>
              <div className="op" style={{width: "100px", display:"inline-block", position:"relative", left:"300px", top: "-18px"}}>
                {that.state.nodeInfo.op}
              </div>
              <div className="device" style={{width: "100px", display:"inline-block", position:"relative", left:"400px", top: "-18px"}}>
                {that.state.nodeInfo.device}
              </div>
              <div className="inputs" style={{width: "300px", display:"inline-block", position:"relative", left:"500px", top: "-18px"}}>
                {that.state.nodeInfo.input}
              </div>
              <div className="outputs" style={{width: "100px", display:"inline-block", position:"relative", left:"800px", top: "-18px"}}>
                {that.state.nodeInfo.output}
              </div>
              <div className="attr" style={{width: "100px", display:"inline-block", position:"relative", left:"900px", top: "-18px"}}>
                {that.state.nodeInfo.attr}
              </div>

           </div>);
        }
        
        if (this.state.nodeInfo.children) {
            if (this.state.expanded) {


                var childNodes = this.state.nodeInfo.children.map(function(node) {
                    return (
                            <GraphNode nodeName={node} level={that.props.level + 1} startExpanded={false}/>
                    );
                });
                
                var expandSpan = <span style={{display: "inline", width: "30px"}} onClick={this.handleContract}>[-]</span>;

                var info = renderInfo(expandSpan);

                // Not expanded
                return (
                      <div>
                         {info}
                         {childNodes}
                      </div>
                );
            }
            else {
                var expandSpan = <span style={{width: "30px"}} onClick={this.handleExpand}>[+]</span>;
                var info = renderInfo(expandSpan);

                // Not expanded
                return (
                      <div>
                         {info}
                      </div>
                );
            }
            

        } else {
            var expandSpan = <span style={{float: "left", width: "30px"}}>&nbsp;</span>;
            // Not expanded
            return (
                  <div>
                     {renderInfo(expandSpan)}
                  </div>
            );
        }
    }
});
    

var indexElement = document.getElementById('index');

React.render(
        <GraphNode nodeName={""} level={0} startExpanded={true}/>
        , indexElement
);
