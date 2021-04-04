  // create an array with nodes
  var nodes_ = new vis.DataSet(nodes);

  // create an array with edges
  var edges_ = new vis.DataSet(edges);

  // create a network
  var container = document.getElementById('mynetwork');
  var data = {
    nodes: nodes_,
    edges: edges_
  };
  var options = {};
  var network = new vis.Network(container, data, options);