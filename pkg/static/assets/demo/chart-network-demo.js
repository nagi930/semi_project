  // create an array with nodes
  var nodes = new vis.DataSet(node_dataset);

  // create an array with edges
  var edges = new vis.DataSet(edge_dataset);

  // create a network
  var container = document.getElementById('mynetwork');
  var data = {
    nodes: nodes,
    edges: edges
  };
  var options = {};
  var network = new vis.Network(container, data, options);