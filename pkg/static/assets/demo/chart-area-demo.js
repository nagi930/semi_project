// Set new default font family and font color to mimic Bootstrap's default styling
Chart.defaults.global.defaultFontFamily = '-apple-system,system-ui,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif';
Chart.defaults.global.defaultFontColor = '#292b2c';
Chart.defaults.global.defaultFontSize = 17;


// Area Chart Example


var myLineChart = null;

function createChart(labels, data, close){
if(myLineChart) {
    myLineChart.destroy();
}

var ctx = document.getElementById("myAreaChart");
  myLineChart = new Chart(ctx, {
  type: 'bar',
  data: {
    labels: labels,
    datasets: [{
      type: 'bar',
      label: "keyword count",
      yAxisID: 'A',
      backgroundColor: "rgba(100,200,100,0.5)",
      borderColor: "rgba(100,200,100,0.5)",
      data: data,
    },
    {
    type: 'line',
    label: "price",
    yAxisID: 'B',
      lineTension: 0,
      backgroundColor: "rgba(208, 49, 0, 0)",
      borderColor: "rgba(244, 87, 0, 1)",
      pointRadius: 2,
      pointBackgroundColor: "rgba(255, 32, 0, 1)",
      pointBorderColor: "rgba(255,255,255,0.8)",
      pointHoverRadius: 5,
      pointHoverBackgroundColor: "rgba(255, 32, 0, 0.5)",
      pointHitRadius: 5,
      pointBorderWidth: 2,
      data: close,
    }],
  },
  options: {
    scales: {
      xAxes: [{
        time: {
          unit: 'date'
        },
        gridLines: {
          display: false
        },
        ticks: {
          maxTicksLimit: 7
        }
      }],
      yAxes: [{
        id: 'A',
        type: 'linear',
        position: 'left',
        scaleLabel: {
          display: true,
          labelString: 'count'
        },
        ticks: {
          min: 0,
          max: 75,
          maxTicksLimit: 5
        },
        gridLines: {
          color: "rgba(0, 0, 0, 0)",
        }
      }, {
        id: 'B',
        type: 'linear',
        position: 'right',
        scaleLabel: {
          display: true,
          labelString: 'price'
        },
        ticks: {
          min: min_,
          max: max_,
          maxTicksLimit: 5
        },
        gridLines: {
          color: "rgba(0, 0, 0, .125)",
        }
      }],
    },
    legend: {
      display: true
    },
//    animation: {
//    onComplete: function () {
//      var chartInstance = this.chart,
//        ctx = chartInstance.ctx;
//        ctx.font = Chart.helpers.fontString(Chart.defaults.global.defaultFontSize, Chart.defaults.global.defaultFontStyle, Chart.defaults.global.defaultFontFamily);
//        ctx.textAlign = 'center';
//        ctx.textBaseline = 'bottom';
//
//        this.data.datasets.forEach(function (dataset, i) {
//          var meta = chartInstance.controller.getDatasetMeta(i);
//          meta.data.forEach(function (bar, index) {
//            var data = dataset.data[index];
//            ctx.fillText(data, bar._model.x, bar._model.y - 5);
//          });
//        });
//    }
//    }
  },
});


}


