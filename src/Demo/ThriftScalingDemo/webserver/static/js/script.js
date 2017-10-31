$(document).ready(function() {
    var lastTime = 0;
    var lastNumTuples = 0;
    var lastNumCores = 0;
    chart = new Highcharts.Chart({
        chart: {
            renderTo: 'container',
            defaultSeriesType: 'spline',
            events: {
                load: requestData
            }
        },
        title: {
            text: 'Live Throughput data of Elasticbolt'
        },
        xAxis: [
            {
                type: 'datetime',
                tickPixelInterval: 150,
                maxZoom: 20 * 1000
            },
            {
                type: 'datetime',
                tickPixelInterval: 150,
                maxZoom: 20 * 1000
            }
        ],
        yAxis: [
            {
                min: 10, 
                max: 100,
                minPadding: 0.2,  
                maxPadding: 0.2,
                title: {
                    text: 'Throughput'
                }
            },
            {
                min: 0, 
                max: 9,
                minPadding: 0.2,  
                maxPadding: 0.2,
                title: {
                    text: 'NumCore'
                },
                opposite: true
            }
        ],
        series: [
            {
                name: 'Elasticbolt Throughput',
                data: [],

            },
            {
                name: 'Elasticbolt NumCore',
                data: [],
                yAxis: 1
            }
        ]
    }); 
    
    /**
     * Request data from the server, add it to the graph and set a timeout 
     * to request again
     */
    function requestData() {
        $.ajax({
            url: 'getData',
            success: function(point) {
                var data = JSON.parse(point);

                for (i = 0; i < data.length; i++) { 
                    if (lastTime == 0) {
                        lastTime = data[i]["time"];
                        lastNumTuples = data[i]["inqueueSize"]
                        lastNumCores = data[i]["numCore"]
                        continue;
                    } else {
                        var timeDelta = data[i]["time"] - lastTime;
                        console.log(timeDelta/1000, lastNumTuples);
                        var perTupleTime = (timeDelta/1000)/lastNumTuples
                        var thruput = Math.pow(perTupleTime,-1)
                        var newTime = (new Date()).getTime()
                        var numCore = lastNumCores*1

                        lastTime = data[i]["time"];
                        lastNumTuples = data[i]["inqueueSize"]
                        lastNumCores = data[i]["numCore"]

                        var series = chart.series[0],
                        shift = series.data.length > 50; // shift if the series is 
                                                         // longer than 20
                        var series1 = chart.series[1],
                        shift1 = series1.data.length > 50; // shift if the series is 
                                                         // longer than 20
                        // add the point
                        // console.log(thruput, numCore)
                        chart.series[0].addPoint([newTime, thruput], true, shift);
                        chart.series[1].addPoint([newTime, numCore], true, shift1);
                    }
                }
                
                
                // call it again after one second
                setTimeout(requestData, 1000);    
            },
            cache: false
        });
    } 

    // requestData();  
});

