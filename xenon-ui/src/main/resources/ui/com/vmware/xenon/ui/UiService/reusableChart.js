d3.custom = {};

d3.custom.barChart = function module() {
    var margin = {top: 20, right: 20, bottom: 40, left: 40},
        width = 500,
        height = 500,
        gap = 0,
        ease = 'cubic-in-out';
    var svg, duration = 500;

    var dispatch = d3.dispatch('customHover');
    function exports(_selection) {
        _selection.each(function(_data) {

            var chartW = width - margin.left - margin.right,
                chartH = height - margin.top - margin.bottom;

            var x1 = d3.scale.ordinal()
                .domain(_data.map(function(d, i){ return i; }))
                .rangeRoundBands([0, chartW], .1);

            var y1 = d3.scale.linear()
                .domain([0, d3.max(_data, function(d, i){ return d; })])
                .range([chartH, 0]);

            var xAxis = d3.svg.axis()
                .scale(x1)
                .orient('bottom');

            var yAxis = d3.svg.axis()
                .scale(y1)
                .orient('left');

            var barW = chartW / _data.length;

            if(!svg) {
                svg = d3.select(this)
                    .append('svg')
                    .classed('chart', true);
                var container = svg.append('g').classed('container-group', true);
                container.append('g').classed('chart-group', true);
                container.append('g').classed('x-axis-group axis', true);
                container.append('g').classed('y-axis-group axis', true);
            }

            svg.transition().duration(duration).attr({width: width, height: height})
            svg.select('.container-group')
                .attr({transform: 'translate(' + margin.left + ',' + margin.top + ')'});

            svg.select('.x-axis-group.axis')
                .transition()
                .duration(duration)
                .ease(ease)
                .attr({transform: 'translate(0,' + (chartH) + ')'})
                .call(xAxis);

            svg.select('.y-axis-group.axis')
                .transition()
                .duration(duration)
                .ease(ease)
                .call(yAxis);

            var gapSize = x1.rangeBand() / 100 * gap;
            var barW = x1.rangeBand() - gapSize;
            var bars = svg.select('.chart-group')
                .selectAll('.bar')
                .data(_data);
            bars.enter().append('rect')
                .classed('bar', true)
                .attr({x: chartW,
                    width: barW,
                    y: function(d, i) { return y1(d); },
                    height: function(d, i) { return chartH - y1(d); }
                })
                .on('mouseover', dispatch.customHover);
            bars.transition()
                .duration(duration)
                .ease(ease)
                .attr({
                    width: barW,
                    x: function(d, i) { return x1(i) + gapSize/2; },
                    y: function(d, i) { return y1(d); },
                    height: function(d, i) { return chartH - y1(d); }
                });
            bars.exit().transition().style({opacity: 0}).remove();

            duration = 500;

        });
    }

    exports.width = function(_x) {
        if (!arguments.length) return width;
        width = parseInt(_x);
        return this;
    };
    exports.height = function(_x) {
        if (!arguments.length) return height;
        height = parseInt(_x);
        duration = 0;
        return this;
    };
    exports.gap = function(_x) {
        if (!arguments.length) return gap;
        gap = _x;
        return this;
    };
    exports.ease = function(_x) {
        if (!arguments.length) return ease;
        ease = _x;
        return this;
    };
    d3.rebind(exports, dispatch, 'on');
    return exports;
};

d3.custom.timelineChart = function module(_selection){
    var svg, duration = 500;
    var m = [20, 15, 15, 180, 50], //top right bottom left
        w = 1024 - m[1] - m[3],
        h, miniHeight, mainHeight;

    function exports (_selection) {
        var _chartData = _selection[0][0].__data__;
        h = (_chartData.laneLength * 45) + (_chartData.laneLength * 22) + 85;
        miniHeight = _chartData.laneLength * 12 + 50;
        mainHeight = h - miniHeight - 50,
        minDate = d3.min(_chartData.items, function(d) { return convertEpochToDate(d.start); }),
        maxDate = d3.max(_chartData.items, function(d) { return convertEpochToDate(d.start + 5000);});

        $('#startTime').text(minDate);
        $('#endTime').text(maxDate);

        var x = d3.scale.linear()
            .domain([minDate, maxDate])
            .range([0, w]);
        var x1 = d3.scale.linear()
            .range([0, w]);
        var y1 = d3.scale.linear()
            .domain([0,_chartData.laneLength])
            .range([0, mainHeight]);
        var y2 = d3.scale.linear()
            .domain([0, _chartData.laneLength])
            .range([0, miniHeight]);

        //If a SVG is a already drawn remove it - needed for when data updates
        if( $('.timelineChartSvg').length ) {
            $('.timelineChartSvg').remove();
        }

        var chart = d3.select(".timelineChart")
            .append("svg")
            .attr("width", w + m[1] + m[3])
            .attr("height", h + m[0] + m[2])
            .attr("class", "timelineChartSvg");




        chart.append("defs").append("clipPath")
            .attr("id", "clip")
            .append("rect")
            .attr("width", w)
            .attr("height", mainHeight);

        var mini = chart.append("g")
            .attr("transform", "translate(" + m[3] + "," + m[0] + ")")
            .attr("width", w)
            .attr("height", miniHeight)
            .attr("class", "mini");

        var main = chart.append("g")
            .attr("transform", "translate(" + m[3] + "," + (miniHeight + m[0]) + ")")
            .attr("width", w)
            .attr("height", mainHeight)
            .attr("class", "main");

        var xDateAxis = d3.svg.axis()
            .scale(x)
            .orient('bottom')
            .ticks(d3.time.mondays, (x.domain()[1] - x.domain()[0]) > 15552e6 ? 2 : 1)
            .tickFormat(d3.time.format('%d'))
            .tickSize(6, 0, 0);

        var x1DateAxis = d3.svg.axis()
            .scale(x1)
            .orient('bottom')
            .ticks(d3.time.days, 1)
            .tickFormat(d3.time.format('%a %d'))
            .tickSize(6, 0, 0);

        var xMonthAxis = d3.svg.axis()
            .scale(x)
            .orient('top')
            .ticks(d3.time.months, 1)
            .tickFormat(d3.time.format('%b %Y'))
            .tickSize(15, 0, 0);

        var x1MonthAxis = d3.svg.axis()
            .scale(x1)
            .orient('top')
            .ticks(d3.time.mondays, 1)
            .tickFormat(d3.time.format('%b - Week %W'))
            .tickSize(15, 0, 0);

        // main lanes and labels
        main.append("g").selectAll(".laneLines")
            .data(_chartData.items)
            .enter().append("line")
            .attr("x1", 0)
            .attr("y1", function(d) {return y1(d.lane);})
            .attr("x2", w)
            .attr("y2", function(d) {return y1(d.lane);})
            .attr("stroke", "#ccc");

        main.append("g").selectAll(".laneText")
            .data(_chartData.lanes)
            .enter().append("text")
            .text(function(d) {return d;})
            .each(wrap)
            .attr("x", -m[1])
            .attr("y", function(d, i) {return y1(i + .5);})
            .attr("dy", ".5ex")
            .attr("text-anchor", "end")
            .attr("class", "laneText");

        // mini lanes and labels
        mini.append("g").selectAll(".laneLines")
            .data(_chartData.items)
            .enter().append("line")
            .attr("x1", 0)
            .attr("y1", function(d) {return y2(d.lane);})
            .attr("x2", w)
            .attr("y2", function(d) {return y2(d.lane);})
            .attr("stroke", "#ccc");

        mini.append("g").selectAll(".laneText")
            .data(_chartData.lanes)
            .enter().append("text")
            .text(function(d) {return d;})
            .each(wrap)
            .attr("x", -m[1])
            .attr("y", function(d, i) {return y2(i + .5);})
            .attr("dy", ".5ex")
            .attr("text-anchor", "end")
            .attr("class", "laneTextMini");

        var itemDisks = main.append("g")
            .attr("clip-path", "url(#clip)");

        // mini item disks
        mini.append("g").selectAll("miniItems")
            .data(_chartData.items)
            .enter().append("circle")
            .attr("class", "timebaritem")
            .style('opacity', 1)
            .style('fill', "#3EC9B6")
            .attr('cx', function (d) {
                return x(d.start);
            })
            .attr('cy', function (d) {
                return y2(d.lane + .5);
            })
            .attr('r', 2);

        main.append('g')
            .attr('transform', 'translate(0,' + mainHeight + ')')
            .attr('class', 'main axis date')
            .call(x1DateAxis);

        main.append('g')
            .attr('transform', 'translate(0,0.5)')
            .attr('class', 'main axis month')
            .call(x1MonthAxis)
            .selectAll('text')
            .attr('dx', 5)
            .attr('dy', 12);

        mini.append('g')
            .attr('transform', 'translate(0,' + miniHeight + ')')
            .attr('class', 'axis date')
            .call(xDateAxis);

        mini.append('g')
            .attr('transform', 'translate(0,0.5)')
            .attr('class', 'axis month')
            .call(xMonthAxis)
            .selectAll('text')
            .attr('dx', 5)
            .attr('dy', 12);

        //wrap label text when too long
        function wrap() {
            var self = d3.select(this),
                textLength = self.node().getComputedTextLength(),
                text = self.text();
            while (textLength > (m[3] - 2 * m[1]) && text.length > 0) {
                text = text.slice(0, -1);
                self.text(text + '...');
                textLength = self.node().getComputedTextLength();
            }
        }

        function convertEpochToDate(epoch) {
            // create a new javascript Date object based on the timestamp
            // multiplied by 1000 so that the argument is in milliseconds, not seconds
            var dt = new Date(epoch);
            dt = new Date(dt.getFullYear(), dt.getMonth(), dt.getDate(), dt.getHours(), dt.getMinutes(), dt.getSeconds());
            return dt;
        }

        // brush
        var brush = d3.svg.brush()
            .x(x)
            .on("brush", display);

        mini.append("g")
            .attr("class", "x brush")
            .call(brush)
            .selectAll("rect")
            .attr("y", 1)
            .attr("height", miniHeight - 1);

        // tooltip
        var div = d3.select("body").append("div")
            .attr("class", "tooltip")
            .style("opacity", 0);
        display();

        function display() {
            var disks, labels,
                minExtent = brush.extent()[0],
                maxExtent = brush.extent()[1],
                visItems = _chartData.items.filter(function (d) {
                    return d.start < maxExtent && d.start > minExtent;
                });

            mini.select(".brush")
                .call(brush.extent([minExtent, maxExtent]));

            x1.domain([minExtent, maxExtent]);

            //update main item disks
            disks = itemDisks.selectAll("circle")
                .data(visItems, function (d) {
                    return d.id;
                })
                .attr("cx", function (d) {
                    return x1(d.start);
                })
                .attr("cy", function (d) {
                    return y1(d.lane + .5) - 5;
                })
                .attr("r", 4)
                .on("mouseover", function(d) {
                    div.transition()
                        .duration(200)
                        .style("opacity", .9);
                    div .html(
                        "<p class='strong'>" + d.id + "</p>"
                        + "<p>"
                        + "<span class='strong'>Action: </span><span>" + d.action + "</span></br>"
                        + "<span class='strong'>Body: </span><span>" + d.jsonBody + "</span></br>"
                        + "<span class='strong'>Referrer: </span><span>" + d.referer + "</span></br>"
                        + "<span class='strong'>Destination: </span><span>" + d.path + "</span></br>"
                        + "<span class='strong'>Context Id: </span><span>" + d.contextId + "</span></br>"
                        + "<span class='strong'>Date: </span><span>" + convertEpochToDate(d.start) + "</span></br>"
                        + "</p>"
                    )
                        .style("left", (d3.event.pageX + 10) + "px")
                        .style("top", (d3.event.pageY - 20) + "px");
                })
                .on("mouseout", function(d) {
                    div.transition()
                        .duration(500)
                        .style("opacity", 0);
                });;

            disks.enter().append("circle")
                .attr("class", function (d) {
                    return "miniItem" + d.lane;
                })
                .style('opacity', 1)
                .style('fill', "#3EC9B6")
                .attr('cx', function (d) {
                    return x1(d.start);
                })
                .attr('cy', function (d) {
                    return y1(d.lane) + 15;
                })
                .attr('r', 4);

            disks.exit().remove();
        }
    }

    return exports;
};