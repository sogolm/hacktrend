/// <autosync enabled="true" />
/// <reference path="modernizr-2.6.2.js" />
/// <reference path="jquery-1.10.2.js" />
/// <reference path="bootstrap.js" />
/// <reference path="respond.js" />
/// <reference path="WinJS.js" />
/// <reference path="base.js" />
/// <reference path="ui.js" />
!function () {
    "use strict"

    var QueryInputClass = "#search-query-input";
    var QuerySubmitClass = "#search-query-submit";
    var baseUrl = "http://ec2-52-0-219-222.compute-1.amazonaws.com/api/plot"
    var data = '<div><a href="https://plot.ly/~sogolm/59/" target="_blank" title="" style="display: block; text-align: center;"><img src="https://plot.ly/~sogolm/59.png" alt="" style="max-width: 100%;" onerror="this.onerror=null;" /></a><script data-plotly="sogolm:59" src="https://plot.ly/embed.js" async></script></div>'

    var $querySubmit = $(QuerySubmitClass);
    $querySubmit.on("click", function () {
        // Hide the search trends & results panels.
        $("#search-trends-panel").fadeTo("fast", 0, function () {

        });
        $("#search-results-panel").fadeTo("fast", 0);

        // Kick off the request to the server, retrieving the plots and results.
        var queryTerm = $("#search-query-input").val();
        WinJS.xhr({
            url: baseUrl + "/" + queryTerm,
        }).then(function (theXhr) {
            var resultsJson = theXhr.response;
            try {
                var results = JSON.parse(resultsJson);
            } catch (e) {
                // TODO: Handle JSON parsing errors
                return;
            }

            // Force a re-draw of the results table
            $("#search-trends-panel").html("");
            $("#search-trends-panel").html(data).fadeTo("slow", 1);

            // The results are a JSON dictionary from date string
            // to an array of URLs, so we can create 
            var $resultsTable = $("#search-results-container");
            $resultsTable.html(""); // reset results content
            Object.keys(results).forEach(function (key) {
                if (!results.hasOwnProperty(key)) {
                    return;
                }

                var month = key;
                var articles = results[month];

                var $monthHeader = $("<h4>").text(month);
                var $monthTable = $("<table>").addClass("table"); // style as a Bootstrap table
                articles.forEach(function (article) {
                    var $articleRow = $("<tr>");
                    var $articleDate = $("<td>").text(article[1]);
                    var $articleUrl = $("<td>");
                    var $articleLink = $("<a>").attr({ "href": article[2] }).text(article[2]).appendTo($articleUrl);
                    $articleRow.append($articleDate);
                    $articleRow.append($articleUrl);
                    $monthTable.append($articleRow)
                });

                $resultsTable.append($monthHeader);
                $resultsTable.append($monthTable);
            });

            $("#search-results-panel").fadeTo("slow", 1);
        });
    });
}();