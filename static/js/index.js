function get_multi(name) {
    var selected = [];
    for (var option of document.getElementById(name).options) {
        if (option.selected) {
            selected.push(option.value);
        }
    }
    return selected.join(',');
}

function get_stations() {
    var years = get_multi("years");
    var names = document.getElementById("names");
    names.options.length = 0;
    data = {}
    fetch("/station_list?year=" + years)
        .then(function (response) {
            return response.json();
        })
        .then(function (data) {
            for (var i = 0; i < data.length; i++) {
                var option = document.createElement("option");
                option.value = data[i].toString();
                option.text = data[i];
                names.appendChild(option);
            }
        })
        .catch(function (err) {
            console.log('error: ' + err);
        });
}
function generate() {
    var year = get_multi("years");
    var station = get_multi("names");
    var meas = get_multi("meas");
    var format = document.getElementById("format").value;
    window.location = "/download?year=" + year + "&station=" + station + "&meas=" + meas + "&format=" + format;
    var citation = document.getElementById("link_anchor");
    var citation_box = document.getElementById("citation_box");
    fetch("/citation?year=" + year)
        .then(function (response) {
            return response.text();
        })
        .then(function (response) {
            citation_box.style.display = "inline-block";
            citation_box.innerHTML = "<caption><b>Recommended Citation</b></caption><p>" + response + "</p>";
        })
        .catch(function (err) {
            console.log('error: ' + err);
        });
}
