function query_redirect() {
    var query_type_box = document.getElementById("query_type");
    var selectedValue = query_type_box.options[query_type_box.selectedIndex].value;
    window.location = "/query?type=" + selectedValue;
}
function displayLoading() {
    var query_type_box = document.getElementById("loading_box");
    loading_box.style.display = "table";
}
