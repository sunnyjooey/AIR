// define case insensitive function
var searchTerm, panelContainerId;
$.expr[":"].containsCaseInsensitive = function(n, i, m) {
  return jQuery(n).text().toUpperCase().indexOf(m[3].toUpperCase()) >= 0;
};

// selected variable list
var selectedVars = [];

// function: show or hide depending on search word
$("#mainTool-search_search").click(function() {
  var searchWordCount = 0;
  // initialize
  let searchTerm = $.trim($("#mainTool-search_text").val().toLowerCase());
  // reset things to begin
  $(".panel").each(function() {
    // hide all the outter and inner panel classes to begin
    $(this).hide();
    // close all the panels to begin
    let idToClose = "#" + this.id + "-collapse";
  });
  
  // indvidual variable: span contains variable name and its description
  $("#mainTool-level_1").find("span").each(function(i, sp) {
    // this is just the variable name
    let spanText = $(sp).contents().filter(function() {
      return this.nodeType == Node.TEXT_NODE;
    }).get(0).nodeValue.toLowerCase();
    // this is just the content (unbolded text) of details
    let detailsText = $(sp).find(".well").clone().children().remove().end().text().toLowerCase();
    if (spanText.indexOf(searchTerm) >= 0 | detailsText.indexOf(searchTerm) >= 0 ) {
      // show only the ones with the search term
      searchWordCount ++;
      $(sp).show(); //varname and checkbox
      $(sp).parent().show(); //this includes the checkbox and span
      $(sp).parents(".panel").each(function(){
        $(this).show();//this includes inner and outter panels
      });
    } else {
      $(sp).parent().hide();
    }
  });
  Shiny.setInputValue("mainTool-varWordCount", searchWordCount);
  Shiny.setInputValue("mainTool-searchWord", searchTerm);
});

// click search when enter-key pressed
$("#mainTool-search_text").keypress(function(e){
    if(e.which == 13){//Enter key pressed
        $("#mainTool-search_search").click();
    }
});

// reset to show original accordion  
$("#mainTool-search_reset").click(function() {
  // empty search box
  $("#mainTool-search_text").val("");
  $("#mainTool-search_search").click();
});

// check and uncheck variable in accordion
$("input[type=checkbox]").on("click", function() {
  if($(this).prop("checked")===true){
    selectedVars.push(this.value);
    Shiny.setInputValue("mainTool-selVars", selectedVars);
  } 
  if($(this).prop("checked")===false){
    v = this.value;
    selectedVars = selectedVars.filter(function(item) {
      return item !== v;
    });
    $("#"+v).parent().remove();
    Shiny.setInputValue("mainTool-selVars", selectedVars);
  }
});

// when reset button is clicked, selectedVars is emptied
$(function() {
  Shiny.addCustomMessageHandler("reSet", function(message) {
    if (message == "reset") {
      selectedVars = selectedVars.filter(function(item) {
        return item === "";
      });
      // delete accordion and search box
      $("#mainTool-variableSelectionAccordion").empty();
      // send empty list of vars
      Shiny.setInputValue("mainTool-selVars", selectedVars);
      // empty search box
      $("#mainTool-search_text").val("");
    }
  });
});

