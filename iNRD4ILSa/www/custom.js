// custom.js
// Given an element id, tell Shiny what the input type of that element is
/*shinyjs.getInputType = function(params) {
  params = shinyjs.getParams(params, {
    id : null,
    shinyUpdateInputId : null
  });
  var id = params.id;

  // Escape characterss that have special selector meaning in jQuery
  id = id.replace( /(:|\.|\[|\]|,)/g, "\\$1" );
  var $el = $('#' + id);
  // find the enclosing shiny input container for the given id
  if (!$el.hasClass('shiny-input-container')) {
    $el = $el.closest('.shiny-input-container');
    if (!$el.length) {    
      console.log('Could not find Shiny input element for id \"' + id + '\"');
      return;
    }
  }
  // find the input type of the element
  var inputType = $el.data('inputType');
  if (!inputType) {
    console.log('Could not find Shiny input type for id \"' + id + '\"');
    return;
  }
  // tell Shiny what input type this element is
  Shiny.onInputChange(params.shinyUpdateInputId, inputType);
};*/

console.log("Working");

/* Parse url to go to the right tab */
Shiny.addCustomMessageHandler('updateSelections',
                        function(data) {
                        var tab_ref = 'a[data-value=' + data.tab +']';
                        $(tab_ref).trigger('click');
                        });
  


/* Get plot width and height */
/*var dimension = 0;
$(document).on("shiny:connected", function(e) {
dimension = window.innerWidth;
Shiny.onInputChange("dimension", dimension);
});
$(window).resize(function(e) {
dimension = window.innerWidth;
Shiny.onInputChange("dimension", dimension);
});*/

/*Stickyfill.add($("#nationalTrendMean-distPlot > img"));
*/
/*
$(".option").on("mouseover", function(e){
  console.log("hello");
    $(this).tooltip({
        content: function(callback) {
         	callback("hello");   
        },
        items: '*'
    }).tooltip("show");
}, function() {           
    $(this).tooltip('destroy');
});

/*$(".option").each(function() {
  console.log("hello");
}
);*/


