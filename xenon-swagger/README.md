# Building swagger-ui
1. clone https://github.com/swagger-api/swagger-ui and checkout a stable tag
2. follow instructions for building swagger-ui
3. copy all files produced by the build from dist/ to *ui/com/vmware/xenon/swagger/SwaggerDescriptorService*
4. edit the index.html by adding this lines after "$(function () {"
 
 ```javascript
  var url = window.location.search.match(/url=([^&]+)/);
  if (url && url.length > 1) {
    url = decodeURIComponent(url[1]);
  } else {
    var loc = window.location;
    url = loc.protocol + "//" + loc.host + "/discovery/swagger";
  }
 ```