package httpserver

import (
	"net/http"
	"html/template"
	"MarsXserver/common"
)

var tpl = `
<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
    <title>beego application error</title>
    <style>
        html, body, body * {padding: 0; margin: 0;}
        #header {background:#ffd; border-bottom:solid 2px #A31515; padding: 20px 10px;}
        #header h2{ }
        #footer {border-top:solid 1px #aaa; padding: 5px 10px; font-size: 12px; color:green;}
        #content {padding: 5px;}
        #content .stack b{ font-size: 13px; color: red;}
        #content .stack pre{padding-left: 10px;}
        table {}
        td.t {text-align: right; padding-right: 5px; color: #888;}
    </style>
    <script type="text/javascript">
    </script>
</head>
<body>
    <div id="header">
        <h2>Error Stack</h2>
    </div>
    <div id="content">
        <table>
            <tr>
                <td class="t">Request Method: </td><td>{{.RequestMethod}}</td>
            </tr>
            <tr>
                <td class="t">Request URL: </td><td>{{.RequestURL}}</td>
            </tr>
            <tr>
                <td class="t">RemoteAddr: </td><td>{{.RemoteAddr }}</td>
            </tr>
        </table>
        <div class="stack">
            <b>Stack</b>
            <pre>{{.Stack}}</pre>
        </div>
    </div>
    <div id="footer">
        <p>try every day</p>
    </div>
</body>
</html>
`
var errTpl = `
<!DOCTYPE html>
<html lang="en">
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
		<title>{{.Title}}</title>
		<style type="text/css">
			* {
				margin:0;
				padding:0;
			}

			body {
				background-color:#EFEFEF;
				font: .9em "Lucida Sans Unicode", "Lucida Grande", sans-serif;
			}

			#wrapper{
				width:600px;
				margin:40px auto 0;
				text-align:center;
				-moz-box-shadow: 5px 5px 10px rgba(0,0,0,0.3);
				-webkit-box-shadow: 5px 5px 10px rgba(0,0,0,0.3);
				box-shadow: 5px 5px 10px rgba(0,0,0,0.3);
			}

			#wrapper h1{
				color:#FFF;
				text-align:center;
				margin-bottom:20px;
			}

			#wrapper a{
				display:block;
				font-size:.9em;
				padding-top:20px;
				color:#FFF;
				text-decoration:none;
				text-align:center;
			}

			#container {
				width:600px;
				padding-bottom:15px;
				background-color:#FFFFFF;
			}

			.navtop{
				height:40px;
				background-color:#24B2EB;
				padding:13px;
			}

			.content {
				padding:10px 10px 25px;
				background: #FFFFFF;
				margin:;
				color:#333;
			}

			a.button{
				color:white;
				padding:15px 20px;
				text-shadow:1px 1px 0 #00A5FF;
				font-weight:bold;
				text-align:center;
				border:1px solid #24B2EB;
				margin:0px 200px;
				clear:both;
				background-color: #24B2EB;
				border-radius:100px;
				-moz-border-radius:100px;
				-webkit-border-radius:100px;
			}

			a.button:hover{
				text-decoration:none;
				background-color: #24B2EB;
			}

		</style>
	</head>
	<body>
		<div id="wrapper">
			<div id="container">
				<div class="navtop">
					<h1>{{.Title}}</h1>
				</div>
				<div id="content">
					{{.Content}}
					<a href="/" title="Home" class="button">Go Home</a><br />

				</div>
			</div>
		</div>
	</body>
</html>
`


func SendError(err error, rw http.ResponseWriter, r *http.Request){

	t, _ := template.New("error_statck_page").Parse(tpl)
	data := make(map[string]string)

	data["RequestMethod"] = r.Method
	data["RequestURL"] = r.RequestURI
	data["RequestAddr"] = r.RemoteAddr
	data["Stack"] = common.GetErrorStack()
	t.Execute(rw, data)
}






//404
func NotFound(rw http.ResponseWriter, r *http.Request) {
	t, _ := template.New("beegoerrortemp").Parse(errTpl)
	data := make(map[string]interface{})
	data["Title"] = "Page Not Found"
	data["Content"] = template.HTML("<br>The Page You have requested flown the coop." +
		"<br>Perhaps you are here because:" +
		"<br><br><ul>" +
		"<br>The page has moved" +
		"<br>The page no longer exists" +
		"<br>You were looking for your puppy and got lost" +
		"<br>You like 404 pages" +
		"</ul>")
	rw.WriteHeader(http.StatusNotFound)
	t.Execute(rw, data)
}

//401
func Unauthorized(rw http.ResponseWriter, r *http.Request) {
	t, _ := template.New("beegoerrortemp").Parse(errTpl)
	data := make(map[string]interface{})
	data["Title"] = "Unauthorized"
	data["Content"] = template.HTML("<br>The Page You have requested can't authorized." +
		"<br>Perhaps you are here because:" +
		"<br><br><ul>" +
		"<br>Contains the credentials that you supplied" +
		"<br>Contains the address for errors" +
		"</ul>")
	rw.WriteHeader(http.StatusUnauthorized)
	t.Execute(rw, data)
}

//403
func Forbidden(rw http.ResponseWriter, r *http.Request) {
	t, _ := template.New("beegoerrortemp").Parse(errTpl)
	data := make(map[string]interface{})
	data["Title"] = "Forbidden"
	data["Content"] = template.HTML("<br>The Page You have requested forbidden." +
		"<br>Perhaps you are here because:" +
		"<br><br><ul>" +
		"<br>Your address may be blocked" +
		"<br>The site may be disabled" +
		"<br>You need to log in" +
		"</ul>")
	rw.WriteHeader(http.StatusForbidden)
	t.Execute(rw, data)
}

//503
func ServiceUnavailable(rw http.ResponseWriter, r *http.Request) {
	t, _ := template.New("beegoerrortemp").Parse(errTpl)
	data := make(map[string]interface{})
	data["Title"] = "Service Unavailable"
	data["Content"] = template.HTML("<br>The Page You have requested unavailable." +
		"<br>Perhaps you are here because:" +
		"<br><br><ul>" +
		"<br><br>The page is overloaded" +
		"<br>Please try again later." +
		"</ul>")
	rw.WriteHeader(http.StatusServiceUnavailable)
	t.Execute(rw, data)
}

//500
func InternalServerError(rw http.ResponseWriter, r *http.Request) {
	t, _ := template.New("beegoerrortemp").Parse(errTpl)
	data := make(map[string]interface{})
	data["Title"] = "Internal Server Error"
	data["Content"] = template.HTML("<br>The Page You have requested has down now." +
		"<br><br><ul>" +
		"<br>simply try again later" +
		"<br>you should report the fault to the website administrator" +
		"</ul>")
	rw.WriteHeader(http.StatusInternalServerError)
	t.Execute(rw, data)
}




























