package httpserver

import (
	"mime/multipart"
	"bytes"
	"io"
	"MarsXserver/common"
	"net/http"
)

func (this *HttpContext) SendRequestWithFile(url, fileFieldName, fileName string, filedata []byte, params map[string]string) error{

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	fileWriter, err := writer.CreateFormFile(fileFieldName, fileName )
	if err != nil{
		return common.ErrorLog("form create file failed", err)
	}

	fileReader := bytes.NewReader(filedata)

	io.Copy(fileWriter, fileReader)

	for key, val := range params{

		fw, err := writer.CreateFormField(key)
		if err != nil{
			return common.ErrorLog("create field failed:", key, err)
		}

		if _, err := fw.Write([]byte(val)); err != nil{
			return common.ErrorLog("write field value failed, f", key, " val:", val, err)
		}
	}

	writer.Close()

	req, err := http.NewRequest("POST", url, &buf)
	if err != nil{
		return common.ErrorLog("send request failed", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{}

	res, err := client.Do(req)
	if res.StatusCode != http.StatusOK{
		return common.ErrorLog(" send req failed code:", res.StatusCode)
	}

	return nil

}


