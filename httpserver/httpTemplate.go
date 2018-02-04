package httpserver

import (
	"html/template"
	"path/filepath"
	"os"
	"regexp"
	"MarsXserver/common"
	"io/ioutil"
)

const (
	DefaultTemplateExt = ".html"
)


type HttpTemplateMgr struct{

	rootDir string
	templates map[string]*template.Template

}

func NewHttpTemplateMgr(_rootDir string) *HttpTemplateMgr{

	return &HttpTemplateMgr{
		rootDir:_rootDir,
		templates:make(map[string]*template.Template),
	}

}

func (this *HttpTemplateMgr) InitTemplates(){

	allfiles := make(map[string][]string)

	absRootDir, err := filepath.Abs(this.rootDir)
	if err != nil{
		common.ErrorLog("get abs path failed:", this.rootDir)
		return
	}

	err = filepath.Walk(absRootDir, func(path string, info os.FileInfo, err error) error{

		if err != nil{
			common.ErrorLog("walk path failed:", err)
		}

		if info.IsDir(){
			return nil
		}

		dir := filepath.Dir(path)
		base := filepath.Base(path)

		if filepath.Ext(base) != DefaultTemplateExt{
			return nil
		}

		if _, ok := allfiles[dir]; ok == true{

			allfiles[dir] = append(allfiles[dir], base)

		}else{

			allfiles[dir] = make([]string, 1)
			allfiles[dir][0] = base
		}
		return nil
	})

	if err != nil{
		common.ErrorLog("walk path failed", err)
		return
	}

	reg, err := regexp.Compile("{{" + "[ ]*template[ ]+\"([^\"]+)\"")
	if err != nil{
		common.FatalLog("compile regex failed")
		return
	}


	for dir, files := range allfiles {

		for _, file := range files{

			if _, ok := this.templates[file]; ok == true{
				continue
			}

			this.templates[file], err = this.readInTemplateDps(reg, allfiles, dir, file, nil)
			if err != nil{
				common.FatalLog("build template failed", dir, file, err)
				return
			}
		}
	}

}


func (this *HttpTemplateMgr) readInTemplateDps(reg *regexp.Regexp, allfiles map[string][]string, dir, file string, t *template.Template) (tp *template.Template, err error){

	dirParts := filepath.SplitList(dir)

	relpath := filepath.Join(dir, file)

	abspath, err := filepath.Abs(relpath)
	if err != nil{
		common.ErrorLog("get abs file path failed")
		return
	}

	data, err := ioutil.ReadFile(abspath)
	if err != nil{
		return nil, common.ErrorLog("get file content failed")

	}

	if t == nil{
		if tp, err = template.New(file).Parse(string(data)); err != nil{
			return nil, common.ErrorLog("parse template failed", err)
		}
	}else{
		if tp,err = t.Parse(string(data)); err != nil{
			return nil, common.ErrorLog("pasre template failed", err)
		}
	}


	strs := reg.FindAllStringSubmatch(string(data), -1)

	for _, str := range strs{

		usedFilePath := str[1]

		usedFileName := filepath.Base(usedFilePath)

		usedParts := filepath.SplitList(usedFilePath)

		parentCnt := 0

		for _, part := range usedParts{
			if part == ".."{
				parentCnt ++
			}
		}

		namedParentPath := filepath.Join(dirParts[0: len(dirParts) - parentCnt]...)
		namedUsedPath := filepath.Join(usedParts[parentCnt:]...)
		fullpath := filepath.Join(namedParentPath, namedUsedPath)

		fullDirPath := filepath.Dir(fullpath)

		if tp.Lookup(usedFileName) != nil {
			continue
		}else {

			if _, ok := allfiles[fullDirPath]; ok == false{
				return t, common.ErrorLog("dir is error", fullDirPath, err)
			}

			this.readInTemplateDps(reg, allfiles, fullDirPath, usedFileName, tp)
			continue
		}
	}

	return tp, nil
}





func (this *HttpTemplateMgr) GetTemplate(name string) (tpl *template.Template, err error){


	tpl, ok := this.templates[name]
	if ok == false{

		return nil, common.ErrorLog("template is missing:", name)
	}


	return
}























