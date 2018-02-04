package httpserver

import (
	"sync"
	"net/http"
	"fmt"
	"io"
	"crypto/rand"
	"time"
	"crypto/sha1"
	"encoding/hex"
	"container/list"
	"net/url"
	"MarsXserver/common"
)


const (
	SESSION_MAX_LIFE_TIME time.Duration = time.Minute * 60
)

type SessionManager struct{

	cookieName string

	sessions map[string]*list.Element

	sessList *list.List

	lock sync.RWMutex

}

func NewSessionManager(_cookieName string) *SessionManager{

	sessMgr := &SessionManager{
		cookieName: _cookieName,
		sessions: make(map[string]*list.Element),
		sessList: list.New(),
	}

	go sessMgr.SessionGC()
	return sessMgr

}

func (this *SessionManager) SessionStart(w http.ResponseWriter, r *http.Request) (session *SessionStore){

	this.lock.Lock()
	defer this.lock.Unlock()

	cookie, err := r.Cookie(this.cookieName)

	if err == nil && len(cookie.Value) > 0{

		sid, err := url.QueryUnescape(cookie.Value)
		if err != nil{
			common.ErrorLog("query unescape failed:", err)
		}else{
			common.InfoLog("find old cookie sid:", sid)
			sess, ok := this.sessions[sid]

			if ok{
				common.InfoLog("got Session, id:", sid)
				sess.Value.(*SessionStore).lastAccessedTime = common.GetTimeNow()
				this.sessList.MoveToFront(sess)
				return sess.Value.(*SessionStore)

			}
		}
	}

	sid := this.SessionId(r)

	common.InfoLog("new Session, id:", sid, url.QueryEscape(sid))

	newCookie := &http.Cookie{
		Name: this.cookieName,
		Value:    url.QueryEscape(sid),
		Path:     "/",
		HttpOnly: true,
		Secure:   false,
	}

	http.SetCookie(w, newCookie)
	common.InfoLog("cookie str:", newCookie.String(), sid)

	r.AddCookie(newCookie)


	sess := this.sessList.PushBack(NewSession(sid))
	this.sessions[sid] = sess

	return sess.Value.(*SessionStore)

}

func (this *SessionManager) SessionRelease(){

}

func (this *SessionManager) SessionId(r *http.Request) (sid string){

	randBytes := make([]byte, 20)
	io.ReadFull(rand.Reader, randBytes)

	sKey := fmt.Sprintf("%s%s%d", r.RemoteAddr, randBytes, common.GetTimeNow().Nanosecond())

	sha1 := sha1.New()
	sha1.Write([]byte(sKey))
	sid = hex.EncodeToString(sha1.Sum(nil))

	return
}


func (this *SessionManager) SessionGC(){

	this.lock.Lock()
	defer this.lock.Unlock()

	common.InfoLog("Session gc is running")

	for{
		common.InfoLog("session gc once")

		ele := this.sessList.Back()

		if ele == nil{
			break
		}

		common.InfoLog("session:", ele.Value.(*SessionStore).sid, " time:", ele.Value.(*SessionStore).lastAccessedTime.Unix(), " diff:", common.GetTimeNow().Unix() - ele.Value.(*SessionStore).lastAccessedTime.Unix())

		if ele.Value.(*SessionStore).lastAccessedTime.Add(SESSION_MAX_LIFE_TIME).Unix() < common.GetTimeNow().Unix(){

			common.InfoLog("remove session", ele.Value.(*SessionStore).sid)
			this.sessList.Remove(ele)
			delete(this.sessions, ele.Value.(*SessionStore).sid)

		}else{
			break
		}
	}

	time.AfterFunc(SESSION_MAX_LIFE_TIME, func(){ this.SessionGC()})

}


















