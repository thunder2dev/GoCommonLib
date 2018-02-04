package common

import (
	"fmt"
	"crypto/rand"
)

type UUID [16]byte



func (this UUID) String() string{

	x := [16]byte(this)

	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		x[0], x[1], x[2], x[3], x[4],
		x[5], x[6],
		x[7], x[8],
		x[9], x[10], x[11], x[12], x[13], x[14], x[15])


}

func NewUUID() (u UUID, err error){
	u = UUID{}
	err = randBytes(u[:])
	return
}

func randBytes(x []byte) error{

	if _, err := rand.Read(x); err != nil{
		ErrorLog("rand read failed", err)
		return err
	}

	x[6] = (x[6] & 0x0F) | 0x40
	x[8] = (x[8] & 0x3F) | 0x80
	return nil
}
