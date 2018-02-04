package common

import (
	"reflect"
	"strconv"
	"time"
)


func EncodeStringForm(st interface{}) (string, error){
	val := reflect.ValueOf(st)
	ind := reflect.Indirect(val)

	return EncodeStringFormValue(ind)

}


func EncodeStringFormValue(st reflect.Value) (string, error){


	switch st.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(st.Int(), 10), nil
	case reflect.String:
		return st.String(),nil
	case reflect.Ptr:
		ind := reflect.Indirect(st)
		return EncodeStringFormValue(ind)
	case reflect.Interface:
		return EncodeStringFormValue(st.Elem())
	case reflect.Struct:
		if st.Type().Name() == "Time"{
			return MakeDBTimeString(st.Interface().(time.Time)), nil
		}else{
			return "", nil
		}
	default:
		return "", nil
	}
}


func EncodeObjectPacket(st interface{}) ([]byte, error){

	v := reflect.ValueOf(st)
	if v.IsNil() || v.Kind() != reflect.Ptr{
		return nil, ErrorLog("not a pointer")
	}

	buf := NewBuffer()

	err := encodeObjectPacketValue(buf, v.Elem())

	return buf.GetDataBuffer(), err

}



func encodeObjectPacketValue(buf *MBuffer, st reflect.Value) error{

	switch st.Kind(){
	case reflect.Struct:

		typeData := st.Type()

		if typeData.Name() == "Time"{

			if err := buf.WriteString(MakeDBTimeString(st.Interface().(time.Time))); err != nil{
				return err
			}

			return nil
		}

		for ii:=0; ii < st.NumField(); ii++{
			f := st.Field(ii)

			//InfoLog("encoding:", typeData.Field(ii).Name, buf.Info())
			if err := encodeObjectPacketValue(buf, f); err != nil{
				return ErrorLog("encode field:", ii, "failed:", err)
			}
		}
	case reflect.Ptr:
		ind := reflect.Indirect(st)
		if err := encodeObjectPacketValue(buf, ind); err != nil{
			return ErrorLog("encode ptr failed:", st.Type().Name())
		}
	case reflect.Float32:
		if err := buf.WriteFloat32(float32(st.Float())); err != nil{
			return err
		}
	case reflect.Float64:
		if err := buf.WriteFloat64(st.Float()); err != nil{
			return err
		}
	case reflect.Bool:
		if st.Bool(){
			if err := buf.WriteByte(1); err != nil{
				return err
			}
		}else{
			if err := buf.WriteByte(0); err != nil{
				return err
			}
		}
	case reflect.Int64:
		if err := buf.WriteLong(st.Int()); err != nil{
			return err
		}
	case reflect.Int, reflect.Int32:
		if err := buf.WriteInt(int(st.Int())); err != nil{
			return err
		}
	case reflect.String:
		if err := buf.WriteString(st.String()); err != nil{
			return err
		}
	case reflect.Slice:
		switch st.Type().Elem().Kind() {
		default:
			slen := st.Len()
			if err := buf.WriteInt(slen); err != nil{
				return err
			}
			for ii := 0; ii < slen; ii++{
				if err := encodeObjectPacketValue(buf, st.Index(ii)); err != nil{
					return err
				}
			}
		case reflect.Uint8:
			if st.IsNil(){
				buf.WriteUint(uint32(0xffffffff))
			}else{
				bytes := st.Bytes()
				blen := len(bytes)
				buf.WriteUint(uint32(blen))
				buf.WriteBytes(bytes)
			}
		}
	case reflect.Map:
		mlen := st.Len()
		if err := buf.WriteInt(mlen); err != nil{
			return err
		}
		for _, key := range st.MapKeys(){
			if err := encodeObjectPacketValue(buf, key); err != nil{
				return ErrorLog("encode field:", key.Type().Name(), "failed:", err)
			}

			if st.MapIndex(key).Kind() == reflect.Interface{
				return ErrorLog("map value cannot be interface kind")
			}

			if err := encodeObjectPacketValue(buf, st.MapIndex(key) ); err != nil{
				return ErrorLog("encode field:", key.Type().Name(), "failed:", err)
			}
		}
	case reflect.Interface:
		//InfoLog("debug interface type:", st.Elem().Kind())
		if err := encodeObjectPacketValue(buf, st.Elem()); err != nil{
			return ErrorLog("encode field:", st.Type().Name() , "failed:", err)
		}

	}

	return nil
}


func DecodeObjectPacketFromBytes(bts []byte, st interface{}) error {


	buff := NewBufferByBytes(bts)

	return DecodeObjectPacket(buff, st)

}


func DecodeObjectPacket(buf *MBuffer, st interface{}) error {

	v := reflect.ValueOf(st)
	if v.IsNil() || v.Kind() != reflect.Ptr{
		return ErrorLog("not a pointer")
	}


	return DecodeObjectPacketValue(buf, v.Elem())

}



func DecodeObjectPacketValue(buf *MBuffer, st reflect.Value) error{

	typeData := st.Type()

	switch st.Kind(){

	case reflect.Struct:
		len := st.NumField()

		if typeData.Name() == "Time"{

			str, err := buf.ReadString()
			if err != nil{
				return ErrorLog("read time string failed", err)
			}

			tm, err := FromDBTimeString(str)
			if err != nil{
				return ErrorLog("parse time string failed", str)
			}

			st.Set(reflect.ValueOf(tm))

			return nil

		}

		for ii := 0; ii < len; ii++{
			//InfoLog("decodeing:", typeData.Field(ii).Name, " buf:", buf.Info())

			if err := DecodeObjectPacketValue(buf, st.Field(ii)); err != nil{
				return ErrorLog("decode struct failed:", st.Type().Name(), err)
			}
		}
	case reflect.Ptr:
		ind := reflect.Indirect(st)

		newSt := reflect.New(st.Type().Elem())
		newInd := reflect.Indirect(newSt)

		if err := DecodeObjectPacketValue(buf, newInd); err != nil{
			return ErrorLog("decode ptr failed:", ind.Type().Name(), err)
		}
		st.Set(newSt)
	case reflect.Bool:
		bval, err := buf.ReadByte()
		if err != nil{
			return err
		}
		st.SetBool(bval == 1)

	case reflect.Int, reflect.Int32:
		intVal, err := buf.ReadInt()
		if err != nil{
			return err
		}
		st.SetInt(int64(intVal))
	case reflect.Int64:
		intVal, err := buf.ReadLong()
		if err != nil{
			return err
		}
		st.SetInt(intVal)
	case reflect.Float32:
		fltVal, err := buf.ReadFloat32()
		if err != nil{
			return err
		}
		st.SetFloat(float64(fltVal))
	case reflect.Float64:
		fltVal, err := buf.ReadFloat64()
		if err != nil{
			return err
		}
		st.SetFloat(fltVal)
	case reflect.String:
		str, err := buf.ReadString()
		if err != nil{
			return ErrorLog("read string failed", err)
		}
		st.SetString(str)
	case reflect.Slice:
		switch st.Type().Elem().Kind() {
		default:
			len, err := buf.ReadInt()
			if err != nil || len < 0{
				return ErrorLog("read slice len failed:", len, err)
			}

			arr := reflect.MakeSlice(st.Type(), len, len)

			for ii := 0; ii < len; ii++{
				if err := DecodeObjectPacketValue(buf, arr.Index(ii)); err != nil{
					return ErrorLog("decode array item failed:", st.Type().Name(), err)
				}
			}
			st.Set(arr)
		case reflect.Uint8:
			len, err := buf.ReadInt()
			if err != nil{
				return ErrorLog("read uint8 array len failed")
			}

			if len < 0{
				st.SetBytes(nil)
			}else{
				bytes, err := buf.ReadBytes(len)
				if err != nil{
					return ErrorLog("decode uint8 array failed")
				}
				newBytes := make([]byte, len)
				copy(newBytes, bytes)

				st.SetBytes(newBytes)
			}
		}
	case reflect.Map:
		len, err := buf.ReadInt()
		if err != nil || len < 0{
			return ErrorLog("read map len failed:", len, err)
		}


		dic := reflect.MakeMap(st.Type())

		for ii := 0; ii < len; ii++{
			key := reflect.New(st.Type().Key()).Elem()

			if err := DecodeObjectPacketValue(buf, key); err != nil{
				return ErrorLog("decode map key failed:", st.Type().Name(), err)
			}

			value := reflect.New(st.Type().Elem()).Elem()

			if err := DecodeObjectPacketValue(buf, value); err != nil{
				return ErrorLog("decode map value failed:", st.Type().Name(), err)
			}

			dic.SetMapIndex(key, value)
		}

	}
	return nil
}

