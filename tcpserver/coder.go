package tcpserver

import (
	"github.com/golang/protobuf/proto"
	"MarsXserver/common"
)


func encodeDoubleMessage(message1, message2 interface{}, msgId int) (buff *common.MBuffer, err error){

	buff = common.NewBuffer()

	buff.WriteUint(uint32(0))

	buff.WriteInt(msgId)

	msgItem, err := getMsgRegItemById(msgId)
	if err != nil{
		common.ErrorLog("get msg item failed")
		return nil, err
	}

	if	(msgItem.MsgProtoType != MessageProtoType_ObjRequest) &&
		(msgItem.MsgProtoType != MessageProtoType_ObjResponse) &&
		(msgItem.MsgProtoType != MessageProtoType_FileResponse) &&
		(msgItem.MsgProtoType != MessageProtoType_FileRequest){

		return nil, common.ErrorLog("msg proto type error:", msgItem.MsgProtoType)
	}

	bytesData1, err := common.EncodeObjectPacket(message1)
	if err != nil{
		return nil, common.ErrorLog("marshal obj 1 failed")
	}

	bytesData2, err := common.EncodeObjectPacket(message2)
	if err != nil{
		return nil, common.ErrorLog("marshal obj 2 failed")
	}


	err = buff.Append(bytesData1)
	if err != nil{
		return nil, err
	}

	err = buff.Append(bytesData2)
	if err != nil{
		return nil, err
	}

	msgLen := len(bytesData1) + len(bytesData2)

	err = buff.PrependInt(msgLen)
	if err != nil{
		return nil, err
	}

	return buff, nil

}



func encodeMessage(message interface{}, protoCat CPP_SERVER_MSG_TYPE, protoSeq int64, msgId int) (buff *common.MBuffer, err error){

	buff = common.NewBuffer()

	buff.WriteUint(uint32(protoCat))

	buff.WriteInt(msgId)

	msgItem, err := getMsgRegItemById(msgId)
	if err != nil{
		common.ErrorLog("get msg item failed")
		return nil, err
	}

	var bytesData []byte

	if msgItem.MsgProtoType == MessageProtoType_ObjRequest ||
		msgItem.MsgProtoType == MessageProtoType_ObjResponse ||
		msgItem.MsgProtoType == MessageProtoType_FileRequest ||
		msgItem.MsgProtoType == MessageProtoType_FileResponse{
		bytesData, err = common.EncodeObjectPacket(message)
		if err != nil{
			return nil, common.ErrorLog("marshal obj failed")
		}
	}else if msgItem.MsgProtoType == MessageProtoType_Proto{
		pMessage, ok := message.(proto.Message)
		if ok == false{
			return nil, common.ErrorLog("not a proto message")
		}

		bytesData, err = proto.Marshal(pMessage)
		if err != nil{
			return nil, common.ErrorLog("marshal message failed", err)
		}

		buff.WriteLong(protoSeq)

	}else{
		return nil, common.ErrorLog("message proto type error:", msgItem.MsgProtoType)
	}

	err = buff.Append(bytesData)
	if err != nil{
		return nil, err
	}

	msgLen := len(bytesData)

	err = buff.PrependInt(msgLen)
	if err != nil{
		return nil, err
	}

	return buff, nil

}






























