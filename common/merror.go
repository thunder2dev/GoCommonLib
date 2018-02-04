package common


type ErrorCode int

const ( 	//change client too

	_ ErrorCode = iota
	ErrorCode_Not_Meet
	ErrorCode_Suggest_End
	ErrorCode_Suggest_Failed

	ErrorCode_CommentCnt_Overflow
	ErrorCode_Comment_Publish_Failed

	ErrorCode_Comment_Refresh_End

	ErrorCode_Slice_Got_Empty

	ErrorCode_Default_Empty

	ErrorCode_Used_Up

	ErrorCode_User_Blocked

)


type CondNotMeetError struct {

}

func (this *CondNotMeetError) Error() string{
	return "cond not meet"
}
func (this *CondNotMeetError) Code() int{
	return int(ErrorCode_Not_Meet)
}



type SuggestEndError struct {

}

func (this SuggestEndError) Error() string{
	return "suggest end"
}
func (this SuggestEndError) Code() int{
	return int(ErrorCode_Suggest_End)
}


type CommentCntOverflowError struct {

}

func (this *CommentCntOverflowError) Error() string{
	return "comment overflow"
}
func (this *CommentCntOverflowError) Code() int{
	return int(ErrorCode_CommentCnt_Overflow)
}





type SliceGotEmptyError struct {
}

func (this *SliceGotEmptyError) Error() string{
	return "slice is used up"
}

func (this *SliceGotEmptyError) Code() int{
	return int(ErrorCode_Slice_Got_Empty)
}



type DefaultEmptyError struct {
}

func (this *DefaultEmptyError) Error() string{
	return "default empty"
}

func (this *DefaultEmptyError) Code() int{
	return int(ErrorCode_Default_Empty)
}



type UsedUpError struct {
}

func (this *UsedUpError) Error() string{
	return "used up"
}

func (this *UsedUpError) Code() int{
	return int(ErrorCode_Used_Up)
}





