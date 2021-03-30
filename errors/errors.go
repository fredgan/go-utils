package errors

// TEST

import "fmt"

const (
	//
	// 400 StatusBadRequest
	//
	CodeParamFormatError          = 40000100 // 请求参数格式有误
	CodeGroupNotEmpty             = 40000200 // 删除用户组时发现组成员不为空
	CodeGroupNotFound             = 40000300 // 用户组不存在
	CodeGroupAlreadyExist         = 40000400 // 用户组已存在
	CodeGroupMemberAlreadyExist   = 40000500 // 插入用户组成员时发现成员已存在
	CodeGroupMemberNotFound       = 40000600 // 组成员不存在
	CodeDeptNotEmpty              = 40000700 // 删除部门时发现部门成员不为空
	CodeDeptNotFound              = 40000800 // 部门不存在
	CodeDeptAlreadyExist          = 40000900 // 部门已存在
	CodeDeptMemberAlreadyExist    = 40001000 // 插入部门成员时发现成员已存在
	CodeDeptMemberNotFound        = 40001100 // 部门成员不存在
	CodeCompanyNotEmpty           = 40001200 // 删除公司时发现公司成员不为空
	CodeCompanyNotFound           = 40001300 // 公司不存在
	CodeCompanyAlreadyExist       = 40001400 // 公司已存在
	CodeCompanyMemberAlreadyExist = 40001500 // 插入公司成员时发现成员已存在
	CodeCompanyMemberNotFound     = 40001600 // 公司成员不存在
	CodeUserNotFound              = 40001700 // 用户不存在
	CodeUserAlreadyExist          = 40001800 // 用户已存在

	//
	// 注意，以后资源不存在或已经存在统一用以下两种错误代码，然后在msg中具体描述清楚是什么资源，而不再像Group/Dept/Company/User细分错误代码了。
	//
	CodeResourceNotFound     = 40001900 // 资源不存在
	CodeResourceAlreadyExist = 40002000 // 资源已经存在

	CodeFavoriteNotFound     = 40002100 // 收藏对象不存在
	CodeFavoriteAlreadyExist = 40002200 // 收藏对象已存在

	CodeSessionNotFound  = 40005000 // 会话不存在
	CodeInvalidSessionID = 40005100 // 无效的会话ID
	CodeSessionIDExisted = 40005200 // 会话ID已经存在

	CodeDeviceNotFound = 40005500 // 设备未找到

	CodeUnsupportedContentType = 40006000 // 不支持处理提交数据指定的ContentType

	CodeForbiddenDelete = 40007100 // 禁止删除某资源
	CodeForbiddenUpdate = 40007200 // 禁止修改某资源

	//
	// 401 StatusUnauthorized
	//
	CodeAuthError     = 40100100 // 没有登录或者会话错误
	CodePasswordError = 40100200 // 密码错误
	CodeAPISignError  = 40100300 // API签名错误

	//
	// 403 StatusForbidden
	//
	CodeNoPermission       = 40300000 // 无权限
	CodeNoGetPermission    = 40300100 // 无获取权限
	CodeNoDeletePermission = 40300200 // 无删除权限
	CodeNoAccessPermission = 40300300 // 无访问权限
	//
	// 500 StatusInternalServerError
	//
	CodeServerError  = 50000000
	CodeRemoteError  = 50000001 // 远程请求出错
	CodeUnknownError = 99999999 // 未知错误
)

func New(code int, message string, args ...interface{}) error {
	if len(args) != 0 {
		message = fmt.Sprintf(message, args...)
	}
	return e{
		code:    code,
		message: message,
	}
}

func GetCode(err error) int {
	if x, ok := err.(e); ok {
		return x.Code()
	}
	return CodeUnknownError
}

type e struct {
	code    int
	message string
}

func (err e) Code() int {
	return err.code
}

func (err e) Error() string {
	return err.message
}
