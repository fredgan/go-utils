package util

import (
	"crypto/aes"
	"errors"
)

var (
	ErrInvalidPadding = errors.New("aes invalid padding")
)

type PaddingFunc func([]byte) []byte
type UnPaddingFunc func([]byte) ([]byte, error)

func Pkcs5Padding(data []byte) []byte {
	remain := aes.BlockSize - (len(data) % aes.BlockSize)
	ch := byte(remain)
	for i := 0; i < remain; i++ {
		data = append(data, ch)
	}
	return data
}

func Pkcs5Unpadding(data []byte) ([]byte, error) {
	s := len(data)
	if (s % 16) != 0 {
		return nil, ErrInvalidPadding
	}
	paddingSize := int(data[s-1])
	if paddingSize > s {
		return nil, ErrInvalidPadding
	}
	return data[:s-paddingSize], nil
}

func AesDecrypt(key []byte, data []byte, unpadding UnPaddingFunc) ([]byte, error) {
	cipher, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	dst := make([]byte, len(data))
	dstRef := dst
	for len(data) > 0 {
		cipher.Decrypt(dstRef, data)
		dstRef = dstRef[aes.BlockSize:]
		data = data[aes.BlockSize:]
	}
	if unpadding == nil {
		return dst, nil
	}
	dst, err = unpadding(dst)
	if err != nil {
		return nil, err
	}
	return dst, nil
}

func AesEncrypt(key []byte, data []byte, padding PaddingFunc) ([]byte, error) {
	if padding != nil {
		data = padding(data)
	}
	cipher, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	dst := make([]byte, len(data))
	dstRef := dst
	for len(data) > 0 {
		cipher.Encrypt(dstRef, data)
		dstRef = dstRef[aes.BlockSize:]
		data = data[aes.BlockSize:]
	}
	return dst, nil
}
