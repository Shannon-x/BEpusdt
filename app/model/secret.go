package model

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"strings"
)

// 敏感配置（交易所 API 凭证）落库前加密：AES-256-GCM，密钥由系统密钥 admin_secret 派生。
// 密钥与数据同库保存，防的是备份/导出文件被随意翻看，不能替代"只授予只读权限 + IP 白名单"。

const secretPrefix = "enc:v1:"

func secretKey() []byte {
	sum := sha256.Sum256([]byte("bepusdt-secret:" + GetK(AdminSecret)))

	return sum[:]
}

func EncryptSecret(plain string) (string, error) {
	block, err := aes.NewCipher(secretKey())
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	sealed := gcm.Seal(nonce, nonce, []byte(plain), nil)

	return secretPrefix + base64.StdEncoding.EncodeToString(sealed), nil
}

// DecryptSecret 解密；不带前缀的历史明文原样返回
func DecryptSecret(data string) (string, error) {
	if !strings.HasPrefix(data, secretPrefix) {
		return data, nil
	}

	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(data, secretPrefix))
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(secretKey())
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(raw) < gcm.NonceSize() {
		return "", errors.New("ciphertext too short")
	}

	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], nil)
	if err != nil {
		return "", err
	}

	return string(plain), nil
}
