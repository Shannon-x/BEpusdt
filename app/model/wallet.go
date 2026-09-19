package model

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/utils"
	"github.com/xssnick/tonutils-go/address"
	"gorm.io/gorm"
)

const (
	WaStatusEnable  uint8 = 1
	WaStatusDisable uint8 = 0
	WaOtherEnable   uint8 = 1
	WaOtherDisable  uint8 = 0
)

type Wallet struct {
	Id
	Name        string `gorm:"column:name;type:varchar(32);not null;default:-';comment:名称" json:"name"`
	Status      uint8  `gorm:"column:status;not null;default:1;comment:地址状态" json:"status"`
	Address     string `gorm:"column:address;type:varchar(128);not null;index;comment:钱包地址" json:"address"`
	MatchAddr   string `gorm:"column:match_addr;type:varchar(128);not null;uniqueIndex:idx_address;comment:匹配地址" json:"match_addr"`
	TradeType   string `gorm:"column:trade_type;type:varchar(20);not null;uniqueIndex:idx_address;comment:交易类型" json:"trade_type"`
	OtherNotify uint8  `gorm:"column:other_notify;not null;default:0;comment:其它通知" json:"other_notify"`
	Remark      string `gorm:"column:remark;type:varchar(255);not null;default:'';comment:备注" json:"remark"`
	// Credentials 交易所 API 凭证（加密 JSON），仅交易所类型钱包使用；不随接口返回
	Credentials    string `gorm:"column:credentials;type:varchar(2048);not null;default:'';comment:交易所 API 凭证(加密)" json:"-"`
	HasCredentials bool   `gorm:"-" json:"has_credentials"`
	AutoTimeAt
}

// ExchangeCredential 交易所只读 API 凭证
type ExchangeCredential struct {
	ApiKey     string `json:"api_key"`
	ApiSecret  string `json:"api_secret"`
	Passphrase string `json:"passphrase,omitempty"` // OKX 专用
}

var exchangeUIDPattern = regexp.MustCompile(`^[1-9]\d{3,19}$`)

// IsExchangeUID 交易所账户 UID：纯数字
func IsExchangeUID(s string) bool {
	return exchangeUIDPattern.MatchString(strings.TrimSpace(s))
}

func (wa *Wallet) TableName() string {

	return "bep_wallet"
}

// AfterFind 查询后补充是否已配置凭证的标记
func (wa *Wallet) AfterFind(*gorm.DB) error {
	wa.HasCredentials = wa.Credentials != ""

	return nil
}

// SetCredentials 加密保存交易所凭证
func (wa *Wallet) SetCredentials(c ExchangeCredential) error {
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}

	enc, err := EncryptSecret(string(raw))
	if err != nil {
		return err
	}

	wa.Credentials = enc
	wa.HasCredentials = true

	return nil
}

// GetCredentials 解密读取交易所凭证
func (wa *Wallet) GetCredentials() (ExchangeCredential, bool) {
	var c ExchangeCredential
	if wa.Credentials == "" {
		return c, false
	}

	raw, err := DecryptSecret(wa.Credentials)
	if err != nil {
		return c, false
	}
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return c, false
	}

	return c, c.ApiKey != "" && c.ApiSecret != ""
}

// GetExchangeWallets 某交易所网络下启用且已配置凭证的钱包
func GetExchangeWallets(n Network) []Wallet {
	trades := GetNetworkTrades(n)
	wallets := make([]Wallet, 0)
	if len(trades) == 0 {
		return wallets
	}

	Db.Where("trade_type in (?) and status = ? and credentials <> ''", trades, WaStatusEnable).Find(&wallets)

	return wallets
}

func (wa *Wallet) SetStatus(status uint8) {
	wa.Status = status
	Db.Save(wa)
}

func (wa *Wallet) Validate() error {
	tradeType := TradeType(wa.TradeType)
	wa.MatchAddr = wa.Address

	if IsExchange(tradeType) {
		if !IsExchangeUID(wa.Address) {
			return errors.New("交易所账户 UID 必须为纯数字")
		}

		return nil
	}

	switch tradeType {
	case TronTrx, UsdtTrc20, UsdcTrc20:
		if !utils.IsValidTronAddress(wa.Address) {
			return errors.New("钱包地址格式不合法，请检查")
		}
	case UsdtSolana, UsdcSolana:
		if !utils.IsValidSolanaAddress(wa.Address) {
			return errors.New("钱包地址格式不合法，请检查")
		}
	case UsdtAptos, UsdcAptos:
		if !utils.IsValidAptosAddress(wa.Address) {
			return errors.New("钱包地址格式不合法，请检查")
		}
	case UsdtTon:
		if !utils.IsValidTonAddress(wa.Address) {
			return errors.New("TON 地址必须以 UQ 开头")
		}
		owner, err := address.ParseAddr(wa.Address)
		if err != nil {
			return err
		}
		addr, err := utils.GetJettonWalletAddr(utils.NewTonClient(GetC(RpcGlobalConfigUrlTon)), address.MustParseAddr(conf.UsdtTon), owner)
		if err != nil {
			return err
		}
		wa.MatchAddr = addr.Bounce(false).String()
		return nil
	case TonGram:
		if !utils.IsValidTonAddress(wa.Address) {
			return errors.New("TON 地址必须以 UQ 开头")
		}
		owner, err := address.ParseAddr(wa.Address)
		if err != nil {
			return err
		}
		wa.MatchAddr = owner.Bounce(false).String()
		return nil
	default:
		if !utils.IsValidEvmAddress(wa.Address) {
			return errors.New("钱包地址格式不合法，请检查")
		}
	}

	if !AddrCaseSens(tradeType) {
		wa.MatchAddr = strings.ToLower(wa.Address)
	}

	return nil
}

func (wa *Wallet) SetOtherNotify(notify uint8) {
	wa.OtherNotify = notify

	Db.Save(wa)
}

func (wa *Wallet) Delete() {
	Db.Delete(wa)
}

func (wa *Wallet) GetTokenContract() string {
	if c, ok := registry[TradeType(wa.TradeType)]; ok {

		return c.Contract
	}

	return ""
}

func (wa *Wallet) GetTokenDecimals() int32 {
	if c, ok := registry[TradeType(wa.TradeType)]; ok {

		return c.Decimal
	}

	return -18
}

func (wa *Wallet) GetNetwork() Network {
	if c, ok := registry[TradeType(wa.TradeType)]; ok {

		return c.Network
	}

	return ""
}

func (wa *Wallet) GetPaymentAddr() string {
	if wa.TradeType == string(UsdtTon) {

		return wa.Address
	}

	return wa.MatchAddr
}

func (wa *Wallet) GetMatchAddr() string {
	return wa.MatchAddr
}

func GetAvailableWallets(t TradeType) []Wallet {
	var wallets = make([]Wallet, 0)

	Db.Where("trade_type = ? and status = ?", t, WaStatusEnable).Find(&wallets)

	return wallets
}

func NewWallet(address string, tradeType TradeType) (Wallet, error) {
	wa := Wallet{Address: address, TradeType: string(tradeType)}
	if err := wa.Validate(); err != nil {
		return Wallet{}, err
	}

	return wa, nil
}
