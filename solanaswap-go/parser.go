package solanaswapgo

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/sirupsen/logrus"
)

type Parser struct {
	meta            *rpc.TransactionMeta
	txInfo          *solana.Transaction
	allAccountKeys  solana.PublicKeySlice
	splTokenInfoMap map[string]TokenInfo // map[authority]TokenInfo
	splDecimalsMap  map[string]uint8     // map[mint]decimals
	Log             *logrus.Logger
}

func NewParserFromBlockTx(tx *rpc.TransactionWithMeta) (*Parser, error) {
	txInfo, err := tx.GetTransaction()

	if err != nil {
		return nil, err
	}

	return _init(txInfo, tx.Meta)
}

func NewTransactionParser(tx *rpc.GetTransactionResult) (*Parser, error) {

	txInfo, err := tx.Transaction.GetTransaction()
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	return _init(txInfo, tx.Meta)
}

func NewSwapParser(txInfo *solana.Transaction, meta *rpc.TransactionMeta) (*Parser, error) {
	return _init(txInfo, meta)
}

func _init(txInfo *solana.Transaction, meta *rpc.TransactionMeta) (*Parser, error) {
	allAccountKeys := append(txInfo.Message.AccountKeys, meta.LoadedAddresses.Writable...)
	allAccountKeys = append(allAccountKeys, meta.LoadedAddresses.ReadOnly...)

	log := logrus.New()
	log.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   true,
	})

	parser := &Parser{
		meta:           meta,
		txInfo:         txInfo,
		allAccountKeys: allAccountKeys,
		Log:            log,
	}

	if err := parser.extractSPLTokenInfo(); err != nil {
		return nil, fmt.Errorf("failed to extract SPL Token Addresses: %w", err)
	}

	if err := parser.extractSPLDecimals(); err != nil {
		return nil, fmt.Errorf("failed to extract SPL decimals: %w", err)
	}

	return parser, nil
}

type SwapData struct {
	Type SwapType
	Data interface{}
}

func (p *Parser) ParseTransaction() ([]SwapData, error) {
	var parsedSwaps []SwapData

	skip := false
	for i, outerInstruction := range p.txInfo.Message.Instructions {
		progID := p.allAccountKeys[outerInstruction.ProgramIDIndex]
		switch {
		case progID.Equals(JUPITER_PROGRAM_ID):
			skip = true
			parsedSwaps = append(parsedSwaps, p.processJupiterSwaps(i)...)
		case progID.Equals(MOONSHOT_PROGRAM_ID):
			skip = true
			parsedSwaps = append(parsedSwaps, p.processMoonshotSwaps()...)
		case progID.Equals(BANANA_GUN_PROGRAM_ID) ||
			progID.Equals(MINTECH_PROGRAM_ID) ||
			progID.Equals(BLOOM_PROGRAM_ID) ||
			progID.Equals(MAESTRO_PROGRAM_ID):
			// Check inner instructions to determine which swap protocol is being used
			if innerSwaps := p.processTradingBotSwaps(i); len(innerSwaps) > 0 {
				parsedSwaps = append(parsedSwaps, innerSwaps...)
			}
		}
	}
	if skip {
		return parsedSwaps, nil
	}

	for i, outerInstruction := range p.txInfo.Message.Instructions {
		progID := p.allAccountKeys[outerInstruction.ProgramIDIndex]
		switch {
		case progID.Equals(RAYDIUM_V4_PROGRAM_ID) ||
			progID.Equals(RAYDIUM_CPMM_PROGRAM_ID) ||
			progID.Equals(RAYDIUM_AMM_PROGRAM_ID) ||
			progID.Equals(RAYDIUM_CONCENTRATED_LIQUIDITY_PROGRAM_ID) ||
			progID.Equals(NOVA_PROGRAM_ID) ||
			progID.Equals(OKX_DEX_ROUTER_PROGRAM_ID) ||
			progID.Equals(solana.MustPublicKeyFromBase58("AP51WLiiqTdbZfgyRMs35PsZpdmLuPDdHYmrB23pEtMU")):
			parsedSwaps = append(parsedSwaps, p.processRaydSwaps(i)...)
		case progID.Equals(ORCA_PROGRAM_ID):
			parsedSwaps = append(parsedSwaps, p.processOrcaSwaps(i)...)
		case progID.Equals(METEORA_PROGRAM_ID) || progID.Equals(METEORA_POOLS_PROGRAM_ID):
			parsedSwaps = append(parsedSwaps, p.processMeteoraSwaps(i)...)
		case progID.Equals(PUMP_FUN_PROGRAM_ID) ||
			progID.Equals(solana.MustPublicKeyFromBase58("BSfD6SHZigAfDWSjzD5Q41jw8LmKwtmjskPH9XW1mrRW")): // PumpFun
			parsedSwaps = append(parsedSwaps, p.processPumpfunSwaps(i)...)
		}
	}

	return parsedSwaps, nil
}

type SwapInfo struct {
	Signers    []solana.PublicKey
	Signatures []solana.Signature
	AMMs       []string
	Timestamp  time.Time

	TokenInMint     solana.PublicKey
	TokenInAmount   uint64
	TokenInDecimals uint8

	TokenOutMint     solana.PublicKey
	TokenOutAmount   uint64
	TokenOutDecimals uint8
}

func (p *Parser) ProcessSwapData(swapDatas []SwapData) (*SwapInfo, error) {

	txInfo := p.txInfo

	swapInfo := &SwapInfo{
		Signers:    txInfo.Message.Signers(),
		Signatures: txInfo.Signatures,
		// TODO: add timestamp (get from block)
	}

	for i, swapData := range swapDatas {
		switch swapData.Type {
		case JUPITER:
			intermediateInfo, err := parseJupiterEvents(swapDatas)
			if err != nil {
				return nil, fmt.Errorf("failed to parse Jupiter events: %w", err)
			}
			jupiterSwapInfo, err := p.convertToSwapInfo(intermediateInfo)
			if err != nil {
				return nil, fmt.Errorf("failed to convert to swap info: %w", err)
			}
			jupiterSwapInfo.Signatures = swapInfo.Signatures
			return jupiterSwapInfo, nil
		case PUMP_FUN:
			if swapData.Data.(*PumpfunTradeEvent).IsBuy {
				swapInfo.TokenInMint = NATIVE_SOL_MINT_PROGRAM_ID // TokenIn info is always SOL for Pumpfun
				swapInfo.TokenInAmount = swapData.Data.(*PumpfunTradeEvent).SolAmount
				swapInfo.TokenInDecimals = 9
				swapInfo.TokenOutMint = swapData.Data.(*PumpfunTradeEvent).Mint
				swapInfo.TokenOutAmount = swapData.Data.(*PumpfunTradeEvent).TokenAmount
				swapInfo.TokenOutDecimals = p.splDecimalsMap[swapInfo.TokenOutMint.String()]
			} else {
				swapInfo.TokenInMint = swapData.Data.(*PumpfunTradeEvent).Mint
				swapInfo.TokenInAmount = swapData.Data.(*PumpfunTradeEvent).TokenAmount
				swapInfo.TokenInDecimals = p.splDecimalsMap[swapInfo.TokenInMint.String()]
				swapInfo.TokenOutMint = NATIVE_SOL_MINT_PROGRAM_ID // TokenOut info is always SOL for Pumpfun
				swapInfo.TokenOutAmount = swapData.Data.(*PumpfunTradeEvent).SolAmount
				swapInfo.TokenOutDecimals = 9
			}
			swapInfo.AMMs = append(swapInfo.AMMs, string(swapData.Type))
			swapInfo.Timestamp = time.Unix(int64(swapData.Data.(*PumpfunTradeEvent).Timestamp), 0)
			return swapInfo, nil // Pumpfun only has one swap event
		case METEORA:
			switch swapData.Data.(type) {
			case *TransferCheck:
				swapData := swapData.Data.(*TransferCheck)
				if i == 0 {
					tokenInAmount, _ := strconv.ParseInt(swapData.Info.TokenAmount.Amount, 10, 64)
					swapInfo.TokenInMint = solana.MustPublicKeyFromBase58(swapData.Info.Mint)
					swapInfo.TokenInAmount = uint64(tokenInAmount)
					swapInfo.TokenInDecimals = swapData.Info.TokenAmount.Decimals
				} else {
					tokenOutAmount, _ := strconv.ParseFloat(swapData.Info.TokenAmount.Amount, 64)
					swapInfo.TokenOutMint = solana.MustPublicKeyFromBase58(swapData.Info.Mint)
					swapInfo.TokenOutAmount = uint64(tokenOutAmount)
					swapInfo.TokenOutDecimals = swapData.Info.TokenAmount.Decimals
				}
			case *TransferData: // Meteora Pools
				swapData := swapData.Data.(*TransferData)
				if i == 0 {
					swapInfo.TokenInMint = solana.MustPublicKeyFromBase58(swapData.Mint)
					swapInfo.TokenInAmount = swapData.Info.Amount
					swapInfo.TokenInDecimals = swapData.Decimals
				} else {
					if swapData.Info.Authority == swapInfo.Signers[0].String() && swapData.Mint == swapInfo.TokenInMint.String() {
						swapInfo.TokenInAmount += swapData.Info.Amount
					}
					swapInfo.TokenOutMint = solana.MustPublicKeyFromBase58(swapData.Mint)
					swapInfo.TokenOutAmount = swapData.Info.Amount
					swapInfo.TokenOutDecimals = swapData.Decimals
				}
			}
		case RAYDIUM, ORCA:
			switch swapData.Data.(type) {
			case *TransferData: // Raydium V4 and Orca
				swapData := swapData.Data.(*TransferData)
				if i == 0 {
					swapInfo.TokenInMint = solana.MustPublicKeyFromBase58(swapData.Mint)
					swapInfo.TokenInAmount = swapData.Info.Amount
					swapInfo.TokenInDecimals = swapData.Decimals
				} else {
					swapInfo.TokenOutMint = solana.MustPublicKeyFromBase58(swapData.Mint)
					swapInfo.TokenOutAmount = swapData.Info.Amount
					swapInfo.TokenOutDecimals = swapData.Decimals
				}
			case *TransferCheck: // Raydium CPMM
				swapData := swapData.Data.(*TransferCheck)
				if i == 0 {
					tokenInAmount, _ := strconv.ParseInt(swapData.Info.TokenAmount.Amount, 10, 64)
					swapInfo.TokenInMint = solana.MustPublicKeyFromBase58(swapData.Info.Mint)
					swapInfo.TokenInAmount = uint64(tokenInAmount)
					swapInfo.TokenInDecimals = swapData.Info.TokenAmount.Decimals
				} else {
					tokenOutAmount, _ := strconv.ParseFloat(swapData.Info.TokenAmount.Amount, 64)
					swapInfo.TokenOutMint = solana.MustPublicKeyFromBase58(swapData.Info.Mint)
					swapInfo.TokenOutAmount = uint64(tokenOutAmount)
					swapInfo.TokenOutDecimals = swapData.Info.TokenAmount.Decimals
				}
			}
		case MOONSHOT:
			swapData := swapData.Data.(*MoonshotTradeInstructionWithMint)
			switch swapData.TradeType {
			case TradeTypeBuy: // BUY
				swapInfo.TokenInMint = NATIVE_SOL_MINT_PROGRAM_ID
				swapInfo.TokenInAmount = swapData.CollateralAmount
				swapInfo.TokenInDecimals = 9
				swapInfo.TokenOutMint = swapData.Mint
				swapInfo.TokenOutAmount = swapData.TokenAmount
				swapInfo.TokenOutDecimals = 9
			case TradeTypeSell: // SELL
				swapInfo.TokenInMint = swapData.Mint
				swapInfo.TokenInAmount = swapData.TokenAmount
				swapInfo.TokenInDecimals = 9
				swapInfo.TokenOutMint = NATIVE_SOL_MINT_PROGRAM_ID
				swapInfo.TokenOutAmount = swapData.CollateralAmount
				swapInfo.TokenOutDecimals = 9
			default:
				return nil, fmt.Errorf("invalid trade type: %d", swapData.TradeType)
			}
		}
		swapInfo.AMMs = append(swapInfo.AMMs, string(swapData.Type))
	}

	return swapInfo, nil
}

type TokenData struct {
	mint     solana.PublicKey
	amount   uint64
	decimals uint8
}

func (p *Parser) ProcessAggregatedSwapData(swapDatas []SwapData) (*SwapInfo, error) {
	// 检查是否都不属于需要特殊处理的类型
	needSpecialProcess := false
	for _, swapData := range swapDatas {
		if swapData.Type == METEORA || swapData.Type == RAYDIUM ||
			swapData.Type == ORCA || swapData.Type == MOONSHOT {
			needSpecialProcess = true
			break
		}
	}

	if !needSpecialProcess {
		return p.ProcessSwapData(swapDatas)
	}

	txInfo := p.txInfo
	swapInfo := &SwapInfo{
		Signers:    txInfo.Message.Signers(),
		Signatures: txInfo.Signatures,
	}

	inputs := make(map[string]TokenData)
	outputs := make(map[string]TokenData)

	// 处理所有交易
	for i := 0; i < len(swapDatas); i++ {
		swapData := swapDatas[i]

		switch swapData.Type {
		case MOONSHOT:
			data := swapData.Data.(*MoonshotTradeInstructionWithMint)
			if data.TradeType == TradeTypeBuy {
				// SOL => Token
				addToMap(inputs, TokenData{
					mint:     NATIVE_SOL_MINT_PROGRAM_ID,
					amount:   data.CollateralAmount,
					decimals: 9,
				})
				addToMap(outputs, TokenData{
					mint:     data.Mint,
					amount:   data.TokenAmount,
					decimals: 9,
				})
			} else {
				// Token => SOL
				addToMap(inputs, TokenData{
					mint:     data.Mint,
					amount:   data.TokenAmount,
					decimals: 9,
				})
				addToMap(outputs, TokenData{
					mint:     NATIVE_SOL_MINT_PROGRAM_ID,
					amount:   data.CollateralAmount,
					decimals: 9,
				})
			}
		case METEORA, RAYDIUM, ORCA:
			var tokenData TokenData
			switch data := swapData.Data.(type) {
			case *TransferData:
				tokenData = TokenData{
					mint:     solana.MustPublicKeyFromBase58(data.Mint),
					amount:   data.Info.Amount,
					decimals: data.Decimals,
				}
			case *TransferCheck:
				amount := uint64(0)
				if i%2 == 0 { // 偶数位置为输入
					parsedAmount, _ := strconv.ParseInt(data.Info.TokenAmount.Amount, 10, 64)
					amount = uint64(parsedAmount)
				} else { // 奇数位置为输出
					parsedAmount, _ := strconv.ParseFloat(data.Info.TokenAmount.Amount, 64)
					amount = uint64(parsedAmount)
				}
				tokenData = TokenData{
					mint:     solana.MustPublicKeyFromBase58(data.Info.Mint),
					amount:   amount,
					decimals: data.Info.TokenAmount.Decimals,
				}
			}

			// 根据位置判断是输入还是输出
			if i%2 == 0 {
				addToMap(inputs, tokenData)
			} else {
				addToMap(outputs, tokenData)
			}
		}

		swapInfo.AMMs = append(swapInfo.AMMs, string(swapData.Type))
	}

	// 移除同时在输入和输出的代币
	for mintStr := range inputs {
		if _, exists := outputs[mintStr]; exists {
			delete(inputs, mintStr)
			delete(outputs, mintStr)
		}
	}

	// 设置最终的输入输出
	// 注意：可能会有多个输入和输出，这里只保留最早的输入和最后的输出
	for _, input := range inputs {
		swapInfo.TokenInMint = input.mint
		swapInfo.TokenInAmount = input.amount
		swapInfo.TokenInDecimals = input.decimals
		break // 只取第一个输入
	}

	for _, output := range outputs {
		swapInfo.TokenOutMint = output.mint
		swapInfo.TokenOutAmount = output.amount
		swapInfo.TokenOutDecimals = output.decimals
		// 不 break，让最后一个输出覆盖前面的
	}

	return swapInfo, nil
}

// 辅助函数：添加到 map 并累加金额
func addToMap(m map[string]TokenData, data TokenData) {
	mintStr := data.mint.String()
	if existing, exists := m[mintStr]; exists {
		existing.amount += data.amount
		m[mintStr] = existing
	} else {
		m[mintStr] = data
	}
}

func (p *Parser) processTradingBotSwaps(instructionIndex int) []SwapData {
	var swaps []SwapData

	// get inner instructions for this index
	innerInstructions := p.getInnerInstructions(instructionIndex)
	if len(innerInstructions) == 0 {
		return swaps
	}

	// track which protocols we've processed to avoid duplicates
	processedProtocols := make(map[string]bool)

	// check program IDs of inner instructions to determine swap type
	for _, inner := range innerInstructions {
		progID := p.allAccountKeys[inner.ProgramIDIndex]

		switch {
		case (progID.Equals(RAYDIUM_V4_PROGRAM_ID) ||
			progID.Equals(RAYDIUM_CPMM_PROGRAM_ID) ||
			progID.Equals(RAYDIUM_AMM_PROGRAM_ID) ||
			progID.Equals(RAYDIUM_CONCENTRATED_LIQUIDITY_PROGRAM_ID)) && !processedProtocols["raydium"]:
			processedProtocols["raydium"] = true
			if raydSwaps := p.processRaydSwaps(instructionIndex); len(raydSwaps) > 0 {
				swaps = append(swaps, raydSwaps...)
			}

		case progID.Equals(ORCA_PROGRAM_ID) && !processedProtocols["orca"]:
			processedProtocols["orca"] = true
			if orcaSwaps := p.processOrcaSwaps(instructionIndex); len(orcaSwaps) > 0 {
				swaps = append(swaps, orcaSwaps...)
			}

		case (progID.Equals(METEORA_PROGRAM_ID) ||
			progID.Equals(METEORA_POOLS_PROGRAM_ID)) && !processedProtocols["meteora"]:
			processedProtocols["meteora"] = true
			if meteoraSwaps := p.processMeteoraSwaps(instructionIndex); len(meteoraSwaps) > 0 {
				swaps = append(swaps, meteoraSwaps...)
			}

		case (progID.Equals(PUMP_FUN_PROGRAM_ID) ||
			progID.Equals(solana.MustPublicKeyFromBase58("BSfD6SHZigAfDWSjzD5Q41jw8LmKwtmjskPH9XW1mrRW"))) && !processedProtocols["pumpfun"]:
			processedProtocols["pumpfun"] = true
			if pumpfunSwaps := p.processPumpfunSwaps(instructionIndex); len(pumpfunSwaps) > 0 {
				swaps = append(swaps, pumpfunSwaps...)
			}
		}
	}

	return swaps
}

// helper function to get inner instructions for a given instruction index
func (p *Parser) getInnerInstructions(index int) []solana.CompiledInstruction {
	if p.meta == nil || p.meta.InnerInstructions == nil {
		return nil
	}

	for _, inner := range p.meta.InnerInstructions {
		if inner.Index == uint16(index) {
			return inner.Instructions
		}
	}

	return nil
}
