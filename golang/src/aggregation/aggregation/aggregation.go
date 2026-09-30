package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	fruitItemMap  map[uint32]map[string]fruititem.FruitItem
	topSize       int
	eofReceived   map[uint32]int
	sumAmount     int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitItemMap:  map[uint32]map[string]fruititem.FruitItem{},
		topSize:       config.TopSize,
		eofReceived:   map[uint32]int{},
		sumAmount:     config.SumAmount,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	if isEof {
		aggregation.eofReceived[clientId]++

		if aggregation.eofReceived[clientId] == aggregation.sumAmount {
			if err := aggregation.handleEndOfRecordsMessage(clientId); err != nil {
				slog.Error("While handling end of record message", "err", err)
				nack()
				return
			}
			delete(aggregation.fruitItemMap, clientId)
			delete(aggregation.eofReceived, clientId)
		}
		ack()
		return
	}

	aggregation.handleDataMessage(clientId, fruitRecords)
	ack()
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId uint32) error {
	slog.Info("Received End Of Records message", "clientId", clientId)

	fruitTopRecords := aggregation.buildFruitTop(clientId)

	message, err := inner.SerializeMessage(clientId, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}

	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMessage := []fruititem.FruitItem{}

	message, err = inner.SerializeMessage(clientId, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}

	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId uint32, fruitRecords []fruititem.FruitItem) {
	clientMap, ok := aggregation.fruitItemMap[clientId]
	if !ok {
		clientMap = map[string]fruititem.FruitItem{}
		aggregation.fruitItemMap[clientId] = clientMap
	}
	for _, fruitRecord := range fruitRecords {
		if currentFruit, ok := clientMap[fruitRecord.Fruit]; ok {
			clientMap[fruitRecord.Fruit] = currentFruit.Sum(fruitRecord)
		} else {
			clientMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientId uint32) []fruititem.FruitItem {
	clientMap := aggregation.fruitItemMap[clientId]
	fruitItems := make([]fruititem.FruitItem, 0, len(clientMap))
	for _, item := range clientMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}

func (aggregation *Aggregation) Close() error {
	var firstErr error

	if err := aggregation.inputExchange.StopConsuming(); err != nil {
		firstErr = err
	}

	if err := aggregation.inputExchange.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	if err := aggregation.outputQueue.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}
