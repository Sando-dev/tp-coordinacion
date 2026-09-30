package sum

import (
	"fmt"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"hash/fnv"
	"log/slog"
	"sync"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	id                 int
	inputQueue         middleware.Middleware
	outputExchange     middleware.Middleware
	coordinationInput  middleware.Middleware
	coordinationOutput middleware.Middleware
	fruitItemMap       map[uint32]map[string]fruititem.FruitItem
	aggregationAmount  int
	aggregationPrefix  string
	processingMutex    sync.Mutex
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}
	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}
	coordinationInputKey := []string{
		fmt.Sprintf("%s_%d", config.SumPrefix, config.Id),
	}
	coordinationInput, err := middleware.CreateExchangeMiddleware(config.SumPrefix, coordinationInputKey, connSettings)
	if err != nil {
		outputExchange.Close()
		inputQueue.Close()
		return nil, err
	}
	coordinationOutputKeys := make([]string, config.SumAmount)
	for i := range config.SumAmount {
		coordinationOutputKeys[i] = fmt.Sprintf("%s_%d", config.SumPrefix, i)
	}
	coordinationOutput, err := middleware.CreateExchangeMiddleware(config.SumPrefix, coordinationOutputKeys, connSettings)
	if err != nil {
		outputExchange.Close()
		coordinationInput.Close()
		inputQueue.Close()
		return nil, err
	}
	return &Sum{
		id:                 config.Id,
		inputQueue:         inputQueue,
		outputExchange:     outputExchange,
		coordinationInput:  coordinationInput,
		coordinationOutput: coordinationOutput,
		fruitItemMap:       map[uint32]map[string]fruititem.FruitItem{},
		aggregationAmount:  config.AggregationAmount,
		aggregationPrefix:  config.AggregationPrefix,
	}, nil
}

func (sum *Sum) Run() error {
	errCh := make(chan error, 2)
	go func() {
		err := sum.inputQueue.StartConsuming(func(
			msg middleware.Message,
			ack, nack func(),
		) {
			sum.handleInputMessage(msg, ack, nack)
		})

		errCh <- err
	}()
	go func() {
		err := sum.coordinationInput.StartConsuming(func(
			msg middleware.Message,
			ack, nack func(),
		) {
			sum.handleCoordinationMessage(msg, ack, nack)
		})

		errCh <- err
	}()
	return <-errCh
}

func (sum *Sum) handleEndOfRecordMessage(clientId uint32) error {
	slog.Info("Received End Of Records message", "clientId", clientId)

	clientMap := sum.fruitItemMap[clientId]

	fruitItems := make([]fruititem.FruitItem, 0, len(clientMap))
	for _, fruitItem := range clientMap {
		fruitItems = append(fruitItems, fruitItem)
	}

	for _, fruitItem := range fruitItems {
		fruitRecords := []fruititem.FruitItem{fruitItem}

		message, err := inner.SerializeMessage(clientId, fruitRecords)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}

		index := getAggregationIndex(
			clientId,
			fruitItem.Fruit,
			sum.aggregationAmount,
		)

		routingKey := fmt.Sprintf(
			"%s_%d",
			sum.aggregationPrefix,
			index,
		)

		if err := sum.outputExchange.SendTo(*message, routingKey); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	eofRecords := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientId, eofRecords)
	if err != nil {
		return err
	}

	if err := sum.outputExchange.Send(*message); err != nil {
		return err
	}

	delete(sum.fruitItemMap, clientId)

	return nil
}

func (sum *Sum) handleDataMessage(clientId uint32, fruitRecords []fruititem.FruitItem) error {
	clientMap, ok := sum.fruitItemMap[clientId]
	if !ok {
		clientMap = map[string]fruititem.FruitItem{}
		sum.fruitItemMap[clientId] = clientMap
	}
	for _, fruitRecord := range fruitRecords {
		currentFruit, ok := clientMap[fruitRecord.Fruit]
		if ok {
			clientMap[fruitRecord.Fruit] = currentFruit.Sum(fruitRecord)
		} else {
			clientMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}

func (sum *Sum) handleInputMessage(msg middleware.Message, ack func(), nack func()) {
	sum.processingMutex.Lock()
	defer sum.processingMutex.Unlock()
	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}
	if isEof {
		if err := sum.coordinationOutput.Send(msg); err != nil {
			slog.Error("While broadcasting EOF", "err", err)
			nack()
		}
		ack()
		return
	}
	if err := sum.handleDataMessage(clientId, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
		nack()
		return
	}

	ack()
}

func (sum *Sum) handleCoordinationMessage(msg middleware.Message, ack func(), nack func()) {
	sum.processingMutex.Lock()
	defer sum.processingMutex.Unlock()

	clientId, _, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing coordination message", "err", err)
		nack()
		return
	}

	if !isEof {
		slog.Error("Unexpected non-EOF coordination message")
		nack()
		return
	}

	if err := sum.handleEndOfRecordMessage(clientId); err != nil {
		slog.Error("While handling end of record message", "err", err)
		nack()
		return
	}
	ack()
}

func getAggregationIndex(clientId uint32, fruit string, aggregationAmount int) int {
	h := fnv.New32a()
	fmt.Fprintf(h, "%d-%s", clientId, fruit)
	return int(h.Sum32() % uint32(aggregationAmount))
}

func (sum *Sum) Close() error {
	var firstErr error
	if err := sum.inputQueue.StopConsuming(); err != nil {
		firstErr = err
	}

	if err := sum.coordinationInput.StopConsuming(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := sum.inputQueue.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	if err := sum.coordinationInput.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	if err := sum.coordinationOutput.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	if err := sum.outputExchange.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}
