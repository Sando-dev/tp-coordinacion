package join

import (
	"log/slog"
	"sort"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue  		middleware.Middleware
	outputQueue 		middleware.Middleware
	fruitItemMap		map[uint32]map[string]fruititem.FruitItem
	eofReceived   		map[uint32]int
	aggregationAmount   int
	topSize				int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}
	return &Join{
		inputQueue: 			inputQueue,
		outputQueue: 			outputQueue,
		fruitItemMap:  			map[uint32]map[string]fruititem.FruitItem{},
		eofReceived:   			map[uint32]int{},
		aggregationAmount:     	config.AggregationAmount,
		topSize:           		config.TopSize,
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		join.eofReceived[clientId]++

		if join.eofReceived[clientId] == join.aggregationAmount {
			top := join.buildFruitTop(clientId)

			message, err := inner.SerializeMessage(clientId, top)
			if err != nil {
				slog.Error("While serializing top", "err", err)
				return
			}

			if err := join.outputQueue.Send(*message); err != nil {
				slog.Error("While sending top", "err", err)
				return
			}
			delete(join.fruitItemMap, clientId)
			delete(join.eofReceived, clientId)
		}
		return
	}

	join.handleDataMessage(clientId, fruitRecords)
}

func (join *Join) handleDataMessage(clientId uint32, fruitRecords []fruititem.FruitItem) {
	clientMap, ok := join.fruitItemMap[clientId]
	if !ok {
		clientMap = map[string]fruititem.FruitItem{}
		join.fruitItemMap[clientId] = clientMap
	}
	for _, fruitRecord := range fruitRecords {
		if currentFruit, ok := clientMap[fruitRecord.Fruit]; ok {
			clientMap[fruitRecord.Fruit] = currentFruit.Sum(fruitRecord)
		} else {
			clientMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}


func (join *Join) buildFruitTop(clientId uint32) []fruititem.FruitItem {
    clientMap := join.fruitItemMap[clientId]

    fruitItems := make([]fruititem.FruitItem, 0, len(clientMap))

    for _, item := range clientMap {
        fruitItems = append(fruitItems, item)
    }

    sort.SliceStable(fruitItems, func(i, j int) bool {
        return fruitItems[j].Less(fruitItems[i])
    })

    finalTopSize := min(join.topSize, len(fruitItems))

    return fruitItems[:finalTopSize]
}