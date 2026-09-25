package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessage(clientId uint32, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	data := []interface{}{}
	data = append(data, clientId)
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	body, err := serializeJson(data)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (uint32, []fruititem.FruitItem, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return 0, nil, false, err
	}

	clientIdFloat, ok := data[0].(float64)
	if !ok {
		return 0, nil, false, errors.New("Client ID is not a number")
	}

    clientId := uint32(clientIdFloat)

	fruitRecords := []fruititem.FruitItem{}
	for _, datum := range data[1:] {
		fruitPair, ok := datum.([]interface{})
		if !ok {
			return 0, nil, false, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return 0, nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return 0, nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return clientId, fruitRecords, len(fruitRecords) == 0, nil
}
