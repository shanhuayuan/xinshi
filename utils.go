package xinshi

import "encoding/json"

func cloneValue(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out any
	if err = json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func cloneEvent(e Event) (Event, error) {
	v, err := cloneValue(e.Data)
	if err != nil {
		return Event{}, err
	}
	e.Data = v
	return e, nil
}

func estimate(e Event) int {
	raw, err := json.Marshal(e)
	if err != nil {
		return 0
	}
	return len(raw)
}
