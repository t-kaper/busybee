package httpx

import "encoding/json"

// Optional хранит значение поля patch-запроса и факт его присутствия в JSON.
type Optional[T any] struct {
	Set   bool
	Value T
}

// UnmarshalJSON помечает поле как переданное, даже если клиент прислал null.
func (o *Optional[T]) UnmarshalJSON(data []byte) error {
	o.Set = true
	if string(data) == "null" {
		var zero T
		o.Value = zero
		return nil
	}
	return json.Unmarshal(data, &o.Value)
}
