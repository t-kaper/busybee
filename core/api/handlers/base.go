// Package handlers содержит тонкий HTTP-слой BusyBee API.
// Каждый хендлер парсит path/query/body из запроса, вызывает метод dto.Service
// и формирует JSON-ответ через httpx. Бизнес-валидация и доступ к БД
// делегируются нижележащим слоям.
package handlers

import (
	"busybee/core/api/dto"
)

// Handler — тонкий HTTP-слой: парсит запрос, вызывает service и формирует response.
type Handler struct {
	service dto.Service
}

// New создает набор HTTP-хендлеров поверх переданного service-контракта.
func New(service dto.Service) *Handler {
	return &Handler{service: service}
}
