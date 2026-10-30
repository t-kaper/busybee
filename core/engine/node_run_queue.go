package engine

import (
	"context"
	"sync"
)

// NodeRunQueue — потокобезопасная очередь ID node_run без ограничения размера.
//
// Dispatcher кладёт ID через Push (неблокирующий, O(1) amortized).
// Воркеры берут ID через Pop (блокирующий, ждёт появления элементов).
//
// Преимущества перед каналом:
//   - Нет фиксированного лимита размера (растёт динамически).
//   - Dispatcher никогда не блокируется на записи.
//   - Воркеры просыпаются только когда есть работа (cond.Wait).
//
// Graceful shutdown: Close() → Push игнорируется, Pop возвращает false
// когда очередь опустеет. RecoverZombieNodeRuns подберёт застрявшие queued
// при следующем старте.
type NodeRunQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	runs   []int
	closed bool
}

// NewNodeRunQueue создаёт пустую очередь с предаллоцированным буфером на 1000 элементов.
func NewNodeRunQueue() *NodeRunQueue {
	q := &NodeRunQueue{
		runs: make([]int, 0, 1000),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Push добавляет node_run ID в очередь.
//
// Неблокирующий. Если очередь закрыта — элемент молча игнорируется.
// Это может произойти при shutdown: Dispatcher закоммитил node_run как queued,
// но очередь уже закрыта. Эти записи подберёт RecoverZombieNodeRuns при рестарте.
func (q *NodeRunQueue) Push(runID int) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.runs = append(q.runs, runID)
	q.mu.Unlock()
	q.cond.Signal()
}

// Pop берёт node_run ID из очереди.
//
// Блокирующий: ждёт появления элементов через cond.Wait.
// Возвращает (runID, true) или (0, false) если очередь закрыта и пуста,
// или ctx отменён.
//
// Через context.AfterFunc будим ожидающие воркеры при отмене контекста
// без создания отдельной горутины на каждый Pop.
func (q *NodeRunQueue) Pop(ctx context.Context) (int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	stopNotify := context.AfterFunc(ctx, func() {
		q.cond.Broadcast()
	})
	defer stopNotify()

	for len(q.runs) == 0 && !q.closed {
		select {
		case <-ctx.Done():
			return 0, false
		default:
		}
		q.cond.Wait()
		select {
		case <-ctx.Done():
			return 0, false
		default:
		}
	}

	if q.closed && len(q.runs) == 0 {
		return 0, false
	}

	runID := q.runs[0]
	q.runs = q.runs[1:]

	return runID, true
}

// Close закрывает очередь (graceful shutdown).
//
// После вызова:
//   - Push игнорирует новые элементы.
//   - Pop продолжает отдавать оставшиеся элементы.
//   - Когда очередь опустеет — Pop возвращает (0, false) всем ожидающим воркерам.
func (q *NodeRunQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

// Len возвращает текущее количество элементов в очереди (для мониторинга/логирования).
func (q *NodeRunQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.runs)
}
