package server

type Persistence struct {
	Term  uint64
	Index uint64
}

func (p *Persistence) Save() {

}
