package messpub

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"time"
)

//MESMAPLISTSIZE .
const MESMAPLISTSIZE = 8

//DEFAULTMESBUFSIZE .
const DEFAULTMESBUFSIZE = 100

//RSPSUCC .
const RSPSUCC = "succ"

//RSPFAIL .
const RSPFAIL = "fail"

const oPSUBMES = "sub"
const oPPUBMES = "pub"
const oPCANCELSUB = "cancellsub"

//Spubor .
type Spubor struct {
	inited  bool
	size    int
	mesmapl []*mesmap
}
type mesmap struct {
	mes       map[string]*map[int64]recver
	opbufchan chan mesoperation
}

type mesoperation struct {
	msgid  string
	oprate string
	//sub:chan string  pub:string cancellsub:opcancellsub
	initem interface{}
	//op result
	outitem chan oprsp
}

type oprsp struct {
	index  int64
	rspmsg string
}

type recver struct {
	index    int64
	recvchan chan string
}

func (m *mesmap) init() {
	m.mes = make(map[string]*map[int64]recver, 64)
	//buf the operation
	m.opbufchan = make(chan mesoperation, 1000)
	//handle bufchan
	go func() {
		for {
			select {
			case opn := <-m.opbufchan:
				var msgid = opn.msgid
				switch opn.oprate {
				//sub
				case oPSUBMES:
					var rcver recver
					rcver.index = int64(time.Now().UnixNano())
					rcver.recvchan = opn.initem.(chan string)

					if _, ok := m.mes[msgid]; !ok {
						var tmp = make(map[int64]recver)
						m.mes[msgid] = &tmp
					}
					(*m.mes[msgid])[rcver.index] = rcver

					var rsp oprsp
					rsp.index = rcver.index
					rsp.rspmsg = RSPSUCC
					opn.outitem <- rsp
				//pub
				case oPPUBMES:
					var item = opn.initem.(string)
					var succ = true
					var failcount int
					for k, v := range *m.mes[msgid] {
						if v.recvchan == nil {
							delete(*m.mes[msgid], k)
							continue
						}
						go func(recvchan chan string) {
							select {
							case <-time.After(2 * time.Second):
								succ = false
								failcount++
								log.Printf("recv chan is timeout,msgid:%s,index:", msgid)
							case recvchan <- item:
							}
						}(v.recvchan)
					}
					var rsp oprsp
					if succ == false {
						rsp.index = -1
						rsp.rspmsg = fmt.Sprintf("%d subors have not received the pubmsg", failcount)
					} else {
						rsp.rspmsg = fmt.Sprintf("pub msg succ")
					}
					opn.outitem <- rsp

				//cancell sub
				case oPCANCELSUB:
					var cancellop = opn.initem.(opcancellsub)
					index := cancellop.index
					msgid := cancellop.msgid
					var rsp oprsp
					var v recver
					var ok bool
					v, ok = (*m.mes[msgid])[index]
					if !ok {
						rsp.index = -1
						rsp.rspmsg = fmt.Sprintf("sub record not fount,msgid:%s,index:%d", msgid, index)
						opn.outitem <- rsp
						continue
					} else if v.recvchan != nil {
						close(v.recvchan)
					}
					delete(*m.mes[msgid], index)

					rsp.index = index
					rsp.rspmsg = fmt.Sprintf("succ cancell sub,msgid:%s,index:%d", msgid, index)
					opn.outitem <- rsp

				default:
					log.Println("invalid opreation:", opn.oprate)
				}
			}
		}
	}()
}

//Init .
func (s *Spubor) Init() error {
	if s.inited {
		return errors.New("the messpubor alright inited")
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	s.size = MESMAPLISTSIZE
	s.mesmapl = make([]*mesmap, s.size)
	for i := 0; i < s.size; i++ {
		s.mesmapl[i] = &mesmap{}
		s.mesmapl[i].init()
	}
	s.inited = true
	return nil
}

//SubMes .
func (s *Spubor) SubMes(msgid string) (index int64, subchan chan string, err error) {
	lindex := s.getMesListIndex(msgid)
	log.Printf("mesid:%s, lindex:%d", msgid, lindex)
	var op mesoperation
	op.msgid = msgid
	op.oprate = oPSUBMES

	subchan = make(chan string, DEFAULTMESBUFSIZE)
	op.initem = subchan
	op.outitem = make(chan oprsp)

	select {
	case (*s.mesmapl[lindex]).opbufchan <- op:
		select {
		case rsp := <-op.outitem:
			return rsp.index, subchan, nil
		}
	}
}

//PubMes .
func (s *Spubor) PubMes(msgid string, item string) error {
	lindex := s.getMesListIndex(msgid)
	var op mesoperation
	op.msgid = msgid
	op.oprate = oPPUBMES
	op.initem = item
	op.outitem = make(chan oprsp)

	select {
	case s.mesmapl[lindex].opbufchan <- op:
		select {
		case rsp := <-op.outitem:
			if rsp.index < 0 {
				return errors.New(rsp.rspmsg)
			}
			return nil
		}
	}
}

type opcancellsub struct {
	index int64
	msgid string
}

//CancellSub .
func (s *Spubor) CancellSub(msgid string, index int64) error {
	lindex := s.getMesListIndex(msgid)
	var op mesoperation
	op.msgid = msgid
	op.oprate = oPCANCELSUB
	op.outitem = make(chan oprsp)
	op.initem = opcancellsub{index, msgid}
	select {
	case s.mesmapl[lindex].opbufchan <- op:
		select {
		case rsp := <-op.outitem:
			if rsp.index < 0 {
				return errors.New(rsp.rspmsg)
			}
			fmt.Println(rsp)
			return nil
		}
	}
}

func (s *Spubor) getMesListIndex(mesid string) int {
	sum := md5.Sum([]byte(mesid))
	buf := bytes.NewReader(sum[:8])
	var n uint64
	binary.Read(buf, binary.BigEndian, &n)
	return int(n % uint64(s.size))
}
