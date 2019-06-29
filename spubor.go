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
const DEFAULTMESBUFSIZE = 4

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
	mes       map[string][]recver
	opbufchan chan mesoperation
}

type mesoperation struct {
	msgid  string
	oprate string
	//sub:chan string  pub:string
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
	m.mes = make(map[string][]recver, 64)
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
					rcver.index = int64(len(m.mes[msgid]))
					rcver.recvchan = opn.initem.(chan string)
					m.mes[msgid] = append(m.mes[msgid], rcver)

					var rsp oprsp
					rsp.index = rcver.index
					rsp.rspmsg = RSPSUCC
					opn.outitem <- rsp
				//pub
				case oPPUBMES:
					var item = opn.initem.(string)
					var succ = true
					var failcount int
					for k, v := range m.mes[msgid] {
						ticker := time.Tick(2 * time.Second)
						select {
						case <-ticker:
							succ = false
							failcount++
							log.Printf("recv chan is timeout,msgid:%s,index:%d", msgid, k)
						case v.recvchan <- item:
						}
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
	case s.mesmapl[lindex].opbufchan <- op:
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

func (s *Spubor) getMesListIndex(mesid string) int {
	sum := md5.Sum([]byte(mesid))
	buf := bytes.NewReader(sum[:8])
	var n uint64
	binary.Read(buf, binary.BigEndian, &n)
	return int(n % uint64(s.size))
}
